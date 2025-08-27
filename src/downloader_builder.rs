use std::borrow::Cow;
use std::num::{NonZeroU8, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use headers::{ETag, HeaderMap, HeaderMapExt};

use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{DownloadExtensionBuilder, ExtendedHttpFileDownloader, HttpFileDownloader};

#[derive(Debug, PartialEq)]
pub enum HttpRedirectionHandle {
    Invalid,
    RequestNewLocation { max_times: usize },
}

pub struct HttpDownloadConfig {
    // Frequently accessed configuration (grouped for cache locality)
    pub url: Arc<Url>,
    pub save_dir: PathBuf,
    pub file_name: String,
    pub header_map: HeaderMap,

    // Download parameters
    pub download_connection_count: NonZeroU8,
    pub chunk_size: NonZeroUsize,
    pub request_retry_count: u8,
    pub strict_check_accept_ranges: bool,

    // Timing and intervals
    pub chunks_send_interval: Option<Duration>,
    pub downloaded_len_send_interval: Option<Duration>,

    // Optional components (less frequently accessed)
    pub etag: Option<ETag>,
    pub cancel_token: Option<CancellationToken>,
    pub http_request_configure:
        Option<Box<dyn Fn(reqwest::Request) -> reqwest::Request + Send + Sync + 'static>>,
    pub open_option: Box<dyn Fn(&mut std::fs::OpenOptions) + Send + Sync + 'static>,

    // Flags and enums
    pub set_len_in_advance: bool,
    pub create_dir: bool,
    pub use_browser_user_agent: bool,
    pub handle_redirection: HttpRedirectionHandle,
}

impl HttpDownloadConfig {
    /// 下载文件路径
    pub fn file_path(&self) -> PathBuf {
        self.save_dir.join(&self.file_name)
    }

    pub(crate) fn create_http_request(
        &self,
        redirection_location: Option<&str>,
    ) -> reqwest::Request {
        let mut url = self.url.as_ref().clone();
        if let Some(location) = redirection_location {
            url.set_path(location);
        }

        let mut request = reqwest::Request::new(reqwest::Method::GET, url);
        let header_map = request.headers_mut();

        // Pre-allocate common headers
        if self.use_browser_user_agent {
            static USER_AGENT: headers::HeaderValue = headers::HeaderValue::from_static(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.48",
            );
            header_map.insert(reqwest::header::USER_AGENT, USER_AGENT.clone());
        }

        static ACCEPT_ALL: headers::HeaderValue = headers::HeaderValue::from_static("*/*");
        header_map.insert(reqwest::header::ACCEPT, ACCEPT_ALL.clone());
        header_map.typed_insert(headers::Connection::keep_alive());

        // Clone headers only when necessary
        if !self.header_map.is_empty() {
            header_map.extend(self.header_map.iter().map(|(k, v)| (k.clone(), v.clone())));
        }

        // Disable timeout to prevent issues with rate limiting
        *request.timeout_mut() = None;

        if let Some(configure) = self.http_request_configure.as_ref() {
            configure(request)
        } else {
            request
        }
    }
}

pub struct HttpDownloaderBuilder {
    chunk_size: NonZeroUsize,
    download_connection_count: NonZeroU8,
    url: Url,
    save_dir: PathBuf,
    set_len_in_advance: bool,
    file_name: Option<String>,
    open_option: Box<dyn Fn(&mut std::fs::OpenOptions) + Send + Sync + 'static>,
    create_dir: bool,
    request_retry_count: u8,
    // timeout: Option<Duration>,
    etag: Option<ETag>,
    client: Option<reqwest::Client>,
    header_map: HeaderMap,
    downloaded_len_send_interval: Option<Duration>,
    chunks_send_interval: Option<Duration>,
    strict_check_accept_ranges: bool,
    http_request_configure:
        Option<Box<dyn Fn(reqwest::Request) -> reqwest::Request + Send + Sync + 'static>>,
    cancel_token: Option<CancellationToken>,
    handle_redirection: HttpRedirectionHandle,
    use_browser_user_agent: bool,
}

impl HttpDownloaderBuilder {
    pub fn new(url: Url, save_dir: PathBuf) -> Self {
        Self {
            // Core configuration
            url,
            save_dir,
            file_name: None,
            header_map: HeaderMap::new(),

            // Download parameters with sensible defaults
            download_connection_count: NonZeroU8::new(3).unwrap(),
            chunk_size: NonZeroUsize::new(4 * 1024 * 1024).unwrap(), // 4MB
            request_retry_count: 3,
            strict_check_accept_ranges: true,

            // Timing defaults
            chunks_send_interval: Some(Duration::from_millis(300)),
            downloaded_len_send_interval: Some(Duration::from_millis(300)),

            // Optional components
            etag: None,
            cancel_token: None,
            http_request_configure: None,
            open_option: Box::new(|o| {
                o.create(true).write(true);
            }),

            // Flags and enums
            set_len_in_advance: false,
            create_dir: true,
            use_browser_user_agent: true,
            handle_redirection: HttpRedirectionHandle::RequestNewLocation { max_times: 8 },

            // Client (optional)
            client: None,
        }
    }

    pub fn client(mut self, client: Option<reqwest::Client>) -> Self {
        self.client = client;
        self
    }

    /// 当目录不存在时，是否创建它
    pub fn create_dir(mut self, create_dir: bool) -> Self {
        self.create_dir = create_dir;
        self
    }

    pub fn cancel_token(mut self, cancel_token: Option<CancellationToken>) -> Self {
        self.cancel_token = cancel_token;
        self
    }

    pub fn handle_redirection(mut self, http_redirection_handle: HttpRedirectionHandle) -> Self {
        self.handle_redirection = http_redirection_handle;
        self
    }

    /// 是否提前设置文件长度
    pub fn set_len_in_advance(mut self, set_len_in_advance: bool) -> Self {
        self.set_len_in_advance = set_len_in_advance;
        self
    }

    /// HTTP 请求重试次数
    pub fn request_retry_count(mut self, request_retry_count: u8) -> Self {
        self.request_retry_count = request_retry_count;
        self
    }

    /// 请求头自定义
    pub fn header_map(mut self, header_map: HeaderMap) -> Self {
        self.header_map = header_map;
        self
    }
    /// 使用浏览器 User Agent
    pub fn use_browser_user_agent(mut self, use_browser_user_agent: bool) -> Self {
        self.use_browser_user_agent = use_browser_user_agent;
        self
    }

    /// 下载长度发送间隔
    pub fn downloaded_len_send_interval(
        mut self,
        downloaded_len_send_interval: Option<Duration>,
    ) -> Self {
        self.downloaded_len_send_interval = downloaded_len_send_interval;
        self
    }

    /// chunks 发送间隔
    pub fn chunks_send_interval(mut self, chunks_send_interval: Option<Duration>) -> Self {
        self.chunks_send_interval = chunks_send_interval;
        self
    }

    /*
    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }*/

    /// 文件名称
    pub fn file_name(mut self, file_name: Option<String>) -> Self {
        self.file_name = file_name;
        self
    }

    pub fn file_name_str(mut self, file_name: &str) -> Self {
        self.file_name = Some(file_name.to_string());
        self
    }

    /// chunk 大小
    pub fn chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// HTTP Etag 校验
    pub fn etag(mut self, etag: Option<ETag>) -> Self {
        self.etag = etag;
        self
    }

    /// 是否严格检测 Accept-Ranges 响应头
    pub fn strict_check_accept_ranges(mut self, strict_check_accept_ranges: bool) -> Self {
        self.strict_check_accept_ranges = strict_check_accept_ranges;
        self
    }

    /// 下载连接数
    pub fn download_connection_count(mut self, download_connection_count: NonZeroU8) -> Self {
        self.download_connection_count = download_connection_count;
        self
    }

    /// reqwest::Request 配置方法
    pub fn http_request_configure(
        mut self,
        http_request_configure: impl Fn(reqwest::Request) -> reqwest::Request + Send + Sync + 'static,
    ) -> Self {
        self.http_request_configure = Some(Box::new(http_request_configure));
        self
    }

    /// 构建 `ExtendedHttpFileDownloader`
    /// 参数为需要开启的扩展，多个扩展用元组来表示，如果不需要扩展可以传入`()`空元组
    pub fn build<DEB: DownloadExtensionBuilder>(
        self,
        extension_builder: DEB,
    ) -> (ExtendedHttpFileDownloader, DEB::ExtensionState) {
        let file_name = self
            .file_name
            .unwrap_or_else(|| self.url.file_name().to_string());

        let config = HttpDownloadConfig {
            url: Arc::new(self.url),
            save_dir: self.save_dir,
            file_name,
            header_map: self.header_map,

            download_connection_count: self.download_connection_count,
            chunk_size: self.chunk_size,
            request_retry_count: self.request_retry_count,
            strict_check_accept_ranges: self.strict_check_accept_ranges,

            chunks_send_interval: self.chunks_send_interval,
            downloaded_len_send_interval: self.downloaded_len_send_interval,

            etag: self.etag,
            cancel_token: self.cancel_token,
            http_request_configure: self.http_request_configure,
            open_option: self.open_option,

            set_len_in_advance: self.set_len_in_advance,
            create_dir: self.create_dir,
            use_browser_user_agent: self.use_browser_user_agent,
            handle_redirection: self.handle_redirection,
        };

        let client = self.client.unwrap_or_default();
        let mut downloader = HttpFileDownloader::new(client, Arc::new(config));

        let (extension, es) = extension_builder.build(&mut downloader);
        (
            ExtendedHttpFileDownloader::new(downloader, Box::new(extension)),
            es,
        )
    }
}

pub trait UrlFileName {
    fn file_name(&self) -> Cow<'_, str>;
}

impl UrlFileName for Url {
    fn file_name(&self) -> Cow<'_, str> {
        const WEBSITE_DEFAULT: &str = "index.html";

        self.path_segments()
            .and_then(|mut segments| segments.next_back())
            .map(|last_segment| {
                if last_segment.is_empty() {
                    Cow::Borrowed(WEBSITE_DEFAULT)
                } else {
                    Cow::Borrowed(last_segment)
                }
            })
            .or_else(|| self.domain().map(Cow::Borrowed))
            .unwrap_or(Cow::Borrowed(WEBSITE_DEFAULT))
    }
}
