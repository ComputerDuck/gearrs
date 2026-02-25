use std::convert::TryFrom;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};
use tokio::time::timeout;
use tokio_util::codec::Framed;
use url::Url;

#[cfg(not(test))]
use std::net::ToSocketAddrs;
#[cfg(not(test))]
use tokio::net::TcpStream;

#[cfg(test)]
use tokio::io::DuplexStream;

use crate::job_tracker::JobTracker;
use crate::messages::job::JobHandleInner;
use crate::packages::{Echo, GetStatus, GetStatusUnique, OptionRes, SetOption, SubmitJob};
use crate::packages::{JobCreated, ServerError, StatusRes, StatusResUnique};
use crate::wire::{ClientMessage, CodecError, GearmanCodec, PacketType, ServerMessage};

/// A configuration for a Gearman Client
///
/// # Examples
/// ```norun
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let timeout = std::time::Duration::from_secs(1);
///     let conn_options = gearrs::ConnectOptions::new("gearman://127.0.0.1")?.with_timeout(timeout);
///
///     let connection = gearrs::Connection::connect(conn_options).await?;
///
///     Ok(())
/// }
/// ```
///
pub struct ConnectOptions {
    pub address: Url,
    pub timeout: Duration,
}

impl ConnectOptions {
    /// Create a new [ConnectOptions] struct with a url
    pub fn new<T>(address: T) -> Result<Self, <Url as TryFrom<T>>::Error>
    where
        Url: TryFrom<T>,
    {
        Ok(Self {
            address: Url::try_from(address)?,
            timeout: Duration::from_secs(300),
        })
    }

    /// Set a maximum timeout for requests to a server (default is 300 seconds)
    pub fn with_timeout(mut self, dur: Duration) -> Self {
        self.timeout = dur;
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GearmanError {
    #[error("server error {code}: {message}")]
    ServerError { code: String, message: String },
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("operation timed out")]
    Timeout,
    #[error("connection closed")]
    ConnectionClosed,
}

impl From<tokio::time::error::Elapsed> for GearmanError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        GearmanError::Timeout
    }
}

impl From<ServerError> for GearmanError {
    fn from(e: ServerError) -> Self {
        GearmanError::ServerError {
            code: e.code.string_representation(),
            message: e.message,
        }
    }
}

/// A pending request waiting for exactly one reply packet from the server.
pub(crate) struct PendingRequest {
    message: Option<ClientMessage>,
    reply_tx: oneshot::Sender<Result<ServerMessage, GearmanError>>,
}

pub(crate) struct SharedState<'a> {
    pub(crate) jobs: RwLock<JobTracker<'a>>,
}

impl<'a> SharedState<'a> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            jobs: RwLock::new(JobTracker::new()),
        })
    }
}

/// A cheaply cloneable handle for sending requests to a Gearman server.
///
/// All requests are serialised through an internal channel so the underlying
/// TCP connection is never accessed concurrently. Clone freely. every clone
/// shares the same connection.
#[derive(Clone)]
pub struct Client<'a> {
    cmd_tx: mpsc::Sender<PendingRequest>,
    timeout: Duration,
    state: Arc<SharedState<'a>>,
}

impl<'a> Client<'a> {
    pub(crate) async fn add_handle(&self, handle: bytes::Bytes, inner: Arc<Mutex<JobHandleInner>>) {
        self.state
            .jobs
            .write()
            .await
            .register_handle(&handle, inner);
    }
    async fn request(&self, message: ClientMessage) -> Result<ServerMessage, GearmanError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.cmd_tx
            .send(PendingRequest {
                message: Some(message),
                reply_tx,
            })
            .await
            .map_err(|_| GearmanError::ConnectionClosed)?;

        timeout(self.timeout, reply_rx)
            .await
            .map_err(|_| GearmanError::Timeout)?
            .map_err(|_| GearmanError::ConnectionClosed)?
    }

    pub async fn submit_job(&self, req: SubmitJob) -> Result<JobCreated, GearmanError> {
        match self.request(ClientMessage::SubmitJob(req)).await? {
            ServerMessage::JobCreated(r) => Ok(r),
            ServerMessage::Error(e) => Err(e.into()),
            m => Err(GearmanError::Codec(CodecError::UnexpectedMessage {
                expected: PacketType::JobCreated,
                received: m,
            })),
        }
    }

    pub async fn echo(&self, payload: Vec<u8>) -> Result<Vec<u8>, GearmanError> {
        match self
            .request(ClientMessage::Echo(Echo {
                payload: payload.into(),
            }))
            .await?
        {
            ServerMessage::EchoRes(r) => Ok(r.payload.into()),
            ServerMessage::Error(e) => Err(e.into()),
            m => Err(GearmanError::Codec(CodecError::UnexpectedMessage {
                expected: PacketType::EchoRes,
                received: m,
            })),
        }
    }

    pub async fn job_status(&self, req: GetStatus) -> Result<StatusRes, GearmanError> {
        match self.request(ClientMessage::GetStatus(req)).await? {
            ServerMessage::StatusRes(r) => Ok(r),
            ServerMessage::Error(e) => Err(e.into()),
            m => Err(GearmanError::Codec(CodecError::UnexpectedMessage {
                expected: PacketType::StatusRes,
                received: m,
            })),
        }
    }

    pub async fn job_status_unique(
        &self,
        req: GetStatusUnique,
    ) -> Result<StatusResUnique, GearmanError> {
        match self.request(ClientMessage::GetStatusUnique(req)).await? {
            ServerMessage::StatusResUnique(r) => Ok(r),
            ServerMessage::Error(e) => Err(e.into()),
            m => Err(GearmanError::Codec(CodecError::UnexpectedMessage {
                expected: PacketType::StatusResUnique,
                received: m,
            })),
        }
    }

    pub async fn set_option(&self, req: SetOption) -> Result<OptionRes, GearmanError> {
        match self.request(ClientMessage::SetOption(req)).await? {
            ServerMessage::OptionRes(res) => Ok(res),
            ServerMessage::Error(e) => Err(e.into()),
            m => Err(GearmanError::Codec(CodecError::UnexpectedMessage {
                expected: PacketType::OptionRes,
                received: m,
            })),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        cmd_tx: mpsc::Sender<PendingRequest>,
        timeout: std::time::Duration,
        state: Arc<SharedState<'a>>,
    ) -> Self {
        Self {
            timeout,
            cmd_tx,
            state,
        }
    }
}

/// Drives the connection. Must be `.run()` (or `.step()`-ed) concurrently with
/// any [`Client`] usage, typically on a spawned task.
pub struct EventLoop<'a> {
    #[cfg(not(test))]
    frame: Framed<TcpStream, GearmanCodec>,
    #[cfg(test)]
    frame: Framed<DuplexStream, GearmanCodec>,
    cmd_rx: mpsc::Receiver<PendingRequest>,
    pending: Option<PendingRequest>,
    state: Arc<SharedState<'a>>,
}

impl<'a> EventLoop<'a> {
    /// Run until the connection is closed or an unrecoverable error occurs.
    pub async fn run(mut self) -> Result<(), GearmanError> {
        loop {
            self.step().await?;
        }
    }

    /// Advance the event loop by one event (a command from a [`Client`] or an
    /// incoming packet from the server).
    pub async fn step(&mut self) -> Result<(), GearmanError> {
        tokio::select! {
            biased;

            cmd = self.cmd_rx.recv() => match cmd {
                None => return Err(GearmanError::ConnectionClosed),
                Some(req) => self.handle_command(req).await?,
            },

            msg = self.frame.next() => match msg {
                None => return Err(GearmanError::ConnectionClosed),
                Some(Err(e)) => return Err(GearmanError::Codec(e)),
                Some(Ok(msg)) => self.handle_message(msg).await,
            },
        }

        Ok(())
    }

    async fn handle_command(&mut self, mut req: PendingRequest) -> Result<(), GearmanError> {
        if self.pending.is_some() {
            let _ = req
                .reply_tx
                .send(Err(GearmanError::Codec(CodecError::RequestAlreadyInFlight)));
            return Ok(());
        }

        assert!(
            req.message.is_some(),
            "A Pending Request in the queue should always contain a payload, otherwise there is a bug in the code"
        );

        self.frame
            .send(req.message.take().unwrap())
            .await
            .map_err(GearmanError::Codec)?;

        self.pending = Some(req);
        Ok(())
    }

    async fn handle_message(&mut self, msg: ServerMessage) {
        if msg.is_async() {
            self.handle_async(msg).await;
        } else {
            self.handle_reply(msg).await;
        }
    }

    async fn handle_reply(&mut self, msg: ServerMessage) {
        match self.pending.take() {
            None => log::warn!("Received reply with no pending request: {msg:?}"),
            Some(req) => {
                if let ServerMessage::JobCreated(ref created) = msg {
                    self.register_job(&created.job_handle).await;
                }
                let _ = req.reply_tx.send(Ok(msg));
            }
        }
    }

    async fn handle_async(&mut self, msg: ServerMessage) {
        let handle = match msg.job_handle() {
            Some(h) => h.clone(),
            None => {
                log::warn!("Async message missing job handle: {msg:?}");
                return;
            }
        };

        let mut jobs = self.state.jobs.write().await;
        let Some(mut job) = jobs.get_handle_mut(&handle).await else {
            log::warn!("Update for unknown job {handle:?}");
            return;
        };

        match msg {
            ServerMessage::WorkStatus(m) => job.submit_status(m),
            ServerMessage::WorkData(m) => job.submit_data(m),
            ServerMessage::WorkWarning(m) => job.submit_warning(m),
            ServerMessage::WorkComplete(m) => {
                job.submit_complete(m);
                drop(job);
                jobs.unregister_job(&handle);
            }
            ServerMessage::WorkFail(m) => {
                job.submit_fail(m);
                drop(job);
                jobs.unregister_job(&handle);
            }
            ServerMessage::WorkException(m) => {
                job.submit_exception(m);
                drop(job);
                jobs.unregister_job(&handle);
            }
            _ => unreachable!("is_async() guarantees only work packets reach here"),
        }
    }

    async fn register_job(&self, handle: &bytes::Bytes) {
        let mut jobs = self.state.jobs.write().await;
        if !jobs.is_registered(handle) {
            jobs.register_job(handle.clone());
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        framed: Framed<DuplexStream, GearmanCodec>,
        cmd_rx: mpsc::Receiver<PendingRequest>,
        state: Arc<SharedState<'a>>,
    ) -> Self {
        Self {
            frame: framed,
            cmd_rx: cmd_rx,
            pending: None,
            state,
        }
    }
    #[cfg(test)]
    pub(crate) fn state_handle(&self) -> Arc<SharedState<'a>> {
        self.state.clone()
    }
}

pub struct Connection;

#[cfg(not(test))]
impl Connection {
    /// Connect to a Gearman server.
    ///
    /// Spawn the event loop on a task, then use the client freely:
    /// ```norun
    /// let (client, event_loop) = Connection::connect(opts).await?;
    /// tokio::spawn(async move { event_loop.run().await });
    ///
    /// client.echo(b"ping".to_vec()).await?;
    /// ```
    pub async fn connect<'a>(
        options: ConnectOptions,
    ) -> Result<(Client<'a>, EventLoop<'a>), io::Error> {
        let host = options
            .address
            .host_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Missing host in URL"))?;
        let port = options.address.port_or_known_default().unwrap_or(4730);
        let socket_addr = format!("{host}:{port}")
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "Could not resolve address")
            })?;

        let stream = timeout(options.timeout, TcpStream::connect(socket_addr))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Connection timed out"))??;

        let frame = Framed::new(stream, GearmanCodec);
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let state = SharedState::new();

        Ok((
            Client {
                cmd_tx,
                timeout: options.timeout,
                state: Arc::clone(&state),
            },
            EventLoop {
                frame,
                cmd_rx,
                pending: None,
                state,
            },
        ))
    }
}
