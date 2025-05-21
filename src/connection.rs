use std::convert::TryFrom;
use std::io;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify, RwLock, oneshot};
use tokio::time::timeout;
use url::Url;

use crate::job_tracker::JobTracker;
use crate::packet::{FromPacket, IntoPacket, Packet, PacketError, PacketType, ParseError};
use crate::packet_stream::{GearmanPacketReader, GearmanPacketSender, packet_stream};
use crate::request::{EchoReq, GetStatus, GetStatusUnique, OptionReq};
use crate::response::{EchoRes, Error, JobCreated, OptionRes, StatusRes, StatusResUnique};
use crate::response::{WorkComplete, WorkData, WorkException, WorkFail, WorkStatus, WorkWarning};

pub struct ConnectOptions {
    pub address: Url,
    pub timeout: Duration,
}

impl ConnectOptions {
    pub fn new<T>(address: T) -> Result<Self, <Url as TryFrom<T>>::Error>
    where
        Url: TryFrom<T>,
    {
        Ok(Self {
            address: Url::try_from(address)?,
            timeout: Duration::from_secs(300),
        })
    }

    pub fn with_timeout(mut self, dur: Duration) -> Self {
        self.timeout = dur;
        self
    }
}

#[derive(Debug)]
pub enum GearmanError {
    ServerError(Error),
    IoError(io::Error),
    ParseError(ParseError),
    InvalidPacket(PacketError),
    Timeout,
    ConnectionClosed,
}

impl std::fmt::Display for GearmanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GearmanError::ServerError(msg) => write!(f, "Server error: {}", msg),
            GearmanError::IoError(err) => write!(f, "IO error: {}", err),
            GearmanError::ParseError(err) => write!(f, "Unable to Parse Packet: {}", err),
            GearmanError::InvalidPacket(err) => write!(f, "Invalid Packet: {}", err),
            GearmanError::ConnectionClosed => write!(f, "Connection closed"),
            GearmanError::Timeout => write!(f, "Operation timed out"),
        }
    }
}

impl std::error::Error for GearmanError {}

impl From<std::io::Error> for GearmanError {
    fn from(err: std::io::Error) -> Self {
        GearmanError::IoError(err)
    }
}

impl From<tokio::time::error::Elapsed> for GearmanError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        GearmanError::Timeout
    }
}

#[derive(PartialEq, Debug)]
enum WaitingType {
    OptionRes,
    StatusResUnique,
    JobCreated,
    EchoRes,
    StatusRes,
}

#[derive(Debug)]
enum WaitingResult {
    Error(Packet),
    Valid(Packet),
}

#[derive(Debug)]
struct WaitingJob {
    waiting_type: WaitingType,
    waiting_job: oneshot::Sender<WaitingResult>,
}
impl WaitingJob {
    fn new(sender: oneshot::Sender<WaitingResult>, waiting_type: WaitingType) -> Self {
        Self {
            waiting_type,
            waiting_job: sender,
        }
    }
    fn submit(self, packet: Packet) -> Result<(), WaitingResult> {
        self.waiting_job.send(WaitingResult::Valid(packet))
    }

    fn submit_error(self, error: Packet) -> Result<(), WaitingResult> {
        self.waiting_job.send(WaitingResult::Error(error))
    }
    fn is_type(&self, other: WaitingType) -> bool {
        self.waiting_type == other
    }
}

pub struct Connection<'a> {
    pub(crate) jobs: RwLock<JobTracker<'a>>,
    ready: Notify,
    waiting: RwLock<Option<WaitingJob>>,
}

impl<'a> Connection<'a> {
    /// Connect to a Gearman Server via Tcp
    ///
    /// This function will fail if the url is invalid or the connection timeout is reached
    pub async fn connect(
        options: ConnectOptions,
    ) -> Result<(Client<'a>, ClientLoop<'a>), std::io::Error> {
        // Parse and validate address
        let url = options.address;

        let host = url.host_str().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "Missing host in URL")
        })?;

        let port = url.port_or_known_default().unwrap_or(4730);

        let addr = format!("{}:{}", host, port);
        let mut addrs = addr.to_socket_addrs()?;

        let stream = match timeout(options.timeout, TcpStream::connect(addrs.next().unwrap())).await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Connection timed out",
                ));
            }
        };

        let (reader, writer) = packet_stream(stream, options.timeout);
        let connection = Arc::new(Self {
            jobs: RwLock::new(JobTracker::new()),
            ready: Notify::new(),
            waiting: RwLock::new(None),
        });

        Ok((
            Client {
                writer: Arc::new(Mutex::new(writer)),
                conn: Arc::clone(&connection),
            },
            ClientLoop {
                reader,
                conn: Arc::clone(&connection),
            },
        ))
    }
}

pub struct ClientLoop<'a> {
    reader: GearmanPacketReader,
    conn: Arc<Connection<'a>>,
}

impl ClientLoop<'_> {
    /// This progresses the runner by one step. This should be run in a continous loop
    /// to listen for any incoming packages and parse them
    pub async fn step(&mut self) -> Result<(), GearmanError> {
        while let Ok(packet) = self.reader.read_packet().await {
            match packet.header().get_type() {
                PacketType::JobCreated => {
                    let read_lock = self.conn.waiting.read().await;
                    let waiting = match *read_lock {
                        Some(ref _waiting) if _waiting.is_type(WaitingType::JobCreated) => {
                            drop(read_lock);
                            self.conn.waiting.write().await.take().expect(
                                "This option must have a value since it was previously read",
                            )
                        }
                        _ => continue,
                    };
                    match JobCreated::from_packet(packet.clone()) {
                        Ok(job_created) => {
                            let handle = job_created.take_handle();
                            let mut jobs = self.conn.jobs.write().await;
                            if !jobs.is_registered(&handle) {
                                jobs.register_job(handle);
                            }
                        }
                        Err(_) => {}
                    }
                    waiting.submit(packet).expect("This waiting job must exist");
                    self.conn.ready.notify_one();
                }
                PacketType::EchoRes => {
                    let read_lock = self.conn.waiting.read().await;
                    let waiting = match *read_lock {
                        Some(ref _waiting) if _waiting.is_type(WaitingType::EchoRes) => {
                            drop(read_lock);
                            self.conn.waiting.write().await.take().expect(
                                "This option must have a value since it was previously read",
                            )
                        }
                        _ => continue,
                    };
                    waiting.submit(packet).expect("This waiting job must exist");
                    self.conn.ready.notify_one();
                }
                PacketType::OptionRes => {
                    let read_lock = self.conn.waiting.read().await;
                    let waiting = match *read_lock {
                        Some(ref _waiting) if _waiting.is_type(WaitingType::OptionRes) => {
                            drop(read_lock);
                            self.conn.waiting.write().await.take().expect(
                                "This option must have a value since it was previously read",
                            )
                        }
                        _ => continue,
                    };
                    waiting.submit(packet).expect("This waiting job must exist");
                    self.conn.ready.notify_one();
                }
                PacketType::StatusRes => {
                    let read_lock = self.conn.waiting.read().await;
                    let waiting = match *read_lock {
                        Some(ref _waiting) if _waiting.is_type(WaitingType::StatusRes) => {
                            drop(read_lock);
                            self.conn.waiting.write().await.take().expect(
                                "This option must have a value since it was previously read",
                            )
                        }
                        _ => continue,
                    };
                    waiting.submit(packet).expect("This waiting job must exist");
                    self.conn.ready.notify_one();
                }
                PacketType::StatusResUnique => {
                    let read_lock = self.conn.waiting.read().await;
                    let waiting = match *read_lock {
                        Some(ref _waiting) if _waiting.is_type(WaitingType::StatusResUnique) => {
                            drop(read_lock);
                            self.conn.waiting.write().await.take().expect(
                                "This option must have a value since it was previously read",
                            )
                        }
                        _ => continue,
                    };
                    waiting.submit(packet).expect("This waiting job must exist");
                    self.conn.ready.notify_one();
                }
                PacketType::Error => {
                    let read_lock = self.conn.waiting.read().await;
                    if read_lock.is_some() {
                        drop(read_lock);
                        let waiting =
                            self.conn.waiting.write().await.take().expect(
                                "This option must have a value since it was previously read",
                            );
                        let _ = waiting.submit_error(packet);
                    }
                    self.conn.ready.notify_one();
                }
                PacketType::WorkStatus => {
                    let work_status = match WorkStatus::from_packet(packet).ok() {
                        Some(work_status) => work_status,
                        None => continue,
                    };
                    let job_handle_lock = self.conn.jobs.write().await;
                    let mut job_handle = job_handle_lock
                        .get_handle_mut(work_status.get_job_handle())
                        .await
                        .expect("Job handle lock is owned here");
                    job_handle.submit_status(work_status);
                    drop(job_handle);
                }
                PacketType::WorkComplete => {
                    let work_complete = match WorkComplete::from_packet(packet).ok() {
                        Some(work_complete) => work_complete,
                        None => continue,
                    };
                    let mut job_handle_lock = self.conn.jobs.write().await;
                    let mut job_handle = job_handle_lock
                        .get_handle_mut(work_complete.get_job_handle())
                        .await
                        .expect("Job handle lock is owned here");
                    job_handle.submit_complete(work_complete.clone());
                    println!("got here 444");
                    drop(job_handle);
                    job_handle_lock.unregister_job(work_complete.get_job_handle());
                }
                PacketType::WorkFail => {
                    let work_fail = match WorkFail::from_packet(packet).ok() {
                        Some(work_fail) => work_fail,
                        None => continue,
                    };
                    let mut job_handle_lock = self.conn.jobs.write().await;
                    let mut job_handle = job_handle_lock
                        .get_handle_mut(work_fail.get_job_handle())
                        .await
                        .expect("Job handle lock is owned here");
                    job_handle.submit_fail(work_fail.clone());
                    drop(job_handle);
                    job_handle_lock.unregister_job(work_fail.get_job_handle());
                }
                PacketType::WorkException => {
                    let work_exception = match WorkException::from_packet(packet).ok() {
                        Some(work_exception) => work_exception,
                        None => continue,
                    };
                    let mut job_handle_lock = self.conn.jobs.write().await;
                    let mut job_handle = job_handle_lock
                        .get_handle_mut(work_exception.get_job_handle())
                        .await
                        .expect("Job handle lock is owned here");
                    job_handle.submit_exception(work_exception.clone());
                    drop(job_handle);
                    job_handle_lock.unregister_job(work_exception.get_job_handle());
                }
                PacketType::WorkData => {
                    let work_data = match WorkData::from_packet(packet).ok() {
                        Some(work_data) => work_data,
                        None => continue,
                    };
                    let job_handle_lock = self.conn.jobs.write().await;
                    let job_handle = job_handle_lock
                        .get_handle_mut(work_data.get_job_handle())
                        .await
                        .expect("Job handle lock is owned here");
                    job_handle.submit_data(work_data);
                }
                PacketType::WorkWarning => {
                    let work_warning = match WorkWarning::from_packet(packet).ok() {
                        Some(work_warning) => work_warning,
                        None => continue,
                    };
                    let job_handle_lock = self.conn.jobs.write().await;
                    let job_handle = job_handle_lock
                        .get_handle_mut(work_warning.get_job_handle())
                        .await
                        .expect("Job handle lock is owned here");
                    job_handle.submit_warning(work_warning);
                }
                _ => {
                    #[cfg(test)]
                    eprintln!(
                        "Unexpected packet type or no waiting job: {:?}",
                        packet.header().get_type()
                    );
                    continue;
                }
            }
        }

        Ok(())
    }
}

pub struct Client<'a> {
    writer: Arc<Mutex<GearmanPacketSender>>,
    pub(crate) conn: Arc<Connection<'a>>,
}

impl Client<'_> {
    pub(super) async fn submit_job(&self, packet: Packet) -> Result<JobCreated, GearmanError> {
        let (sender, receiver) = oneshot::channel();
        if self.conn.waiting.read().await.is_some() {
            self.conn.ready.notified().await;
        }
        *self.conn.waiting.write().await = Some(WaitingJob::new(sender, WaitingType::JobCreated));
        self.write_packet(packet).await?;
        let waiting_result = receiver.await.expect("This channel should only close then the JobCreated command or an error is returned by the server");
        match waiting_result {
            WaitingResult::Valid(packet) => {
                JobCreated::from_packet(packet).map_err(|err| GearmanError::ParseError(err))
            }
            WaitingResult::Error(err_packet) => {
                let error =
                    Error::from_packet(err_packet).map_err(|err| GearmanError::ParseError(err))?;
                Err(GearmanError::ServerError(error))
            }
        }
    }

    pub(super) async fn submit_echo(&self, packet: EchoReq) -> Result<EchoRes, GearmanError> {
        let (sender, receiver) = oneshot::channel();
        if self.conn.waiting.read().await.is_some() {
            self.conn.ready.notified().await;
        }
        *self.conn.waiting.write().await = Some(WaitingJob::new(sender, WaitingType::EchoRes));
        self.write_packet(packet.to_packet()).await?;
        let waiting_result = receiver.await.expect("This channel should only close then the JobCreated command or an error is returned by the server");
        match waiting_result {
            WaitingResult::Valid(packet) => {
                EchoRes::from_packet(packet).map_err(|err| GearmanError::ParseError(err))
            }
            WaitingResult::Error(err_packet) => {
                let error =
                    Error::from_packet(err_packet).map_err(|err| GearmanError::ParseError(err))?;
                Err(GearmanError::ServerError(error))
            }
        }
    }

    #[allow(unused)] // TODO: add OptionReq
    pub(super) async fn submit_option(&self, packet: OptionReq) -> Result<OptionRes, GearmanError> {
        let (sender, receiver) = oneshot::channel();
        if self.conn.waiting.read().await.is_some() {
            self.conn.ready.notified().await;
        }
        *self.conn.waiting.write().await = Some(WaitingJob::new(sender, WaitingType::OptionRes));
        self.write_packet(packet.to_packet()).await?;
        let waiting_result = receiver.await.expect("This channel should only close then the JobCreated command or an error is returned by the server");
        match waiting_result {
            WaitingResult::Valid(packet) => {
                OptionRes::from_packet(packet).map_err(|err| GearmanError::ParseError(err))
            }
            WaitingResult::Error(err_packet) => {
                let error =
                    Error::from_packet(err_packet).map_err(|err| GearmanError::ParseError(err))?;
                Err(GearmanError::ServerError(error))
            }
        }
    }

    #[allow(unused)] // TODO: add StatusReq
    pub(super) async fn submit_status(&self, packet: GetStatus) -> Result<StatusRes, GearmanError> {
        let (sender, receiver) = oneshot::channel();
        if self.conn.waiting.read().await.is_some() {
            self.conn.ready.notified().await;
        }
        *self.conn.waiting.write().await = Some(WaitingJob::new(sender, WaitingType::StatusRes));
        self.write_packet(packet.to_packet()).await?;
        let waiting_result = receiver.await.expect("This channel should only close then the JobCreated command or an error is returned by the server");
        match waiting_result {
            WaitingResult::Valid(packet) => {
                StatusRes::from_packet(packet).map_err(|err| GearmanError::ParseError(err))
            }
            WaitingResult::Error(err_packet) => {
                let error =
                    Error::from_packet(err_packet).map_err(|err| GearmanError::ParseError(err))?;
                Err(GearmanError::ServerError(error))
            }
        }
    }

    #[allow(unused)] // TODO: add StatusReqUnique
    pub(super) async fn submit_status_unique(
        &self,
        packet: GetStatusUnique,
    ) -> Result<StatusResUnique, GearmanError> {
        let (sender, receiver) = oneshot::channel();
        if self.conn.waiting.read().await.is_some() {
            self.conn.ready.notified().await;
        }
        self.write_packet(packet.to_packet()).await?;
        *self.conn.waiting.write().await =
            Some(WaitingJob::new(sender, WaitingType::StatusResUnique));
        let waiting_result = receiver.await.expect("This channel should only close then the JobCreated command or an error is returned by the server");
        match waiting_result {
            WaitingResult::Valid(packet) => {
                StatusResUnique::from_packet(packet).map_err(|err| GearmanError::ParseError(err))
            }
            WaitingResult::Error(err_packet) => {
                let error =
                    Error::from_packet(err_packet).map_err(|err| GearmanError::ParseError(err))?;
                Err(GearmanError::ServerError(error))
            }
        }
    }

    async fn write_packet(&self, packet: Packet) -> Result<(), GearmanError> {
        let mut writer_lock = self.writer.lock().await;
        writer_lock.send_packet(&packet).await
    }
}

impl Clone for Client<'_> {
    fn clone(&self) -> Self {
        Self {
            writer: Arc::clone(&self.writer),
            conn: Arc::clone(&self.conn),
        }
    }
}
