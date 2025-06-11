use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::task::Waker;

use bytes::Bytes;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::connection::{Client, GearmanError};
use crate::packet::IntoPacket;
use crate::request::{
    SubmitJob, SubmitJobBG, SubmitJobHigh, SubmitJobHighBG, SubmitJobLow, SubmitJobLowBG,
};
use crate::response::{WorkComplete, WorkData, WorkException, WorkFail, WorkStatus, WorkWarning};

pub enum JobPriority {
    High,
    Default,
    Low,
}

struct WaitingQueue<T> {
    sender: UnboundedSender<T>,
    receiver: UnboundedReceiver<T>,
}
impl<T> WaitingQueue<T> {
    fn new() -> Self {
        let (sender, receiver) = unbounded_channel();
        Self { sender, receiver }
    }
    fn send(&self, item: T) {
        self.sender
            .send(item)
            .expect("The channel must be open while this queue exists")
    }
    async fn read(&mut self) -> T {
        self.receiver
            .recv()
            .await
            .expect("The channel must be open while this queue exists")
    }
}

pub struct Job {
    background: bool,
    priority: JobPriority,
    payload: Bytes,
}

impl Job {
    pub fn new<P>(payload: P) -> Self
    where
        Bytes: From<P>,
    {
        let payload = Bytes::from(payload);
        Self {
            background: false,
            priority: JobPriority::Default,
            payload,
        }
    }
    pub fn set_priority(self, priority: JobPriority) -> Self {
        Self { priority, ..self }
    }
    pub fn is_background(self, is_background: bool) -> Self {
        Self {
            background: is_background,
            ..self
        }
    }
    pub async fn submit<'a, F>(
        self,
        function_name: F,
        connection: &Client<'a>,
    ) -> Result<JobHandle, GearmanError>
    where
        F: Into<String>,
    {
        let function_name: String = function_name.into();

        let uid;
        let job_packet = match self.priority {
            JobPriority::Default => {
                if !self.background {
                    let job = SubmitJob::new(function_name, self.payload);
                    uid = job.get_uid();
                    job.to_packet()
                } else {
                    let job = SubmitJobBG::new(function_name, self.payload);
                    uid = job.get_uid();
                    job.to_packet()
                }
            }
            JobPriority::High => {
                if !self.background {
                    let job = SubmitJobHigh::new(function_name, self.payload);
                    uid = job.get_uid();
                    job.to_packet()
                } else {
                    let job = SubmitJobHighBG::new(function_name, self.payload);
                    uid = job.get_uid();
                    job.to_packet()
                }
            }
            JobPriority::Low => {
                if !self.background {
                    let job = SubmitJobLow::new(function_name, self.payload);
                    uid = job.get_uid();
                    job.to_packet()
                } else {
                    let job = SubmitJobLowBG::new(function_name, self.payload);
                    uid = job.get_uid();
                    job.to_packet()
                }
            }
        };
        let handle = connection.submit_job(job_packet).await?.take_handle();
        let waker = Arc::new(StdMutex::new(None));
        let (status_sender, status_receiver) = tokio::sync::watch::channel(JobStatus::Working);
        let job_handle = JobHandle {
            inner: Arc::new(Mutex::new(JobHandleInner {
                handle: handle.clone(),
                uid,
                waker: waker.clone(),
                status_sender,
                ..JobHandleInner::default()
            })),
            status_receiver,
            waker: waker.clone(),
        };

        connection
            .conn
            .jobs
            .write()
            .await
            .register_handle(&handle, job_handle.inner.clone());

        Ok(job_handle)
    }
}

#[derive(Debug)]
pub enum JobResult {
    WorkFail,
    WorkException(WorkException),
    WorkComplete(WorkComplete),
}
#[derive(Debug, Clone)]
enum JobStatus {
    WorkFail,
    WorkException(WorkException),
    WorkComplete(WorkComplete),
    Working,
}

#[derive(Debug)]
pub enum JobError {
    Server { code: i32, message: String },
    Worker { code: i32, message: String },
}
impl std::error::Error for JobError {}
impl std::fmt::Display for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Server { code, message } => {
                write!(f, "Server responded with error code {}: {}", code, message)
            }
            Self::Worker { code, message } => {
                write!(f, "Worker responded with error code {}: {}", code, message)
            }
        }
    }
}

pub enum JobProgress {
    Percentile { numerator: u64, denominator: u64 },
    Unknown,
}
impl JobProgress {
    /// returns a float between 0 and 1
    ///
    /// If the status is unknown, 0 is returned
    pub fn as_float(&self) -> f32 {
        match self {
            Self::Percentile {
                numerator,
                denominator,
            } => *numerator as f32 / *denominator as f32,
            Self::Unknown => 0.,
        }
    }
}

pub struct JobHandle {
    inner: Arc<Mutex<JobHandleInner>>,
    waker: Arc<StdMutex<Option<Waker>>>,
    status_receiver: tokio::sync::watch::Receiver<JobStatus>,
}

#[allow(unused)]
impl JobHandle {
    // TODO: add status updates
    async fn get_status<'a>(&self, connection: &Client<'a>) -> WorkStatus {
        self.inner.lock().await.get_status(connection).await
    }
    // TODO: add unique status updates
    async fn get_status_unique<'a>(&self, connection: &Client<'a>) -> WorkStatus {
        self.inner.lock().await.get_status_unique(connection).await
    }
    // TODO: If both of these are called with Tokio select then they will deadlock each other.
    // Probably better to replace this with a RwLock!!
    async fn next_warning(&self) -> WorkWarning {
        self.inner.lock().await.next_warning().await
    }
    async fn next_data(&self) -> WorkData {
        self.inner.lock().await.next_data().await
    }
}

impl Future for JobHandle {
    type Output = JobResult;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let mut waker = this.waker.lock().expect("This lock should not be poisoned");
        match *waker {
            None => *waker = Some(cx.waker().clone()),
            Some(_) => {}
        }

        match this.status_receiver.borrow_and_update().deref() {
            JobStatus::WorkFail => std::task::Poll::Ready(JobResult::WorkFail),
            JobStatus::WorkException(exception) => {
                std::task::Poll::Ready(JobResult::WorkException(exception.clone()))
            }
            JobStatus::WorkComplete(result) => {
                std::task::Poll::Ready(JobResult::WorkComplete(result.clone()))
            }
            JobStatus::Working => std::task::Poll::Pending,
        }
    }
}

impl JobHandleInner {
    pub(crate) async fn get_status<'a>(&self, _connection: &Client<'a>) -> WorkStatus {
        // let req = GetStatus::new(self.get_handle().await.clone());
        // connection.submit_status(req).await;
        todo!()
    }
    pub(crate) async fn get_status_unique<'a>(&self, _connection: &Client<'a>) -> WorkStatus {
        // let req = GetStatusUnique::new(self.get_uid().await.clone());
        // connection.submit_status_unique(req).await;
        todo!()
    }
    pub(crate) async fn next_warning(&mut self) -> WorkWarning {
        self.warnings.read().await
    }
    pub(crate) async fn next_data(&mut self) -> WorkData {
        self.data.read().await
    }
    pub(crate) fn submit_status(&mut self, status: WorkStatus) {
        self.progress = JobProgress::Percentile {
            numerator: status.numerator(),
            denominator: status.denominator(),
        }
    }
    pub(crate) fn submit_complete(&mut self, complete: WorkComplete) {
        let new_status = JobStatus::WorkComplete(complete.clone());
        self.status = new_status.clone();
        self.status_sender
            .send(new_status)
            .expect("This watch must not be closed before the job is complete");
        if let Some(ref waker) = *self.waker.lock().expect("This should work") {
            waker.wake_by_ref();
        }
    }
    pub(crate) fn submit_fail(&mut self, _fail: WorkFail) {
        let new_status = JobStatus::WorkFail;
        self.status = new_status.clone();
        self.status_sender
            .send(new_status)
            .expect("This watch must not be closed before the job is complete");
        if let Some(ref waker) = *self.waker.lock().expect("This should work") {
            waker.wake_by_ref();
        }
    }
    pub(crate) fn submit_exception(&mut self, exception: WorkException) {
        let new_status = JobStatus::WorkException(exception);
        self.status = new_status.clone();
        self.status_sender
            .send(new_status)
            .expect("This watch must not be closed before the job is complete");
        if let Some(ref waker) = *self.waker.lock().expect("This should work") {
            waker.wake_by_ref();
        }
    }
    pub(crate) fn submit_warning(&self, warning: WorkWarning) {
        self.warnings.send(warning);
    }
    pub(crate) fn submit_data(&self, data: WorkData) {
        self.data.send(data);
    }
}

pub(crate) struct JobHandleInner {
    #[allow(unused)]
    handle: Bytes,
    #[allow(unused)]
    uid: String,
    warnings: WaitingQueue<WorkWarning>,
    data: WaitingQueue<WorkData>,
    status: JobStatus,
    progress: JobProgress,
    status_sender: tokio::sync::watch::Sender<JobStatus>,
    waker: Arc<StdMutex<Option<Waker>>>,
}

impl Default for JobHandleInner {
    fn default() -> Self {
        Self {
            handle: Bytes::new(),
            uid: String::from(""),
            warnings: WaitingQueue::new(),
            data: WaitingQueue::new(),
            status: JobStatus::Working,
            progress: JobProgress::Unknown,
            status_sender: tokio::sync::watch::channel(JobStatus::Working).0,
            waker: Arc::new(StdMutex::new(None)),
        }
    }
}
