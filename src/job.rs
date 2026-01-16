use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::task::Poll;
use std::task::Waker;

use bytes::Bytes;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;

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
    async fn try_read(&mut self) -> Option<T> {
        match self.receiver.try_recv() {
            Err(TryRecvError::Empty) => return None,
            Err(TryRecvError::Disconnected) => {
                panic!("The channel must be open while this queue exists")
            }
            Ok(v) => Some(v),
        }
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
        self.inner_submit(function_name, connection, None).await
    }

    pub async fn submit_with_timeout<'a, F>(
        self,
        function_name: F,
        connection: &Client<'a>,
        timeout: std::time::Duration,
    ) -> Result<JobHandle, GearmanError>
    where
        F: Into<String>,
    {
        self.inner_submit(function_name, connection, Some(timeout))
            .await
    }

    async fn inner_submit<'a, F>(
        self,
        function_name: F,
        connection: &Client<'a>,
        timeout: Option<std::time::Duration>,
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
        let handle = connection
            .submit_job(job_packet, timeout)
            .await?
            .take_handle();
        let waker = Arc::new(StdMutex::new(None));
        let status = Arc::new(StdMutex::new(JobStatus::Working));
        let job_handle = JobHandle {
            inner: Arc::new(
                JobHandleInner {
                    handle: handle.clone(),
                    uid,
                    waker: waker.clone(),
                    status: status.clone(),
                    ..JobHandleInner::default()
                }
                .into(),
            ),
            status: status.clone(),
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

#[derive(Debug, Clone)]
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

    status: Arc<StdMutex<JobStatus>>,
}

#[allow(unused)]
impl JobHandle {
    // TODO: add status updates
    async fn get_status<'a>(&self, connection: &Client<'a>) -> WorkStatus {
        // self.inner.lock().await.get_status(connection).await
        todo!()
    }
    // TODO: add unique status updates
    async fn get_status_unique<'a>(&self, connection: &Client<'a>) -> WorkStatus {
        // self.inner.lock().await.get_status_unique(connection).await
        todo!()
    }
    // TODO: If both of these are called with Tokio select then they will deadlock each other.
    // Probably better to replace this with a RwLock!!
    async fn next_warning(&self) -> WorkWarning {
        loop {
            let mut lock = self.inner.lock().await;
            if let Some(warning) = lock.try_next_warning().await {
                drop(lock);
                return warning;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
    async fn next_data(&self) -> WorkData {
        loop {
            let mut lock = self.inner.lock().await;
            if let Some(data) = lock.try_next_data().await {
                drop(lock);
                return data;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
    pub fn try_poll(&self) -> Poll<JobResult> {
        match self.status.lock().expect("Result lock is poisoned").deref() {
            JobStatus::Working => Poll::Pending,
            JobStatus::WorkComplete(res) => Poll::Ready(JobResult::WorkComplete(res.clone())),
            JobStatus::WorkException(res) => Poll::Ready(JobResult::WorkException(res.clone())),
            JobStatus::WorkFail => Poll::Ready(JobResult::WorkFail),
        }
    }
}

impl Future for JobHandle {
    type Output = JobResult;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match this.status.lock().expect("Result lock is poisoned").deref() {
            JobStatus::Working => (),
            JobStatus::WorkComplete(res) => {
                return Poll::Ready(JobResult::WorkComplete(res.clone()))
            }
            JobStatus::WorkException(res) => {
                return Poll::Ready(JobResult::WorkException(res.clone()))
            }
            JobStatus::WorkFail => return Poll::Ready(JobResult::WorkFail),
        };

        // Register waker
        let mut waker = this.waker.lock().unwrap();
        match &*waker {
            Some(existing) if existing.will_wake(cx.waker()) => {}
            _ => *waker = Some(cx.waker().clone()),
        }

        // Still not complete
        Poll::Pending
    }
}

impl JobHandleInner {
    #[allow(unused)] // TODO
    pub(crate) async fn get_status<'a>(&self, _connection: &Client<'a>) -> WorkStatus {
        // let req = GetStatus::new(self.get_handle().await.clone());
        // connection.submit_status(req).await;
        todo!()
    }
    #[allow(unused)] // TODO
    pub(crate) async fn get_status_unique<'a>(&self, _connection: &Client<'a>) -> WorkStatus {
        // let req = GetStatusUnique::new(self.get_uid().await.clone());
        // connection.submit_status_unique(req).await;
        todo!()
    }
    pub(crate) async fn try_next_warning(&mut self) -> Option<WorkWarning> {
        self.warnings.try_read().await
    }
    pub(crate) async fn try_next_data(&mut self) -> Option<WorkData> {
        self.data.try_read().await
    }
    pub(crate) fn submit_status(&mut self, status: WorkStatus) {
        self.progress = JobProgress::Percentile {
            numerator: status.numerator(),
            denominator: status.denominator(),
        }
    }
    pub(crate) fn submit_complete(&mut self, complete: WorkComplete) {
        let new_status = JobStatus::WorkComplete(complete.clone());
        *self.status.lock().expect("The status lock is poisoned") = new_status.clone();

        if let Some(ref waker) = *self.waker.lock().expect("The waker lock is poisoned") {
            waker.wake_by_ref();
        }
    }
    pub(crate) fn submit_fail(&mut self, _fail: WorkFail) {
        let new_status = JobStatus::WorkFail;
        *self.status.lock().expect("The status lock is poisoned") = new_status.clone();

        if let Some(ref waker) = *self.waker.lock().expect("The waker lock is poisoned") {
            waker.wake_by_ref();
        }
    }
    pub(crate) fn submit_exception(&mut self, exception: WorkException) {
        let new_status = JobStatus::WorkException(exception);
        *self.status.lock().expect("The status lock is poisoned") = new_status.clone();

        if let Some(ref waker) = *self.waker.lock().expect("The waker lock is poisoned") {
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
    status: Arc<StdMutex<JobStatus>>,
    progress: JobProgress,
    waker: Arc<StdMutex<Option<Waker>>>,
}

impl Default for JobHandleInner {
    fn default() -> Self {
        Self {
            handle: Bytes::new(),
            uid: String::from(""),
            warnings: WaitingQueue::new(),
            data: WaitingQueue::new(),
            status: Arc::new(JobStatus::Working.into()),
            progress: JobProgress::Unknown,
            waker: Arc::new(None.into()),
        }
    }
}
