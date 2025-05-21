use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Mutex, MutexGuard};

use crate::job::JobHandleInner;

pub struct JobTracker<'a> {
    jobs: HashMap<Bytes, Option<Arc<Mutex<JobHandleInner>>>>,
    marker: PhantomData<&'a ()>,
}

impl<'a> JobTracker<'a> {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
            marker: PhantomData,
        }
    }
    pub fn register_job(&mut self, handle: Bytes) {
        match self.jobs.entry(handle.clone()) {
            std::collections::hash_map::Entry::Occupied(_) => {}
            std::collections::hash_map::Entry::Vacant(_) => {
                self.jobs.insert(handle, None);
            }
        }
    }
    pub fn is_registered(&self, handle: &Bytes) -> bool {
        self.jobs.get(handle).is_some()
    }
    pub fn unregister_job(&mut self, handle: &Bytes) {
        let _ = self.jobs.remove(handle);
    }

    /// Adds a JobHandle to a handle in this tracker.
    /// - If the handle exists, but has no JobHandle, the JobHandle is added.
    /// - If there is already a JobHandle associated with the handle, the JobHandle is ignored.
    /// - If the handle doesn't exist yet, it will be created
    pub fn register_handle(&mut self, handle: &Bytes, job_handle: Arc<Mutex<JobHandleInner>>) {
        match self.jobs.entry(handle.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) if entry.get().is_some() => {}
            std::collections::hash_map::Entry::Occupied(_entry) => {
                self.jobs.insert(handle.clone(), Some(job_handle));
            }
            std::collections::hash_map::Entry::Vacant(_) => {
                self.jobs.insert(handle.clone(), Some(job_handle));
            }
        }
    }

    pub async fn try_get_handle_mut<'b>(
        &'b self,
        handle: &Bytes,
    ) -> Option<MutexGuard<'b, JobHandleInner>> {
        match self.jobs.get(handle)? {
            Some(handle) => Some(handle.lock().await),
            None => None,
        }
    }
    pub async fn get_handle_mut<'b>(
        &'b self,
        handle: &Bytes,
    ) -> Option<MutexGuard<'b, JobHandleInner>> {
        loop {
            if self.jobs.get(handle).is_some() {
                break self.try_get_handle_mut(handle).await;
            }
        }
    }
}
