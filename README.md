# Gearrs - A Rust Gearman Client

`gearrs` is an asynchronous Gearman client library for Rust. Gearman is a job server system that allows distribution of workloads to a number of workers. This library is an implementation of the Gearman Client protocol which allows interaction with Gearman servers. The client manages jobs, it can receive warnings from the server as well as status updates and waits for responses asynchronousy.

## Features
### Supported Protocol Features

- **GearmanJobs**: Support for job submission with **priority** and **background** options.
- **EchoRequest**: Echo functionality for testing communication

### Upcoming Features

- **OptionsReq**: Setting Server options
- **StatusReq**: Allows querying the status of jobs.
- **Scheduled Jobs**: scheduling jobs 

## Example Usage

Here’s an example of how to use the `gearrs` library to submit a job to a Gearman server:

```rust
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use gearrs::job::JobPriority;
use gearrs::Job;
use gearrs::{ConnectOptions, Connection};

#[tokio::main(flavor = "current_thread")]
async fn main() {

    let conn_options = ConnectOptions::new("gearman://127.0.0.1")
        .expect("invalid address")
        .with_timeout(Duration::from_secs(30));
    let (mut client, mut client_loop) = Connection::connect(conn_options)
        .await
        .expect("Failed to connect to server");

    let is_running = Arc::new(AtomicBool::new(true));
    let is_running_clone = Arc::clone(&is_running);
    let client_loop_handle = tokio::spawn(async move {
        while is_running_clone.load(Ordering::Relaxed) {
            client_loop.step().await.expect("Failed to receive new responses")
        }
    });

    let job = Job::new("this payload will be sent to the worker").set_priority(JobPriority::Default).is_background(false);

    let handle = job.submit("worker_function", &mut client).await.expect("Server responded with error");

    match 
        handle.await {
            gearrs::job::JobResult::WorkFail => println!("Worker failed"),
            gearrs::job::JobResult::WorkException(excpetion) => println!("Worker returned exception: {:?}", excpetion.get_opaque_response()),
            gearrs::job::JobResult::WorkComplete(result) => println!("Worker returned bytes: {:?}", result.get_opaque_response())
    }

    is_running.store(false, Ordering::Relaxed);
    client_loop_handle.await.unwrap()
}
```

This example demonstrates a simple job submission where we connect to the Gearman server at 127.0.0.1:4730, submit a job, and print the result.

## License

This project is licensed under either the MIT License or the Apache License 2.0, at your option. See the LICENSE-MIT or LICENSE-APACHE files for details.

## About Gearman

Gearman is a job server that distributes work to multiple worker systems, allowing you to build scalable applications that can perform tasks concurrently. It's especially useful for systems that require job queues, parallel processing, and load balancing.
