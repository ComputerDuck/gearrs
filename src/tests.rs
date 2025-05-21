#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use tokio::net::TcpListener;

    use crate::Echo;
    use crate::Job;
    use crate::packet::FromPacket;
    use crate::packet::IntoPacket;
    use crate::packet_stream::packet_stream;
    use crate::request::EchoReq;
    use crate::request::SubmitJob;
    use crate::response::EchoRes;
    use crate::response::JobCreated;
    use crate::response::WorkComplete;
    use crate::{ConnectOptions, Connection};

    #[tokio::test]
    async fn submit_echo_req() {
        let should_run = Arc::new(AtomicBool::new(true));
        let server_running = Arc::new(AtomicBool::new(false));

        let server_running_for_server = Arc::clone(&server_running);
        let server_handle = tokio::task::spawn(async move {
            let server = TcpListener::bind("127.0.0.1:15000")
                .await
                .expect("Failed to initilaize server");
            server_running_for_server.store(true, Ordering::Relaxed);

            let connection = server.accept().await.expect("Connection unsuccessful");

            let (mut packet_reader, mut packet_writer) =
                packet_stream(connection.0, Duration::from_secs(30));

            println!("ST: connection set up");
            let echo_req = EchoReq::from_packet(
                packet_reader
                    .read_packet()
                    .await
                    .expect("The echo packet must be sent before the connection closes"),
            )
            .expect("Failed to parse EchoReq packet");

            println!("ST: got echo_req");

            let echo_res = EchoRes::new(echo_req.as_payload());

            packet_writer
                .send_packet(&echo_res.to_packet())
                .await
                .expect("Failed to return packet");
            println!("ST: sent echo_res");
        });

        while !server_running.load(Ordering::Relaxed) {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let connection_options = ConnectOptions::new("gearman://127.0.0.1:15000")
            .expect("Failed to create conn options");
        let (client, mut client_loop) = Connection::connect(connection_options)
            .await
            .expect("failed to connect to client");

        println!("MT: connected to server");

        let should_run_for_runner = Arc::clone(&should_run);
        let runner_handle = tokio::task::spawn(async move {
            while should_run_for_runner.load(Ordering::Relaxed) {
                client_loop.step().await.expect("Failed runner step");
            }
            println!("RL: finished runner");
        });

        println!("MT: started runner");

        let echo = Echo::new("Echo this");
        let res_fut = echo.submit(&client);
        println!("MT: submitted echo req");

        let res = res_fut.await.expect("This echo should not fail haha");

        println!("MT: submitted echo req");

        res.validate("Echo this")
            .expect("Response bytes not equal to request bytes");

        println!("MT: received valid echo_res");

        should_run.store(false, Ordering::Relaxed);
        runner_handle.await.expect("Failed to join threads");
        server_handle.await.expect("Failed to join threads");

        println!("MT: cleaned up");
    }

    #[tokio::test]
    async fn submit_create_job_req() {
        static JOB_HANDLE: &'static [u8] = "test_job_handle".as_bytes();
        static TEST_PAYLOAD: &'static [u8] = "test".as_bytes();
        static TEST_FUNCTION_NAME: &'static str = "test";

        let should_run = Arc::new(AtomicBool::new(true));
        let server_running = Arc::new(AtomicBool::new(false));

        let server_running_for_server = Arc::clone(&server_running);
        let server_handle = tokio::task::spawn(async move {
            let server = TcpListener::bind("127.0.0.1:15001")
                .await
                .expect("Failed to initilaize server");
            server_running_for_server.store(true, Ordering::Relaxed);

            let connection = server.accept().await.expect("Connection unsuccessful");

            let (mut packet_reader, mut packet_writer) =
                packet_stream(connection.0, Duration::from_secs(30));

            println!("ST: connection set up");
            let job_req = SubmitJob::from_packet(
                packet_reader
                    .read_packet()
                    .await
                    .expect("The echo packet must be sent before the connection closes"),
            )
            .expect("Failed to parse SubmitJob packet");

            assert!(
                job_req.get_payload() == TEST_PAYLOAD,
                "Payloads are not equal"
            );

            println!("ST: got submit_job");

            let job_created = JobCreated::new(bytes::Bytes::from(JOB_HANDLE));

            packet_writer
                .send_packet(&job_created.to_packet())
                .await
                .expect("Failed to return packet");
            println!("ST: sent job_created");

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            let completed = WorkComplete::new(
                bytes::Bytes::from(JOB_HANDLE),
                bytes::Bytes::from("test".as_bytes()),
            );

            packet_writer
                .send_packet(&completed.to_packet())
                .await
                .expect("Failed to return packet");
            println!("ST: sent job_completed");
        });

        while !server_running.load(Ordering::Relaxed) {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let connection_options = ConnectOptions::new("gearman://127.0.0.1:15001")
            .expect("Failed to create conn options");
        let (mut client, mut client_loop) = Connection::connect(connection_options)
            .await
            .expect("failed to connect to client");

        println!("MT: connected to server");

        let should_run_for_runner = Arc::clone(&should_run);
        let runner_handle = tokio::task::spawn(async move {
            while should_run_for_runner.load(Ordering::Relaxed) {
                client_loop.step().await.unwrap();
            }
            println!("RL: finished runner");
        });

        let job = Job::new(TEST_PAYLOAD);

        let job_handle = job
            .submit(TEST_FUNCTION_NAME, &mut client)
            .await
            .expect("Expected job handle and got error");

        println!("MT: submitted job to server and got job_handle");

        let result = job_handle.await;

        println!("MT: received job_result from server");

        match result {
            crate::job::JobResult::WorkFail => panic!("expected completed"),
            crate::job::JobResult::WorkComplete(_result) => (),
            crate::job::JobResult::WorkException(exception) => {
                panic!("expected complete, found: {exception:?}")
            }
        }

        should_run.store(false, Ordering::Relaxed);
        runner_handle.await.expect("Failed to join threads");
        server_handle.await.expect("Failed to join threads");
    }
}
