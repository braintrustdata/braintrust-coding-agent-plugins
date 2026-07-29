use axum::Router;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Lifecycle wrapper for any ephemeral Axum test service.
pub struct TestServer {
    uri: String,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
}

impl TestServer {
    pub async fn start(router: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral test server");
        let address = listener.local_addr().expect("read test server address");
        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        Self {
            uri: format!("http://{address}"),
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
