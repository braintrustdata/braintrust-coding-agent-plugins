use axum::Router;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// HTTP lifecycle owned by the mock-inference component. It stays private:
/// consumers start an OpenAI or Anthropic mock, not a generic test server.
pub(super) struct InferenceServer {
    uri: String,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
}

impl InferenceServer {
    pub(super) async fn start(router: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock inference server");
        let address = listener
            .local_addr()
            .expect("read mock inference server address");
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

    pub(super) fn uri(&self) -> &str {
        &self.uri
    }

    pub(super) async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for InferenceServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
