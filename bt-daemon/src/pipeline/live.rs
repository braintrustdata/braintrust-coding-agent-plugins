//! Bounded, ephemeral fan-out. SQLite is the recovery path, never the live queue.
use crate::translate::CowSpanOp;
use parking_lot::Mutex;
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore};

pub(crate) type SharedOp = CowSpanOp;
pub(crate) const BYTE_BUDGET: usize = 256 * 1024;
pub(crate) const WINDOW: std::time::Duration = std::time::Duration::from_secs(30);

pub(crate) struct Record {
    pub position: u64,
    pub op: SharedOp,
    pub bytes: usize,
}
impl Clone for Record {
    fn clone(&self) -> Self {
        Self {
            position: self.position,
            op: self.op.clone(),
            bytes: self.bytes,
        }
    }
}
struct Queued {
    record: Record,
    _permit: OwnedSemaphorePermit,
}
struct Route {
    tx: mpsc::Sender<Queued>,
    budget: Arc<Semaphore>,
    paused: watch::Sender<bool>,
}
#[derive(Default)]
pub(crate) struct Multiplexer {
    routes: Mutex<Vec<Weak<Route>>>,
}
pub(crate) struct Receiver {
    route: Arc<Route>,
    rx: mpsc::Receiver<Queued>,
}
impl Multiplexer {
    pub fn subscribe(&self) -> Receiver {
        let (tx, rx) = mpsc::channel(32);
        let route = Arc::new(Route {
            tx,
            budget: Arc::new(Semaphore::new(BYTE_BUDGET)),
            paused: watch::channel(false).0,
        });
        self.routes.lock().push(Arc::downgrade(&route));
        Receiver { route, rx }
    }
    pub async fn publish(&self, record: Record) {
        let routes = {
            let mut routes = self.routes.lock();
            let active = routes.iter().filter_map(Weak::upgrade).collect::<Vec<_>>();
            routes.retain(|route| route.strong_count() > 0);
            active
        };
        for route in routes {
            let mut paused = route.paused.subscribe();
            if *paused.borrow() {
                continue;
            }
            // Healthy routes apply byte-bounded backpressure. Failed/abandoned
            // routes explicitly pause their live feed and recover from disk.
            // An oversized operation travels alone.
            let permits = record.bytes.clamp(1, BYTE_BUDGET) as u32;
            let permit = tokio::select! {
                permit=route.budget.clone().acquire_many_owned(permits)=>match permit { Ok(permit)=>permit, Err(_)=>continue },
                _=paused.changed()=>continue,
            };
            if *paused.borrow() {
                continue;
            }
            tokio::select! {
                _=route.tx.send(Queued { record:record.clone(), _permit:permit })=>{},
                _=paused.changed()=>{},
            }
        }
    }
}
#[derive(Clone)]
pub(crate) struct Control(Weak<Route>);
impl Control {
    pub fn pause(&self) {
        if let Some(route) = self.0.upgrade() {
            route.paused.send_replace(true);
        }
    }
}
impl Receiver {
    pub fn control(&self) -> Control {
        Control(Arc::downgrade(&self.route))
    }
    pub fn resume(&self) {
        self.route.paused.send_replace(false);
    }

    pub fn try_recv(&mut self) -> Option<Record> {
        self.rx.try_recv().ok().map(|queued| queued.record)
    }
    pub async fn recv(&mut self) -> Option<Record> {
        self.rx.recv().await.map(|queued| queued.record)
    }
    pub fn clear(&mut self) {
        while self.rx.try_recv().is_ok() {}
    }
}
impl Drop for Receiver {
    fn drop(&mut self) {
        // Only weak route references live in the multiplexer; idle journals do
        // not retain payloads or abandoned route channels.
        self.route.paused.send_replace(true);
        self.route.budget.close();
    }
}
