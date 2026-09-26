//! Instrumentação temporária de profiling (feature `profile`).
//!
//! Mede TEMPO DE CPU da THREAD (não wall-clock) via clock_gettime(
//! CLOCK_THREAD_CPUTIME_ID) — chamada vdso de ~20ns. Como o runtime é
//! single-thread (current_thread), tempo de CPU da thread = tempo de CPU
//! do server, então isso dá o gasto real de CPU por etapa do hot path.
//! Resumo impresso a cada 5s.

use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "profile")]
mod cputime {
    #[repr(C)]
    struct Timespec {
        tv_sec: i64,
        tv_nsec: i64,
    }
    extern "C" {
        fn clock_gettime(clk_id: i32, tp: *mut Timespec) -> i32;
    }
    // Linux: relógio que conta CPU consumida pela *thread atual*.
    const CLOCK_THREAD_CPUTIME_ID: i32 = 3;

    /// Nanossegundos de CPU consumidos por esta thread até agora.
    #[inline]
    pub fn thread_cpu_nanos() -> u64 {
        let mut ts = Timespec { tv_sec: 0, tv_nsec: 0 };
        unsafe {
            // vdso: não é syscall, custa ~20ns — seguro no hot path.
            clock_gettime(CLOCK_THREAD_CPUTIME_ID, &mut ts);
        }
        (ts.tv_sec as u64) * 1_000_000_000 + ts.tv_nsec as u64
    }
}

#[cfg(feature = "profile")]
pub static T_ADMIN_TOTAL: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "profile")]
pub static T_PARSE: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "profile")]
pub static T_ROUTE: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "profile")]
pub static T_REPLY: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "profile")]
pub static T_WS_SEND_ADMIN: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "profile")]
pub static T_WS_SEND_CLIENT: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "profile")]
pub static T_CLIENT_FWD: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "profile")]
pub static T_TO_ADMIN: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "profile")]
pub static T_ADMIN_LOOP: AtomicU64 = AtomicU64::new(0); // iteração inteira do select!
#[cfg(feature = "profile")]
pub static T_READ: AtomicU64 = AtomicU64::new(0); // stream.next() no admin
#[cfg(feature = "profile")]
pub static N_ADMIN_MSG: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "profile")]
pub static N_CLIENT_MSG: AtomicU64 = AtomicU64::new(0);

/// Cronômetro de CPU: mede a CPU da thread entre `new` e o `drop`.
pub struct Scope<'a>(&'a AtomicU64, Option<u64>);

#[cfg(feature = "profile")]
impl<'a> Scope<'a> {
    #[inline]
    pub fn new(counter: &'a AtomicU64) -> Self {
        Scope(counter, Some(cputime::thread_cpu_nanos()))
    }
}

#[cfg(feature = "profile")]
impl Drop for Scope<'_> {
    #[inline]
    fn drop(&mut self) {
        if let Some(t0) = self.1 {
            let dt = cputime::thread_cpu_nanos().saturating_sub(t0);
            self.0.fetch_add(dt, Ordering::Relaxed);
        }
    }
}

#[cfg(not(feature = "profile"))]
impl<'a> Scope<'a> {
    #[inline]
    pub fn new(_counter: &'a AtomicU64) -> Self {
        Scope(&T_DUMMY, None)
    }
}

#[cfg(not(feature = "profile"))]
static T_DUMMY: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "profile")]
pub async fn reporter_task() {
    use tokio::time::{sleep, Duration};
    sleep(Duration::from_secs(3)).await;
    let mut prev = (
        T_ADMIN_TOTAL.load(Ordering::Relaxed), T_PARSE.load(Ordering::Relaxed),
        T_ROUTE.load(Ordering::Relaxed), T_REPLY.load(Ordering::Relaxed),
        T_WS_SEND_ADMIN.load(Ordering::Relaxed), T_WS_SEND_CLIENT.load(Ordering::Relaxed),
        T_CLIENT_FWD.load(Ordering::Relaxed), T_TO_ADMIN.load(Ordering::Relaxed),
        T_ADMIN_LOOP.load(Ordering::Relaxed), T_READ.load(Ordering::Relaxed),
        N_ADMIN_MSG.load(Ordering::Relaxed), N_CLIENT_MSG.load(Ordering::Relaxed),
    );
    loop {
        sleep(Duration::from_secs(5)).await;
        let cur = (
            T_ADMIN_TOTAL.load(Ordering::Relaxed), T_PARSE.load(Ordering::Relaxed),
            T_ROUTE.load(Ordering::Relaxed), T_REPLY.load(Ordering::Relaxed),
            T_WS_SEND_ADMIN.load(Ordering::Relaxed), T_WS_SEND_CLIENT.load(Ordering::Relaxed),
            T_CLIENT_FWD.load(Ordering::Relaxed), T_TO_ADMIN.load(Ordering::Relaxed),
            T_ADMIN_LOOP.load(Ordering::Relaxed), T_READ.load(Ordering::Relaxed),
            N_ADMIN_MSG.load(Ordering::Relaxed), N_CLIENT_MSG.load(Ordering::Relaxed),
        );
        let d = |a: u64, b: u64| a.saturating_sub(b);
        let msgs = d(cur.10, prev.10);
        let cmsgs = d(cur.11, prev.11);
        eprintln!(
            "\n[profile] últimos 5s (CPU da thread): admin_msgs={msgs} client_msgs={cmsgs}\n\
             [profile]   iteração loop admin     {:>8} us/msg  (TUDO: select! + handler)\n\
             [profile]   ├─ stream.next() (read) {:>8} us/msg\n\
             [profile]   ├─ handle_admin_message {:>8} us/msg\n\
             [profile]   │   ├─ serde parse      {:>8} us/msg\n\
             [profile]   │   ├─ send_to_client   {:>8} us/msg\n\
             [profile]   │   └─ reply            {:>8} us/msg  (encode+write+await)\n\
             [profile]   └─ ws send p/ cliente   {:>8} us/msg  (task cliente: rx→socket)",
            (d(cur.8, prev.8) / msgs.max(1)) / 1000,
            (d(cur.9, prev.9) / msgs.max(1)) / 1000,
            (d(cur.0, prev.0) / msgs.max(1)) / 1000,
            (d(cur.1, prev.1) / msgs.max(1)) / 1000,
            (d(cur.2, prev.2) / msgs.max(1)) / 1000,
            (d(cur.3, prev.3) / msgs.max(1)) / 1000,
            (d(cur.5, prev.5) / msgs.max(1)) / 1000,
        );
        prev = cur;
    }
}
