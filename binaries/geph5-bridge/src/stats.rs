use std::{sync::LazyLock, time::Duration};

use geph5_broker_protocol::Mac;
use geph5_stats::StatBatcher;

/// Stats accumulated locally, shipped to the broker in periodic batches.
pub static STAT_BATCHER: LazyLock<StatBatcher> = LazyLock::new(StatBatcher::new);

const FLUSH_INTERVAL: Duration = Duration::from_secs(10);

/// Periodically drains STAT_BATCHER into the broker's authenticated report_stats RPC.
pub async fn stats_flush_loop(auth_token: &str, broker_rpc: crate::broker::Client) {
    let mac_key = blake3::hash(auth_token.as_bytes());

    loop {
        tokio::time::sleep(FLUSH_INTERVAL).await;
        let events = STAT_BATCHER.drain();
        if events.is_empty() {
            continue;
        }
        let res = broker_rpc
            .report_stats(Mac::new(events, mac_key.as_bytes()))
            .await;
        match res {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::warn!(err = %err, "broker rejected stats batch"),
            Err(err) => tracing::warn!(err = %err, "failed to ship stats batch"),
        }
    }
}
