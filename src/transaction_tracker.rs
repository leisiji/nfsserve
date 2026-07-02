use std::collections::{hash_map, HashMap};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

/// `TransactionTracker` tracks the state of transactions to detect retransmissions.
pub struct TransactionTracker {
    retention_period: Duration,
    transactions: Mutex<HashMap<(u32, String), TransactionState>>,
}

impl TransactionTracker {
    pub fn new(retention_period: Duration) -> Self {
        Self {
            retention_period,
            transactions: Mutex::new(HashMap::new()),
        }
    }

    /// Checks if the transaction is a retransmission.
    /// If it's a new transaction, it is marked as `InProgress`.
    ///
    /// Returns `true` if the transaction is a retransmission, `false` otherwise.
    pub fn is_retransmission(&self, xid: u32, client_addr: &str) -> bool {
        let key = (xid, client_addr.to_string());
        let mut transactions = self.transactions.lock().expect("unable to unlock transactions mutex");
        housekeeping(&mut transactions, self.retention_period);
        if let hash_map::Entry::Vacant(e) = transactions.entry(key) {
            e.insert(TransactionState::InProgress);
            false
        } else {
            true
        }
    }

    /// Marks the transaction as processed.
    pub fn mark_processed(&self, xid: u32, client_addr: &str) {
        let key = (xid, client_addr.to_string());
        let completion_time = SystemTime::now();
        let mut transactions = self.transactions.lock().expect("unable to unlock transactions mutex");
        if let Some(tx) = transactions.get_mut(&key) {
            *tx = TransactionState::Completed(completion_time);
        }
    }

    /// Clears all transactions for a given client.
    /// Useful on unmount so a fresh mount doesn't trigger false retransmission detection.
    pub fn clear_client(&self, client_addr: &str) {
        let mut transactions = self.transactions.lock().expect("unable to unlock transactions mutex");
        transactions.retain(|(_, addr), _| addr != client_addr);
    }
}

fn housekeeping(transactions: &mut HashMap<(u32, String), TransactionState>, max_age: Duration) {
    let mut cutoff = SystemTime::now() - max_age;
    transactions.retain(|_, v| match v {
        TransactionState::InProgress => true,
        TransactionState::Completed(completion_time) => completion_time >= &mut cutoff,
    });
}

pub enum TransactionState {
    InProgress,
    Completed(SystemTime),
}
