// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Post-commit hooks for transaction processing.
//!
//! This module contains notification logic that runs after a transaction is committed.
//! Extracting this into a separate module minimizes merge conflicts with upstream changes
//! to authority.rs.

use std::str::FromStr;
use std::sync::Arc;

use dashmap::DashSet;
use sui_json_rpc_types::SuiEvent;
use sui_types::base_types::ObjectID;
use sui_types::executable_transaction::VerifiedExecutableTransaction;
use sui_types::object::Object;
use sui_types::object::Owner;
use sui_types::storage::BackingPackageStore;
use sui_types::transaction::TransactionDataAPI;

use crate::authority::authority_per_epoch_store::AuthorityPerEpochStore;
use crate::cache_update_handler::CacheUpdateHandler;
use crate::transaction_outputs::TransactionOutputs;
use crate::tx_handler::TxHandler;

/// Runs post-commit notification hooks for MEV monitoring.
///
/// This function handles:
/// 1. Notifying connected clients about changed objects that match monitoring criteria
/// 2. Streaming transaction effects and events to subscribers
pub fn run_post_commit_hooks(
    certificate: &VerifiedExecutableTransaction,
    transaction_outputs: &Arc<TransactionOutputs>,
    epoch_store: &Arc<AuthorityPerEpochStore>,
    cache_update_handler: &CacheUpdateHandler,
    tx_handler: &TxHandler,
    pool_related_ids: &DashSet<ObjectID>,
    backing_package_store: Arc<dyn BackingPackageStore + Send + Sync>,
) {
    // Skip system transactions
    if certificate.transaction_data().is_system_tx() {
        return;
    }

    let tx_digest = *certificate.digest();

    // Object change notification
    notify_object_changes(
        transaction_outputs,
        cache_update_handler,
        pool_related_ids,
    );

    // Event notification
    notify_events(
        &tx_digest,
        transaction_outputs,
        epoch_store,
        tx_handler,
        backing_package_store,
    );
}

/// Notifies connected clients about object changes that match monitoring criteria.
fn notify_object_changes(
    transaction_outputs: &Arc<TransactionOutputs>,
    cache_update_handler: &CacheUpdateHandler,
    pool_related_ids: &DashSet<ObjectID>,
) {
    let changed_objects: Vec<_> = transaction_outputs
        .written
        .iter()
        .map(|(id, obj)| (*id, obj.clone()))
        .collect();

    if changed_objects.is_empty() {
        return;
    }

    let need_notify = changed_objects.iter().any(|(id, obj)| {
        let is_our_object = std::env::var("SUI_ADDRESS")
            .ok()
            .and_then(|addr| ObjectID::from_str(&addr).ok())
            .map(|target| obj.owner() == &Owner::AddressOwner(target.into()))
            .unwrap_or(false);

        let is_pool_related = pool_related_ids.contains(id);
        is_our_object || is_pool_related
    });

    if need_notify {
        let handler = cache_update_handler.clone();
        tokio::spawn(async move {
            handler.notify_written(changed_objects).await;
        });
    }
}

/// Converts and streams transaction events to subscribers.
fn notify_events(
    tx_digest: &sui_types::digests::TransactionDigest,
    transaction_outputs: &Arc<TransactionOutputs>,
    epoch_store: &Arc<AuthorityPerEpochStore>,
    tx_handler: &TxHandler,
    backing_package_store: Arc<dyn BackingPackageStore + Send + Sync>,
) {
    let raw_events = &transaction_outputs.events;

    if raw_events.data.is_empty() || transaction_outputs.written.is_empty() {
        return;
    }

    let executor = epoch_store.executor();
    let tx_digest = *tx_digest;

    let sui_events: Vec<SuiEvent> = raw_events
        .data
        .iter()
        .enumerate()
        .filter_map(|(seq, event)| {
            let mut layout_resolver =
                executor.type_layout_resolver(Box::new(backing_package_store.as_ref()));
            match layout_resolver.get_annotated_layout(&event.type_) {
                Ok(layout) => {
                    SuiEvent::try_from(event.clone(), tx_digest, seq as u64, None, layout).ok()
                }
                Err(_) => None,
            }
        })
        .collect();

    if !sui_events.is_empty() {
        let tx_handler = tx_handler.clone();
        let effects = transaction_outputs.effects.clone();
        tokio::spawn(async move {
            let _ = tx_handler
                .send_tx_effects_and_events(&effects, sui_events)
                .await;
        });
    }
}
