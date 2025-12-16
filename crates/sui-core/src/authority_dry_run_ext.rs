// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Extracted dry-run with object overrides implementation.
//! Kept separate from authority.rs to minimize merge conflicts during rebase.

use std::collections::BTreeMap;
use std::sync::Arc;

use sui_json_rpc_types::{
    DryRunTransactionBlockResponse, SuiTransactionBlockData, SuiTransactionBlockEvents,
};
use sui_types::base_types::{ObjectID, ObjectRef, TransactionDigest};
use sui_types::effects::{TransactionEffects, TransactionEffectsAPI};
use sui_types::error::{SuiErrorKind, SuiResult};
use sui_types::execution_params::ExecutionOrEarlyError;
use sui_types::inner_temporary_store::{PackageStoreWithFallback, TemporaryModuleResolver};
use sui_types::object::{MoveObject, Object, OBJECT_START_VERSION, Owner};
use sui_types::storage::WriteKind;
use sui_types::transaction::{TransactionData, TransactionDataAPI};

use crate::authority::authority_per_epoch_store::AuthorityPerEpochStore;
use crate::authority::AuthorityState;
use crate::override_cache::{InputLoaderCache, ObjectCache};
use crate::transaction_input_loader::TransactionInputLoader;

/// Execute a dry-run transaction with object overrides.
/// This is the implementation extracted from AuthorityState to reduce diff size in authority.rs.
#[allow(clippy::type_complexity)]
pub async fn dry_exec_transaction_override_impl(
    state: &AuthorityState,
    input_loader: &TransactionInputLoader,
    epoch_store: &AuthorityPerEpochStore,
    transaction: TransactionData,
    transaction_digest: TransactionDigest,
    override_objects: Vec<(ObjectID, Object)>,
) -> SuiResult<(
    DryRunTransactionBlockResponse,
    BTreeMap<ObjectID, (ObjectRef, Object, WriteKind)>,
    TransactionEffects,
    Option<ObjectID>,
)> {
    // Cheap validity checks for a transaction, including input size limits.
    transaction.validity_check_no_gas_check(epoch_store.protocol_config())?;

    let input_object_kinds = transaction.input_objects()?;
    let receiving_object_refs = transaction.receiving_objects();

    sui_transaction_checks::deny::check_transaction_for_signing(
        &transaction,
        &[],
        &input_object_kinds,
        &receiving_object_refs,
        &state.config.transaction_deny_config,
        state.get_backing_package_store().as_ref(),
    )?;

    let cached_input_loader = InputLoaderCache {
        loader: input_loader,
        cache: override_objects.clone(),
    };

    let (input_objects, receiving_objects) = match cached_input_loader.read_objects_for_signing(
        // We don't want to cache this transaction since it's a dry run.
        None,
        &input_object_kinds,
        &receiving_object_refs,
        epoch_store.epoch(),
    ) {
        Ok((input_objects, receiving_objects)) => (input_objects, receiving_objects),
        Err(e) => {
            return Err(e);
        }
    };

    // make a gas object if one was not provided
    // Unused: let mut gas_object_refs = transaction.gas().to_vec();
    let ((gas_status, checked_input_objects), mock_gas) = if transaction.gas().is_empty() {
        let sender = transaction.sender();
        // use a 1B sui coin
        const MIST_TO_SUI: u64 = 1_000_000_000;
        const DRY_RUN_SUI: u64 = 1_000_000_000;
        let max_coin_value = MIST_TO_SUI * DRY_RUN_SUI;
        let gas_object_id = ObjectID::random();
        let gas_object = Object::new_move(
            MoveObject::new_gas_coin(OBJECT_START_VERSION, gas_object_id, max_coin_value),
            Owner::AddressOwner(sender),
            TransactionDigest::genesis_marker(),
        );
        // Unused: let gas_object_ref = gas_object.compute_object_reference();
        // Unused: gas_object_refs = vec![gas_object_ref];
        (
            sui_transaction_checks::check_transaction_input_with_given_gas(
                epoch_store.protocol_config(),
                epoch_store.reference_gas_price(),
                &transaction,
                input_objects,
                receiving_objects,
                gas_object,
                &state.metrics.bytecode_verifier_metrics,
                &state.config.verifier_signing_config,
            )?,
            Some(gas_object_id),
        )
    } else {
        (
            sui_transaction_checks::check_transaction_input(
                epoch_store.protocol_config(),
                epoch_store.reference_gas_price(),
                &transaction,
                input_objects,
                &receiving_objects,
                &state.metrics.bytecode_verifier_metrics,
                &state.config.verifier_signing_config,
            )?,
            None,
        )
    };
    let gas_data = transaction.gas_data().clone();
    let protocol_config = epoch_store.protocol_config();
    let (kind, signer, _) = transaction.execution_parts();

    let silent = true;
    let executor = sui_execution::executor(protocol_config, silent)
        .expect("Creating an executor should not fail here");

    let expensive_checks = false;
    let object_cache = ObjectCache::new(Arc::clone(state.get_backing_store()), override_objects);
    let (inner_temp_store, _, effects,_timings, _execution_error, ) = executor
        .execute_transaction_to_effects(
            &object_cache,
            protocol_config,
            state.metrics.limits_metrics.clone(),
            expensive_checks,
            ExecutionOrEarlyError::Ok(()),
            &epoch_store.epoch_start_config().epoch_data().epoch_id(),
            epoch_store
                .epoch_start_config()
                .epoch_data()
                .epoch_start_timestamp(),
            checked_input_objects,
            gas_data,
            gas_status,
            kind,
            signer,
            transaction_digest,
            &mut None,
        );
    let tx_digest = *effects.transaction_digest();

    let module_cache =
        TemporaryModuleResolver::new(&inner_temp_store, epoch_store.module_cache().clone());

    let mut layout_resolver =
        epoch_store
            .executor()
            .type_layout_resolver(Box::new(PackageStoreWithFallback::new(
                &inner_temp_store,
                state.get_backing_package_store(),
            )));
    
    // Returning empty vector here because we recalculate changes in the rpc layer.
    let object_changes = Vec::new();

    // Returning empty vector here because we recalculate changes in the rpc layer.
    let balance_changes = Vec::new();

    let written_with_kind = effects
        .created()
        .into_iter()
        .map(|(oref, _)| (oref, WriteKind::Create))
        .chain(
            effects
                .unwrapped()
                .into_iter()
                .map(|(oref, _)| (oref, WriteKind::Unwrap)),
        )
        .chain(
            effects
                .mutated()
                .into_iter()
                .map(|(oref, _)| (oref, WriteKind::Mutate)),
        )
        .map(|(oref, kind)| {
            let obj = inner_temp_store.written.get(&oref.0).unwrap();
            // TODO: Avoid clones.
            (oref.0, (oref, obj.clone(), kind))
        })
        .collect();
    let execution_error_source = _execution_error
    .as_ref()
    .err()
    .and_then(|e| e.source().as_ref().map(|e| e.to_string()));
    Ok((
        DryRunTransactionBlockResponse {
            suggested_gas_price: state
                .congestion_tracker
                .get_suggested_gas_prices(&transaction),
            input: SuiTransactionBlockData::try_from_with_module_cache(transaction, &module_cache).map_err(
                |e| SuiErrorKind::TransactionSerializationError {
                    error: format!(
                        "Failed to convert transaction to SuiTransactionBlockData: {}",
                        e
                    ),
                },
            )?, // TODO: replace the underlying try_from to SuiError. This one goes deep
            effects: effects.clone().try_into()?,
            events: SuiTransactionBlockEvents::try_from(
                inner_temp_store.events.clone(),
                tx_digest,
                None,
                layout_resolver.as_mut(),
            )?,
            object_changes,
            balance_changes,
            execution_error_source,
        },
        written_with_kind,
        effects,
        mock_gas,
    ))
}
