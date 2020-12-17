/**
 *    Copyright (C) 2020-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kTenantMigration

#include "mongo/platform/basic.h"

#include "mongo/db/client.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/tenant_migration_access_blocker.h"
#include "mongo/db/repl/tenant_migration_committed_info.h"
#include "mongo/db/repl/tenant_migration_conflict_info.h"
#include "mongo/logv2/log.h"
#include "mongo/util/cancelation.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/future_util.h"

namespace mongo {

namespace {

MONGO_FAIL_POINT_DEFINE(tenantMigrationBlockRead);
MONGO_FAIL_POINT_DEFINE(tenantMigrationBlockWrite);

const Backoff kExponentialBackoff(Seconds(1), Milliseconds::max());

}  // namespace

using tenant_migration_donor::ConditionHandle;
using tenant_migration_donor::RepeatableConditionNotification;

void TenantMigrationAccessBlocker::init() {
    _repeatableConditionNotification =
        std::make_unique<RepeatableConditionNotification<TenantMigrationAccessBlocker>>(
            std::weak_ptr<TenantMigrationAccessBlocker>{shared_from_this()}, _mutex);

    _canReadOrRejectedFn = [this](Timestamp opCtxTimestamp) -> bool {
        return _state == State::kAllow || _state == State::kAborted ||
            _state == State::kBlockWrites || opCtxTimestamp < *_blockTimestamp ||
            _state == State::kReject;
    };
}

void TenantMigrationAccessBlocker::checkIfCanWriteOrThrow() {
    stdx::lock_guard<Latch> lg(_mutex);

    switch (_state) {
        case State::kAllow:
            return;
        case State::kAborted:
            return;
        case State::kBlockWrites:
        case State::kBlockWritesAndReads:
            uasserted(TenantMigrationConflictInfo(_tenantId, shared_from_this()),
                      "Write must block until this tenant migration commits or aborts");
        case State::kReject:
            uasserted(TenantMigrationCommittedInfo(_tenantId, _recipientConnString),
                      "Write must be re-routed to the new owner of this tenant");
        default:
            MONGO_UNREACHABLE;
    }
}

SharedSemiFuture<ConditionHandle<TenantMigrationAccessBlocker>>
TenantMigrationAccessBlocker::getTransitionOutOfBlockingForClusterTimeRead(
    OperationContext* opCtx) {
    auto targetTimestamp = _targetTimestampForRead(opCtx);
    if (!targetTimestamp) {
        // Return a ready future with null pointer, indicating that any further
        // condition check is not necessary.
        return SharedSemiFuture(ConditionHandle<TenantMigrationAccessBlocker>{
            std::weak_ptr<TenantMigrationAccessBlocker>{}});
    }
    stdx::unique_lock<Latch> ul(_mutex);
    if (_canReadOrRejectedFn(*targetTimestamp)) {
        // Return a ready future with pointer, so that the condition could be waited for at the
        // receiving end.
        return SharedSemiFuture(ConditionHandle<TenantMigrationAccessBlocker>(shared_from_this()));
    }
    return _repeatableConditionNotification->getFuture();
}

// static
std::optional<Timestamp> TenantMigrationAccessBlocker::_targetTimestampForRead(
    OperationContext* opCtx) {
    auto readConcernArgs = repl::ReadConcernArgs::get(opCtx);
    if (auto afterClusterTime = readConcernArgs.getArgsAfterClusterTime()) {
        return afterClusterTime->asTimestamp();
    }
    if (auto atClusterTime = readConcernArgs.getArgsAtClusterTime()) {
        return atClusterTime->asTimestamp();
    }
    if (readConcernArgs.getLevel() == repl::ReadConcernLevel::kSnapshotReadConcern) {
        return repl::StorageInterface::get(opCtx)->getPointInTimeReadTimestamp(opCtx);
    }
    return std::nullopt;
}

Status TenantMigrationAccessBlocker::waitUntilCommittedOrAborted(OperationContext* opCtx) {
    return Status::OK();
    stdx::unique_lock<Latch> ul(_mutex);

    auto canWrite = [&]() { return _state == State::kAllow || _state == State::kAborted; };

    if (!canWrite()) {
        tenantMigrationBlockWrite.shouldFail();
    }

    opCtx->waitForConditionOrInterrupt(
        _repeatableConditionNotification->cv(), ul, [this, &canWrite]() {
            return canWrite() || _state == State::kReject;
        });
    return onCompletion().getNoThrow();
}

void TenantMigrationAccessBlocker::checkIfCanDoClusterTimeReadOrBlock(
    OperationContext* opCtx) const {
    auto targetTimestamp = _targetTimestampForRead(opCtx);
    invariant(targetTimestamp);  // Verified in getTransitionOutOfBlockingForClusterTimeRead.
    stdx::unique_lock<Latch> ul(_mutex);

    if (!_canReadOrRejectedFn(*targetTimestamp)) {
        tenantMigrationBlockRead.shouldFail();
    }

    opCtx->waitForConditionOrInterrupt(
        _repeatableConditionNotification->cv(), ul, [this, targetTimestamp = *targetTimestamp]() {
            return _canReadOrRejectedFn(targetTimestamp);
        });

    uassert(TenantMigrationCommittedInfo(_tenantId, _recipientConnString),
            "Read must be re-routed to the new owner of this tenant",
            _canReadOrRejectedFn(*targetTimestamp));
}

void TenantMigrationAccessBlocker::checkIfLinearizableReadWasAllowedOrThrow(
    OperationContext* opCtx) {
    stdx::lock_guard<Latch> lg(_mutex);
    uassert(TenantMigrationCommittedInfo(_tenantId, _recipientConnString),
            "Read must be re-routed to the new owner of this tenant",
            _state != State::kReject);
}

void TenantMigrationAccessBlocker::startBlockingWrites() {
    stdx::lock_guard<Latch> lg(_mutex);

    LOGV2(5093800, "Tenant migration starting to block writes", "tenantId"_attr = _tenantId);

    invariant(!_inShutdown);
    invariant(_state == State::kAllow);
    invariant(!_blockTimestamp);
    invariant(!_commitOrAbortOpTime);
    invariant(!_waitForCommitOrAbortToMajorityCommitOpCtx);

    _state = State::kBlockWrites;
}

void TenantMigrationAccessBlocker::startBlockingReadsAfter(const Timestamp& blockTimestamp) {
    stdx::lock_guard<Latch> lg(_mutex);

    LOGV2(5093801,
          "Tenant migration starting to block reads after blockTimestamp",
          "tenantId"_attr = _tenantId,
          "blockTimestamp"_attr = blockTimestamp);

    invariant(!_inShutdown);
    invariant(_state == State::kBlockWrites);
    invariant(!_blockTimestamp);
    invariant(!_commitOrAbortOpTime);
    invariant(!_waitForCommitOrAbortToMajorityCommitOpCtx);

    _state = State::kBlockWritesAndReads;
    _blockTimestamp = blockTimestamp;
}

void TenantMigrationAccessBlocker::rollBackStartBlocking() {
    stdx::lock_guard<Latch> lg(_mutex);

    invariant(!_inShutdown);
    invariant(_state == State::kBlockWrites || _state == State::kBlockWritesAndReads);
    invariant(!_commitOrAbortOpTime);
    invariant(!_waitForCommitOrAbortToMajorityCommitOpCtx);

    _state = State::kAllow;
    _blockTimestamp.reset();
    _repeatableConditionNotification->notifyAll(
        ConditionHandle<TenantMigrationAccessBlocker>{shared_from_this()});
}

void TenantMigrationAccessBlocker::commit(repl::OpTime commitOpTime) {
    stdx::lock_guard<Latch> lg(_mutex);

    LOGV2(5093802,
          "Tenant migration starting to wait for commit OpTime to be majority-committed",
          "tenantId"_attr = _tenantId,
          "commitOpTime"_attr = commitOpTime);

    invariant(!_inShutdown);
    invariant(_state == State::kBlockWritesAndReads);
    invariant(_blockTimestamp);
    invariant(!_commitOrAbortOpTime);
    invariant(!_waitForCommitOrAbortToMajorityCommitOpCtx);

    _commitOrAbortOpTime = commitOpTime;

    _waitForOpTimeToMajorityCommit(commitOpTime)
        .then([this, self = shared_from_this(), commitOpTime]() {
            stdx::lock_guard<Latch> lg(_mutex);

            invariant(_state == State::kBlockWritesAndReads);
            invariant(_blockTimestamp);
            invariant(_commitOrAbortOpTime == commitOpTime);
            invariant(!_waitForCommitOrAbortToMajorityCommitOpCtx);

            _state = State::kReject;
            _repeatableConditionNotification->notifyAll(
                ConditionHandle<TenantMigrationAccessBlocker>{shared_from_this()});
            _completionPromise.setError(
                {ErrorCodes::TenantMigrationCommitted,
                 "Write must be re-routed to the new owner of this tenant",
                 TenantMigrationCommittedInfo(_tenantId, _recipientConnString).toBSON()});
        })
        .getAsync([this, self = shared_from_this()](Status status) {
            stdx::lock_guard<Latch> lg(_mutex);
            LOGV2(5093803,
                  "Tenant migration finished waiting for commit OpTime to be majority-committed",
                  "tenantId"_attr = _tenantId,
                  "status"_attr = status);
        });
}

void TenantMigrationAccessBlocker::abort(repl::OpTime abortOpTime) {
    stdx::lock_guard<Latch> lg(_mutex);

    LOGV2(5093804,
          "Tenant migration starting to wait for abort OpTime to be majority-committed",
          "tenantId"_attr = _tenantId,
          "abortOpTime"_attr = abortOpTime);

    invariant(!_inShutdown);
    invariant(!_commitOrAbortOpTime);
    invariant(!_waitForCommitOrAbortToMajorityCommitOpCtx);

    _commitOrAbortOpTime = abortOpTime;

    _waitForOpTimeToMajorityCommit(abortOpTime)
        .then([this, self = shared_from_this(), abortOpTime]() {
            stdx::lock_guard<Latch> lg(_mutex);

            invariant(_commitOrAbortOpTime == abortOpTime);
            invariant(!_waitForCommitOrAbortToMajorityCommitOpCtx);

            _state = State::kAborted;
            _repeatableConditionNotification->notifyAll(
                ConditionHandle<TenantMigrationAccessBlocker>{shared_from_this()});
            _completionPromise.setError(
                {ErrorCodes::TenantMigrationAborted, "Tenant migration aborted"});
        })
        .getAsync([this, self = shared_from_this()](Status status) {
            stdx::lock_guard<Latch> lg(_mutex);
            LOGV2(5093805,
                  "Tenant migration finished waiting for abort OpTime to be majority-committed",
                  "tenantId"_attr = _tenantId,
                  "status"_attr = status);
        });
}

void TenantMigrationAccessBlocker::shutDown() {
    stdx::lock_guard<Latch> lg(_mutex);
    if (_inShutdown) {
        return;
    }

    _inShutdown = true;
    if (_waitForCommitOrAbortToMajorityCommitOpCtx) {
        stdx::lock_guard<Client> lk(*_waitForCommitOrAbortToMajorityCommitOpCtx->getClient());
        _waitForCommitOrAbortToMajorityCommitOpCtx->markKilled();
    }
}

SharedSemiFuture<void> TenantMigrationAccessBlocker::onCompletion() {
    return _completionPromise.getFuture();
}

ExecutorFuture<void> TenantMigrationAccessBlocker::_waitForOpTimeToMajorityCommit(
    repl::OpTime opTime) {
    return AsyncTry([this, self = shared_from_this(), opTime] {
               ThreadClient tc("TenantMigrationAccessBlocker", _serviceContext);
               const auto opCtxHolder = tc->makeOperationContext();
               const auto opCtx = opCtxHolder.get();

               // We will save 'opCtx' below, so make sure we clear it before 'opCtx' is destroyed.
               const auto guard = makeGuard([&] {
                   stdx::lock_guard<Latch> lg(_mutex);
                   _waitForCommitOrAbortToMajorityCommitOpCtx = nullptr;
               });

               {
                   stdx::lock_guard<Latch> lg(_mutex);

                   uassert(ErrorCodes::TenantMigrationAccessBlockerShuttingDown,
                           "TenantMigrationAccessBlocker was shut down",
                           !_inShutdown);

                   // Save 'opCtx' so that if shutDown() is called after this point, 'opCtx' will be
                   // interrupted and 'waitUntilMajorityOpTime' will return an interrupt error.
                   _waitForCommitOrAbortToMajorityCommitOpCtx = opCtx;
               }
               uassertStatusOK(repl::ReplicationCoordinator::get(opCtx)->waitUntilMajorityOpTime(
                   opCtx, opTime));
           })
        .until([this, self = shared_from_this(), opTime](Status status) {
            bool shouldStop =
                status.isOK() || status == ErrorCodes::TenantMigrationAccessBlockerShuttingDown;
            if (!shouldStop) {
                LOGV2(5093806,
                      "Tenant migration retrying waiting for OpTime to be majority-committed",
                      "tenantId"_attr = _tenantId,
                      "opTime"_attr = opTime,
                      "status"_attr = status);
            }
            return shouldStop;
        })
        .withBackoffBetweenIterations(kExponentialBackoff)
        .on(_executor, CancelationToken::uncancelable());
}

void TenantMigrationAccessBlocker::appendInfoForServerStatus(BSONObjBuilder* builder) const {
    stdx::lock_guard<Latch> lg(_mutex);

    BSONObjBuilder tenantBuilder;
    tenantBuilder.append("state", stateToString(_state));
    if (_blockTimestamp) {
        tenantBuilder.append("blockTimestamp", _blockTimestamp.get());
    }
    if (_commitOrAbortOpTime) {
        tenantBuilder.append("commitOrAbortOpTime", _commitOrAbortOpTime->toBSON());
    }
    builder->append(_tenantId, tenantBuilder.obj());
}

std::string TenantMigrationAccessBlocker::stateToString(State state) const {
    switch (state) {
        case State::kAllow:
            return "allow";
        case State::kBlockWrites:
            return "blockWrites";
        case State::kBlockWritesAndReads:
            return "blockWritesAndReads";
        case State::kReject:
            return "reject";
        case State::kAborted:
            return "aborted";
        default:
            MONGO_UNREACHABLE;
    }
}

}  // namespace mongo
