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
#include "mongo/util/str.h"

#include "mongo/db/repl/tenant_migration_donor_util.h"

#include "mongo/db/commands/tenant_migration_recipient_cmds_gen.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/persistent_task_store.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/tenant_migration_access_blocker_registry.h"
#include "mongo/db/repl/tenant_migration_state_machine_gen.h"
#include "mongo/executor/network_interface_factory.h"
#include "mongo/executor/thread_pool_task_executor.h"
#include "mongo/logv2/log.h"
#include "mongo/transport/service_executor.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/future_util.h"

namespace mongo {

// Failpoint that will cause recoverTenantMigrationAccessBlockers to return early.
MONGO_FAIL_POINT_DEFINE(skipRecoverTenantMigrationAccessBlockers);

namespace tenant_migration_donor {

namespace {

constexpr char kThreadNamePrefix[] = "TenantMigrationWorker-";
constexpr char kPoolName[] = "TenantMigrationWorkerThreadPool";
constexpr char kNetName[] = "TenantMigrationWorkerNetwork";

const auto donorStateDocToDeleteDecoration = OperationContext::declareDecoration<BSONObj>();

class InlineExecutor final : public OutOfLineExecutor {
public:
    InlineExecutor() = default;

    void schedule(Task task) noexcept override {
        task(Status::OK());
    }

    static auto make() {
        return std::make_shared<InlineExecutor>();
    }
};

}  // namespace

TenantMigrationDonorDocument parseDonorStateDocument(const BSONObj& doc) {
    auto donorStateDoc =
        TenantMigrationDonorDocument::parse(IDLParserErrorContext("donorStateDoc"), doc);

    if (donorStateDoc.getExpireAt()) {
        uassert(ErrorCodes::BadValue,
                "contains \"expireAt\" but the migration has not committed or aborted",
                donorStateDoc.getState() == TenantMigrationDonorStateEnum::kCommitted ||
                    donorStateDoc.getState() == TenantMigrationDonorStateEnum::kAborted);
    }

    const std::string errmsg = str::stream() << "invalid donor state doc " << doc;

    switch (donorStateDoc.getState()) {
        case TenantMigrationDonorStateEnum::kUninitialized:
            break;
        case TenantMigrationDonorStateEnum::kDataSync:
            uassert(ErrorCodes::BadValue,
                    errmsg,
                    !donorStateDoc.getBlockTimestamp() && !donorStateDoc.getCommitOrAbortOpTime() &&
                        !donorStateDoc.getAbortReason());
            break;
        case TenantMigrationDonorStateEnum::kBlocking:
            uassert(ErrorCodes::BadValue,
                    errmsg,
                    donorStateDoc.getBlockTimestamp() && !donorStateDoc.getCommitOrAbortOpTime() &&
                        !donorStateDoc.getAbortReason());
            break;
        case TenantMigrationDonorStateEnum::kCommitted:
            uassert(ErrorCodes::BadValue,
                    errmsg,
                    donorStateDoc.getBlockTimestamp() && donorStateDoc.getCommitOrAbortOpTime() &&
                        !donorStateDoc.getAbortReason());
            break;
        case TenantMigrationDonorStateEnum::kAborted:
            uassert(ErrorCodes::BadValue, errmsg, donorStateDoc.getAbortReason());
            break;
        default:
            MONGO_UNREACHABLE;
    }

    return donorStateDoc;
}

void checkIfCanReadOrBlock(OperationContext* opCtx, StringData dbName) {
    auto mtab = TenantMigrationAccessBlockerRegistry::get(opCtx->getServiceContext())
                    .getTenantMigrationAccessBlockerForDbName(dbName);

    if (!mtab) {
        return;
    }

    // Source to cancel the timeout if the operation completed in time.
    CancelationSource cancelTimeoutSource;
    std::vector<ExecutorFuture<void>> futures;

    auto executor = mtab->getAsyncBlockingOperationsExecutor();
    futures.emplace_back(mtab->getCanReadFuture(opCtx).semi().thenRunOn(executor));

    // Optimisation: if the future is already ready, we are done.
    if (futures[0].isReady()) {
        futures[0].get();  // Throw if error.
        return;
    }

    if (opCtx->hasDeadline()) {
        auto deadlineReachedFuture =
            executor->sleepUntil(opCtx->getDeadline(), cancelTimeoutSource.token());
        // The timeout condition is optional with index #1.
        futures.push_back(std::move(deadlineReachedFuture));
    }

    const auto& [status, idx] = whenAny(std::move(futures)).get();

    if (idx == 0) {
        // Read unblock condition finished first.
        cancelTimeoutSource.cancel();
        uassertStatusOK(status);
    } else if (idx == 1) {
        // Deadline finished first, throw error.
        uassertStatusOK(Status(opCtx->getTimeoutError(),
                               "Read timed out waiting for tenant migration blocker",
                               mtab->getDebugInfo()));
    }
}

ExecutorFuture<void> getCanReadFuture(OperationContext* opCtx, StringData dbName,
boost::intrusive_ptr<ClientStrand> strand) {
    auto mtab = TenantMigrationAccessBlockerRegistry::get(opCtx->getServiceContext())
                    .getTenantMigrationAccessBlockerForDbName(dbName);

    if (!mtab) {
        return ExecutorFuture<void>(InlineExecutor::make(), Status::OK());
    }

    // Source to cancel the timeout if the operation completed in time.
    CancelationSource cancelTimeoutSource;

    auto canReadFuture = mtab->getCanReadFuture(opCtx);

    // Optimisation: if the future is already ready, we are done.
    if (canReadFuture.isReady()) {
        canReadFuture.get();  // Throw if error.
        return ExecutorFuture<void>(InlineExecutor::make(), Status::OK());
    }

    auto executor = mtab->getAsyncBlockingOperationsExecutor();
    std::vector<ExecutorFuture<void>> futures;
    futures.emplace_back(std::move(canReadFuture).semi().thenRunOn(executor));

    if (opCtx->hasDeadline()) {
        auto deadlineReachedFuture =
            executor->sleepUntil(opCtx->getDeadline(), cancelTimeoutSource.token());
        // The timeout condition is optional with index #1.
        futures.push_back(std::move(deadlineReachedFuture));
    }

    return whenAny(std::move(futures))
        .thenRunOn(executor)
        .then([cancelTimeoutSource, opCtx, mtab, executor,
                strand] (WhenAnyResult<void> result) mutable {
            auto strandBindToThread = strand->bind();
            const auto& [status, idx] = result;
            if (idx == 0) {
                // Read unblock condition finished first.
                std::cerr << "!!!! continuation in getCanReadFuture t " << std::this_thread::get_id() << std::endl;
                cancelTimeoutSource.cancel();
                uassertStatusOK(status);
            } else if (idx == 1) {
                // Deadline finished first, throw error.
                std::cerr << "!!!! timeout in getCanReadFuture t " << std::this_thread::get_id() << std::endl;
                uassertStatusOK(Status(opCtx->getTimeoutError(),
                                    "Read timed out waiting for tenant migration blocker",
                                    mtab->getDebugInfo()));
            }
        });
}

void checkIfLinearizableReadWasAllowedOrThrow(OperationContext* opCtx, StringData dbName) {
    if (repl::ReadConcernArgs::get(opCtx).getLevel() ==
        repl::ReadConcernLevel::kLinearizableReadConcern) {
        if (auto mtab = TenantMigrationAccessBlockerRegistry::get(opCtx->getServiceContext())
                            .getTenantMigrationAccessBlockerForDbName(dbName)) {
            mtab->checkIfLinearizableReadWasAllowedOrThrow(opCtx);
        }
    }
}

void onWriteToDatabase(OperationContext* opCtx, StringData dbName) {
    auto mtab = TenantMigrationAccessBlockerRegistry::get(opCtx->getServiceContext())
                    .getTenantMigrationAccessBlockerForDbName(dbName);

    if (mtab) {
        mtab->checkIfCanWriteOrThrow();
    }
}

void recoverTenantMigrationAccessBlockers(OperationContext* opCtx) {
    TenantMigrationAccessBlockerRegistry::get(opCtx->getServiceContext()).shutDown();

    if (MONGO_unlikely(skipRecoverTenantMigrationAccessBlockers.shouldFail())) {
        return;
    }

    PersistentTaskStore<TenantMigrationDonorDocument> store(
        NamespaceString::kTenantMigrationDonorsNamespace);
    Query query;

    store.forEach(opCtx, query, [&](const TenantMigrationDonorDocument& doc) {
        // Skip creating a TenantMigrationAccessBlocker for aborted migrations that have been
        // marked as garbage collected.
        if (doc.getExpireAt() && doc.getState() == TenantMigrationDonorStateEnum::kAborted)
            return true;

        auto mtab = std::make_shared<TenantMigrationAccessBlocker>(
            opCtx->getServiceContext(),
            doc.getTenantId().toString(),
            doc.getRecipientConnectionString().toString());

        TenantMigrationAccessBlockerRegistry::get(opCtx->getServiceContext())
            .add(doc.getTenantId(), mtab);

        switch (doc.getState()) {
            case TenantMigrationDonorStateEnum::kDataSync:
                break;
            case TenantMigrationDonorStateEnum::kBlocking:
                invariant(doc.getBlockTimestamp());
                mtab->startBlockingWrites();
                mtab->startBlockingReadsAfter(doc.getBlockTimestamp().get());
                break;
            case TenantMigrationDonorStateEnum::kCommitted:
                invariant(doc.getBlockTimestamp());
                mtab->startBlockingWrites();
                mtab->startBlockingReadsAfter(doc.getBlockTimestamp().get());
                mtab->setCommitOpTime(opCtx, doc.getCommitOrAbortOpTime().get());
                break;
            case TenantMigrationDonorStateEnum::kAborted:
                if (doc.getBlockTimestamp()) {
                    mtab->startBlockingWrites();
                    mtab->startBlockingReadsAfter(doc.getBlockTimestamp().get());
                }
                mtab->setAbortOpTime(opCtx, doc.getCommitOrAbortOpTime().get());
                break;
            default:
                MONGO_UNREACHABLE;
        }
        return true;
    });
}

void handleTenantMigrationConflict(OperationContext* opCtx, Status status) {
    auto migrationConflictInfo = status.extraInfo<TenantMigrationConflictInfo>();
    invariant(migrationConflictInfo);
    auto mtab = migrationConflictInfo->getTenantMigrationAccessBlocker();
    invariant(mtab);
    uassertStatusOK(mtab->waitUntilCommittedOrAborted(opCtx));
}

}  // namespace tenant_migration_donor

}  // namespace mongo
