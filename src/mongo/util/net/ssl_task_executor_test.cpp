/**
 *    Copyright (C) 2018-present MongoDB, Inc.
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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kExecutor

#include "mongo/platform/basic.h"

#include "mongo/executor/thread_pool_mock.h"
#include "mongo/executor/thread_pool_task_executor.h"
#include "mongo/executor/thread_pool_task_executor_test_fixture.h"
#include "mongo/s/sharding_router_test_fixture.h"
#include "mongo/s/sharding_task_executor.h"

namespace mongo {
namespace {

const HostAndPort kTestConfigShardHost("FakeConfigHost", 12345);

using executor::NetworkInterfaceMock;
using executor::RemoteCommandRequest;
using executor::TaskExecutor;

LogicalSessionId constructFullLsid() {
    auto id = UUID::gen();
    auto uid = SHA256Block{};

    return LogicalSessionId(id, uid);
}

class SSLTaskExecutorTest : public ShardingTestFixture {
protected:
    SSLTaskExecutorTest() {
        configTargeter()->setFindHostReturnValue(kTestConfigShardHost);

        auto netForFixedTaskExecutor = std::make_unique<executor::NetworkInterfaceMock>();
        _network = netForFixedTaskExecutor.get();

        _threadPool = makeThreadPoolTestExecutor(std::move(netForFixedTaskExecutor));
    }

    void assertOpCtxLsidEqualsCmdObjLsid(const BSONObj& cmdObj) {
        auto opCtxLsid = operationContext()->getLogicalSessionId();

        ASSERT(opCtxLsid);

        auto cmdObjLsid = LogicalSessionFromClient::parse("lsid"_sd, cmdObj["lsid"].Obj());

        ASSERT_EQ(opCtxLsid->getId(), cmdObjLsid.getId());
        ASSERT_EQ(opCtxLsid->getUid(), *cmdObjLsid.getUid());
    }

    executor::NetworkInterfaceMock* _network{nullptr};

    std::unique_ptr<executor::ThreadPoolTaskExecutor> _threadPool;
};

}  // namespace
}  // namespace mongo
