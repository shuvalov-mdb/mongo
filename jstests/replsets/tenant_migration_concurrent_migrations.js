/**
 * Test that multiple concurrent tenant migrations are supported.
 *
 * Tenant migrations are not expected to be run on servers with ephemeralForTest, and in particular
 * this test fails on ephemeralForTest because the donor has to wait for the write to set the
 * migration state to "committed" and "aborted" to be majority committed but it cannot do that on
 * ephemeralForTest.
 *
 * @tags: [requires_fcv_47, requires_majority_read_concern, incompatible_with_eft]
 */

(function() {
'use strict';

load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallelTester.js");
load("jstests/libs/uuid_util.js");
load("jstests/replsets/libs/tenant_migration_test.js");
load("jstests/replsets/libs/tenant_migration_util.js");

let startupParams = {};

// TODO SERVER-51734: Remove the failpoint 'returnResponseOkForRecipientSyncDataCmd'.
startupParams["failpoint.returnResponseOkForRecipientSyncDataCmd"] = tojson({mode: 'alwaysOn'});

const rst0 = new ReplSetTest({nodes: 1, name: 'rst0', nodeOptions: {setParameter: startupParams}});
const rst1 = new ReplSetTest({nodes: 1, name: 'rst1', nodeOptions: {setParameter: startupParams}});
const rst2 = new ReplSetTest({nodes: 1, name: 'rst2', nodeOptions: {setParameter: startupParams}});

rst0.startSet();
rst0.initiate();

rst1.startSet();
rst1.initiate();

rst2.startSet();
rst2.initiate();

const kTenantIdPrefix = "testTenantId";

// Test concurrent outgoing migrations to different recipients.
(() => {
    const tenantMigrationTest0 = new TenantMigrationTest({donorRst: rst0, recipientRst: rst1});
    if (!tenantMigrationTest0.isFeatureFlagEnabled()) {
        jsTestLog("Skipping test because the tenant migrations feature flag is disabled");
        return;
    }
    const tenantMigrationTest1 = new TenantMigrationTest({donorRst: rst0, recipientRst: rst2});
    const tenantId0 = `${kTenantIdPrefix}-ConcurrentOutgoingMigrationsToDifferentRecipient0`;
    const tenantId1 = `${kTenantIdPrefix}-ConcurrentOutgoingMigrationsToDifferentRecipient1`;
    const donorPrimary = rst0.getPrimary();
    const connPoolStatsBefore = assert.commandWorked(donorPrimary.adminCommand({connPoolStats: 1}));

    const migrationOpts0 = {
        migrationIdString: extractUUIDFromObject(UUID()),
        tenantId: tenantId0,
    };
    const migrationOpts1 = {
        migrationIdString: extractUUIDFromObject(UUID()),
        tenantId: tenantId1,
    };

    assert.commandWorked(tenantMigrationTest0.startMigration(migrationOpts0));
    assert.commandWorked(tenantMigrationTest1.startMigration(migrationOpts1));

    const stateRes0 =
        assert.commandWorked(tenantMigrationTest0.waitForMigrationToComplete(migrationOpts0));
    const stateRes1 =
        assert.commandWorked(tenantMigrationTest1.waitForMigrationToComplete(migrationOpts1));

    // Verify that both migrations succeeded.
    assert(stateRes0.state, TenantMigrationTest.State.kCommitted);
    assert(stateRes1.state, TenantMigrationTest.State.kCommitted);

    const connPoolStatsAfter = assert.commandWorked(donorPrimary.adminCommand({connPoolStats: 1}));
    assert.eq(connPoolStatsAfter.numReplicaSetMonitorsCreated,
              connPoolStatsBefore.numReplicaSetMonitorsCreated + 2);

    assert.commandWorked(tenantMigrationTest0.forgetMigration(migrationOpts0.migrationIdString));
    assert.commandWorked(tenantMigrationTest1.forgetMigration(migrationOpts1.migrationIdString));
    assert.eq(Object.keys(connPoolStatsAfter.replicaSets).length, 0);
})();

// Test concurrent incoming migrations from different donors.
(() => {
    const tenantMigrationTest0 = new TenantMigrationTest({donorRst: rst0, recipientRst: rst2});
    if (!tenantMigrationTest0.isFeatureFlagEnabled()) {
        jsTestLog("Skipping test because the tenant migrations feature flag is disabled");
        return;
    }
    const tenantMigrationTest1 = new TenantMigrationTest({donorRst: rst1, recipientRst: rst2});
    const tenantId0 = `${kTenantIdPrefix}-ConcurrentIncomingMigrations0`;
    const tenantId1 = `${kTenantIdPrefix}-ConcurrentIncomingMigrations1`;

    const migrationOpts0 = {
        migrationIdString: extractUUIDFromObject(UUID()),
        tenantId: tenantId0,
    };
    const migrationOpts1 = {
        migrationIdString: extractUUIDFromObject(UUID()),
        tenantId: tenantId1,
    };

    assert.commandWorked(tenantMigrationTest0.startMigration(migrationOpts0));
    assert.commandWorked(tenantMigrationTest1.startMigration(migrationOpts1));

    const stateRes0 =
        assert.commandWorked(tenantMigrationTest0.waitForMigrationToComplete(migrationOpts0));
    const stateRes1 =
        assert.commandWorked(tenantMigrationTest1.waitForMigrationToComplete(migrationOpts1));

    // Verify that both migrations succeeded.
    assert(stateRes0.state, TenantMigrationTest.State.kCommitted);
    assert(stateRes1.state, TenantMigrationTest.State.kCommitted);
})();

// SERVER-50467: Ensure that tenant migration donor only removes a ReplicaSetMonitor for
// a recipient when the last migration to that recipient completes. Before SERVER-50467, one of the
// migration thread could try to remove the recipient RSM while the other is still using it.
// Test concurrent outgoing migrations to same recipient.
(() => {
    const tenantMigrationTest0 = new TenantMigrationTest({donorRst: rst0, recipientRst: rst1});
    if (!tenantMigrationTest0.isFeatureFlagEnabled()) {
        jsTestLog("Skipping test because the tenant migrations feature flag is disabled");
        return;
    }
    const tenantMigrationTest1 = new TenantMigrationTest({donorRst: rst0, recipientRst: rst1});

    const tenantId0 = `${kTenantIdPrefix}-ConcurrentOutgoingMigrationsToSameRecipient0`;
    const tenantId1 = `${kTenantIdPrefix}-ConcurrentOutgoingMigrationsToSameRecipient1`;

    const donorsColl = tenantMigrationTest0.getDonorRst().getPrimary().getCollection(
        TenantMigrationTest.kConfigDonorsNS);

    const migrationOpts0 = {
        migrationIdString: extractUUIDFromObject(UUID()),
        tenantId: tenantId0,
    };
    const migrationOpts1 = {
        migrationIdString: extractUUIDFromObject(UUID()),
        tenantId: tenantId1,
    };

    const donorPrimary = rst0.getPrimary();

    const connPoolStatsBefore = assert.commandWorked(donorPrimary.adminCommand({connPoolStats: 1}));

    let blockFp = configureFailPoint(donorPrimary, "pauseTenantMigrationAfterBlockingStarts");
    assert.commandWorked(tenantMigrationTest0.startMigration(migrationOpts0));
    assert.commandWorked(tenantMigrationTest1.startMigration(migrationOpts1));

    // Make sure that there is an overlap between the two migrations.
    blockFp.wait();
    blockFp.wait();
    blockFp.off();

    // Verify that both migrations succeeded.

    const stateRes0 =
        assert.commandWorked(tenantMigrationTest0.waitForMigrationToComplete(migrationOpts0));
    const stateRes1 =
        assert.commandWorked(tenantMigrationTest1.waitForMigrationToComplete(migrationOpts1));

    // Verify that both migrations succeeded.
    assert(stateRes0.state, TenantMigrationTest.State.kCommitted);
    assert(stateRes1.state, TenantMigrationTest.State.kCommitted);

    jsTestLog(donorsColl.find().toArray());
    assert(donorsColl.findOne({tenantId: migrationOpts0.tenantId, state: "committed"}));
    jsTestLog(migrationOpts1.tenantId);
    assert(donorsColl.findOne({tenantId: migrationOpts1.tenantId, state: "committed"}));

    // Verify that the recipient RSM was only created once and was removed after both migrations
    // finished.
    const connPoolStatsAfter = assert.commandWorked(donorPrimary.adminCommand({connPoolStats: 1}));
    assert.eq(connPoolStatsAfter.numReplicaSetMonitorsCreated,
              connPoolStatsBefore.numReplicaSetMonitorsCreated + 1);
    assert.eq(Object.keys(connPoolStatsAfter.replicaSets).length, 0);
})();

rst0.stopSet();
rst1.stopSet();
rst2.stopSet();
})();
