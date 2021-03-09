/**
 * Tests that the collection TTL is suspended during tenant migration to
 * avoid consistency errors as the data synchronization phase may operate
 * concurrently with TTL deletions.
 *
 * @tags: [requires_fcv_47, requires_majority_read_concern, incompatible_with_eft,
 * incompatible_with_windows_tls]
 */

(function() {
"use strict";

load("jstests/libs/fail_point_util.js");
load("jstests/libs/uuid_util.js");
load("jstests/replsets/libs/tenant_migration_test.js");
load("jstests/replsets/libs/tenant_migration_util.js");

const garbageCollectionOpts = {
    // Set the delay before a donor state doc is garbage collected to be short to speed
    // up the test.
    tenantMigrationGarbageCollectionDelayMS: 5 * 1000,
    // Set the TTL interval large enough to decrease the probability of races.
    ttlMonitorSleepSecs: 5
};

const tenantMigrationTest = new TenantMigrationTest(
    {name: jsTestName(), sharedOptions: {setParameter: garbageCollectionOpts}});
if (!tenantMigrationTest.isFeatureFlagEnabled()) {
    jsTestLog("Skipping test because the tenant migrations feature flag is disabled");
    donorRst.stopSet();
    recipientRst.stopSet();
    return;
}

const tenantId = "testTenantId";
const dbName = tenantMigrationTest.tenantDB(tenantId, "testDB");
const collName = "testColl";

const donorRst = tenantMigrationTest.getDonorRst();
const recipientRst = tenantMigrationTest.getRecipientRst();
const donorPrimary = donorRst.getPrimary();
const recipientPrimary = recipientRst.getPrimary();

const numDocs = 20;

function prepareData() {
    // Timestamp to use in TTL.
    const timestamp = new Date();
    // This creates an array of tuples.
    return Array.apply(null, Array(numDocs)).map(function(x, i) {
        return {_id: i, time: timestamp};
    });
}

function prepareDb(ttlTimeoutSeconds = 0) {
    let db = donorPrimary.getDB(dbName);
    tenantMigrationTest.insertDonorDB(dbName, collName, prepareData());
    // Create TTL index.
    assert.commandWorked(
        db[collName].createIndex({time: 1}, {expireAfterSeconds: ttlTimeoutSeconds}));
}

function getNumTTLPasses(node) {
    let serverStatus = assert.commandWorked(node.adminCommand({serverStatus: 1}));
    jsTestLog(`TTL: ${tojson(serverStatus.metrics.ttl)}`);
    return serverStatus.metrics.ttl.passes;
}

function waitForOneTTLPassAtNode(node) {
    // Wait for one TTL pass.
    let initialTTLCount = getNumTTLPasses(node);
    assert.soon(() => {
        return getNumTTLPasses(node) > initialTTLCount;
    }, "TTLMonitor never did any passes.");
}

function assertTTLNotDeleteExpiredDocs(node) {
    let db = node.getDB(dbName);
    assert.eq(numDocs, db[collName].find({}).count());
}

function assertTTLDeleteExpiredDocs(node) {
    waitForOneTTLPassAtNode(node);
    let db = node.getDB(dbName);
    assert.soon(() => {
        let found = db[collName].find({}).count();
        jsTest.log(`${found} documents in the ${node} collection`);
        return found == 0;
    }, `TTL doesn't clean the database at ${node}`);
}

(() => {
    jsTest.log("Test that the TTL does not delete documents during tenant migration");

    // Force the donor to preserve all snapshot history to ensure that transactional reads do not
    // fail with TransientTransactionError "Read timestamp is older than the oldest available
    // timestamp".
    donorRst.nodes.forEach(node => {
        configureFailPoint(node, "WTPreserveSnapshotHistoryIndefinitely");
    });

    const migrationId = UUID();
    const migrationOpts = {
        migrationIdString: extractUUIDFromObject(migrationId),
        tenantId: tenantId,
        recipientConnString: tenantMigrationTest.getRecipientConnString(),
    };
    let blockFp =
        configureFailPoint(donorPrimary, "pauseTenantMigrationBeforeLeavingBlockingState");

    waitForOneTTLPassAtNode(donorPrimary);
    const ttlPassesAtDonorBeforeTest = getNumTTLPasses(donorPrimary);
    prepareDb();

    assert.commandWorked(tenantMigrationTest.startMigration(migrationOpts));
    // TTL interval should be long enough to avoid any race.
    assert.eq(ttlPassesAtDonorBeforeTest, getNumTTLPasses(donorPrimary));

    // Tests that the TTL cycle at the donor will not erase any docs.
    waitForOneTTLPassAtNode(donorPrimary);
    assertTTLNotDeleteExpiredDocs(donorPrimary);
    blockFp.wait();
    blockFp.off();

    const stateRes =
        assert.commandWorked(tenantMigrationTest.waitForMigrationToComplete(migrationOpts));
    assert.eq(stateRes.state, TenantMigrationTest.State.kCommitted);

    // Tests that the TTL cleanup was suspended during the tenant migration.
    waitForOneTTLPassAtNode(donorPrimary);
    waitForOneTTLPassAtNode(recipientPrimary);
    assertTTLNotDeleteExpiredDocs(donorPrimary);
    assertTTLNotDeleteExpiredDocs(recipientPrimary);

    assert.commandWorked(tenantMigrationTest.forgetMigration(migrationOpts.migrationIdString));

    // After the tenant migration is aborted, the TTL cleanup is restored.
    assertTTLDeleteExpiredDocs(recipientPrimary);

    // Tests that the TTL at donor is still suspended, and thus the collection cleanup
    // at the recipient side was TTL and not sync.
    assertTTLNotDeleteExpiredDocs(donorPrimary);
    assertTTLDeleteExpiredDocs(donorPrimary);
})();

tenantMigrationTest.stop();
donorRst.stopSet();
recipientRst.stopSet();
})();
