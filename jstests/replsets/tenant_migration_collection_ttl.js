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

const kGarbageCollectionDelayMS = 5 * 1000;

const garbageCollectionOpts = {
    // Set the delay before a donor state doc is garbage collected to be short to speed
    // up the test.
    tenantMigrationGarbageCollectionDelayMS: kGarbageCollectionDelayMS,
    ttlMonitorSleepSecs: 1
};

const donorRst = new ReplSetTest({
    nodes: [{}, {rsConfig: {priority: 0}}, {rsConfig: {priority: 0}}],
    name: "TenantMigrationTest_donor",
    nodeOptions: Object.assign(TenantMigrationUtil.makeX509OptionsForTest().donor,
        {setParameter: garbageCollectionOpts })
});
donorRst.startSet();
donorRst.initiateWithHighElectionTimeout();

const recipientRst = new ReplSetTest({
    nodes: [{}, {rsConfig: {priority: 0}}, {rsConfig: {priority: 0}}],
    name: "TenantMigrationTest_recipient",
    nodeOptions: Object.assign(TenantMigrationUtil.makeX509OptionsForTest().recipient,
    {setParameter: garbageCollectionOpts })
});
recipientRst.startSet();
recipientRst.initiateWithHighElectionTimeout();

const tenantMigrationTest = new TenantMigrationTest({name: jsTestName(), donorRst: donorRst, recipientRst: recipientRst});
if (!tenantMigrationTest.isFeatureFlagEnabled()) {
    jsTestLog("Skipping test because the tenant migrations feature flag is disabled");
    donorRst.stopSet();
    recipientRst.stopSet();
    return;
}

const tenantId = "testTenantId";
const dbName = tenantMigrationTest.tenantDB(tenantId, "testDB");
const collName = "testColl";

const donorPrimary = donorRst.getPrimary();
const recipientPrimary = recipientRst.getPrimary();

// Timestamp to use in TTL.
const timestamp = new ISODate();
const numDocs = 20;

function prepareData() {
    const testData = [];
    for (let i = 0; i < numDocs; ++i) {
        testData.push({_id: i, time: timestamp});
    }
    return testData;
}

function prepareDb(ttlTimeoutSeconds = 0) {
    let db = donorPrimary.getDB(dbName);
    try {
        db.dropDatabase();
    } catch (err) {
        // First time the DB doesn't exist.
    }
    tenantMigrationTest.insertDonorDB(dbName, collName, prepareData());
    // Create TTL index.
    assert.commandWorked(db[collName].createIndex({time: 1},
        {expireAfterSeconds: ttlTimeoutSeconds}));
}

function getNumTTLPasses(node) {
    let serverStatus = assert.commandWorked(node.adminCommand({serverStatus: 1}));
    jsTestLog(`TTL: ${tojson(serverStatus.metrics.ttl)}`);
    return serverStatus.metrics.ttl.passes;
}

function waitForOneTtlPassAtNode(node) {
    // Wait for one TTL pass.
    let initialTtlCount = getNumTTLPasses(node);
    assert.soon(() => {
        return getNumTTLPasses(node) > initialTtlCount;
    }, "TTLMonitor never did any passes.");
}

function testRecipientDb() {
    jsTestLog("Test recipient DB");
    waitForOneTtlPassAtNode(recipientPrimary);
    let db = recipientPrimary.getDB(dbName);
    let found = db[collName].find({}).count();
    jsTest.log(`${found} documents in the recipient collection`);
}

(() => {
    jsTest.log("Test that the TTL does not delete documents during tenant migration");

    prepareDb();

    const migrationId = UUID();
    const migrationOpts = {
        migrationIdString: extractUUIDFromObject(migrationId),
        tenantId: tenantId,
        recipientConnString: tenantMigrationTest.getRecipientConnString(),
    };
    let abortFp =
    configureFailPoint(donorPrimary, "abortTenantMigrationBeforeLeavingBlockingState", {
        blockTimeMS: Math.floor(Math.random() * 10),
    });
    assert.commandWorked(tenantMigrationTest.startMigration(migrationOpts));

    const stateRes = assert.commandWorked(tenantMigrationTest.waitForMigrationToComplete(
        migrationOpts, false /* retryOnRetryableErrors */));
    assert.eq(stateRes.state, TenantMigrationTest.State.kAborted);

    abortFp.wait();
    abortFp.off();

    testRecipientDb();
    assert.commandWorked(tenantMigrationTest.forgetMigration(migrationOpts.migrationIdString));
})();

tenantMigrationTest.stop();
donorRst.stopSet();
recipientRst.stopSet();
})();
