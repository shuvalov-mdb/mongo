/**
 * Stress test verifies that non-idempotent multi updates made during tenant migration
 * were not retried on migration abort, which would create duplicate updates. Partially
 * updated collection where each update is applied no more than once is still an expected result.
 * 
 * @tags: [requires_fcv_47, requires_majority_read_concern, incompatible_with_eft]
 */

(function() {
"use strict";

load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallelTester.js");
load("jstests/libs/uuid_util.js");
load("jstests/replsets/libs/tenant_migration_test.js");
load("jstests/replsets/libs/tenant_migration_util.js");

const tenantMigrationTest = new TenantMigrationTest({name: jsTestName()});
if (!tenantMigrationTest.isFeatureFlagEnabled()) {
    jsTestLog("Skipping test because the tenant migrations feature flag is disabled");
    return;
}

const donorRst = tenantMigrationTest.getDonorRst();

const primary = donorRst.getPrimary();

const kTenantIdPrefix = "testTenantId";
const kCollName = "testColl";
const kTenantDefinedDbName = "0";
const kRecords = 1000;

function prepareDatabase(dbName) {
    let db = primary.getDB(dbName);
    assert.commandWorked(db.runCommand({create: kCollName}));
    for (let i = 0; i < kRecords;) {
        let bulk = db[kCollName].initializeUnorderedBulkOp();
        for (let j = 0; j < 100 && i < kRecords; ++j, ++i) {
            bulk.insert({_id: i, x: -1, a: [1, 2]});
        }
        assert.commandWorked(bulk.execute());
    }
}

function doMultiUpdate(primaryHost, dbName, collName, records) {
    const primary = new Mongo(primaryHost);
    const cycles = 400;
    let db = primary.getDB(dbName);
    let bulkRes;
    let completeCycles;

    for (completeCycles = 0; completeCycles < cycles; ++completeCycles) {
        var bulk = db[collName].initializeUnorderedBulkOp();
        try {
            bulk.find({a: 2}).update({$inc: {x: 1}});
            bulkRes = bulk.execute({w: "majority"}).toSingleResult();
            if (bulkRes["nModified"] != records && bulkRes["nModified"] > 0) {
                jsTest.log('Detected partial update');
                jsTest.log(bulkRes);
                break;
            }
        } catch(err) {
            jsTest.log(err);
            if (err["nModified"] != records && err["nModified"] > 0) {
                break;  // The final consistency analysis allows one failure.
            }
            jsTest.log('Continue the test');
        }
    }

    // Verify the data after update.
    let result = assert.commandWorked(db.runCommand({
        find: collName,
    }));
    for (let j = 0; j < result.length; ++j) {
        if (result[j]['x'] != completeCycles && result[j]['x'] != completeCycles - 1) {
            assert(false, `Unexpected result ${result}`);
        }
    }
    jsTest.log('All updates completed without error');
}

function testMultiWritesWhileInBlockingState() {
    const donorRst = tenantMigrationTest.getDonorRst();
    const donorPrimary = donorRst.getPrimary();

    const tenantId = `${kTenantIdPrefix}-multiWrites`;
    const dbName = tenantMigrationTest.tenantDB(tenantId, kTenantDefinedDbName);

    let db = primary.getDB(dbName);
    prepareDatabase(dbName);

    // Start non-idempotent writes in a thread.
    let writesThread = new Thread(doMultiUpdate, primary.host, dbName, kCollName, kRecords);
    writesThread.start();

    for (let i = 0; i < 10; ++i) {
        const migrationId = UUID();
        const migrationOpts = {
            migrationIdString: extractUUIDFromObject(migrationId),
            tenantId,
            recipientConnString: tenantMigrationTest.getRecipientConnString(),
        };
        let abortFp = configureFailPoint(db, "abortTenantMigrationAfterBlockingStarts", {
           blockTimeMS: Math.floor(Math.random() * 10),
        });
        jsTest.log('Fault inject done');
        assert.commandWorked(tenantMigrationTest.startMigration(migrationOpts));
        jsTest.log('Start migration passed');

        const stateRes = assert.commandWorked(tenantMigrationTest.waitForMigrationToComplete(
            migrationOpts, false /* retryOnRetryableErrors */));
        assert.eq(stateRes.state, TenantMigrationTest.State.kAborted);
        jsTest.log('Got migration completion');

        abortFp.wait();
        jsTest.log('After fp wait');
        abortFp.off();
        jsTest.log('After fp wait');

        assert.commandWorked(tenantMigrationTest.forgetMigration(migrationOpts.migrationIdString));
        jsTest.log('Migration cycle completed.');
    }

    writesThread.join();
}

jsTest.log("Test sending multi write while in migration blocking state");
testMultiWritesWhileInBlockingState();

tenantMigrationTest.stop();
})();
