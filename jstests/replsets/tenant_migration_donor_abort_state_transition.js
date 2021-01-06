/**
 * Tests that the migration still proceeds successfully after a state transition write aborts.
 *
 * @tags: [requires_fcv_47, incompatible_with_eft, requires_majority_read_concern]
 */
(function() {
"use strict";

load("jstests/libs/fail_point_util.js");
load("jstests/libs/uuid_util.js");
load("jstests/libs/parallelTester.js");
load("jstests/replsets/libs/tenant_migration_test.js");
load("jstests/replsets/libs/tenant_migration_util.js");

const kTenantIdPrefix = "testTenantId";

const donorRst = new ReplSetTest({
    nodes: 2,
    name: jsTestName() + "_donor",
    nodeOptions: {
        setParameter: {
            // To support blocking state timing out test.
            tenantMigrationBlockingStateTimeoutMS: 5000,
        }
    }
});
donorRst.startSet();
donorRst.initiateWithHighElectionTimeout();

const tenantMigrationTest = new TenantMigrationTest({name: jsTestName(), donorRst: donorRst});
if (!tenantMigrationTest.isFeatureFlagEnabled()) {
    jsTestLog("Skipping test because the tenant migrations feature flag is disabled");
    return;
}

/**
 * Starts a migration and forces the write to insert the donor's state doc to abort on the first few
 * tries. Asserts that the migration still completes successfully.
 */
function testAbortInitialState() {
    const donorPrimary = donorRst.getPrimary();

    // Force the storage transaction for the insert to abort prior to inserting the WiredTiger
    // record.
    let writeConflictFp = configureFailPoint(donorPrimary, "WTWriteConflictException");

    const tenantId = `${kTenantIdPrefix}-initial`;
    const migrationId = UUID();
    const migrationOpts = {
        migrationIdString: extractUUIDFromObject(migrationId),
        tenantId,
        recipientConnString: tenantMigrationTest.getRecipientConnString(),
    };

    const donorRstArgs = TenantMigrationUtil.createRstArgs(donorRst);
    jsTestLog(donorRstArgs);

    // Run the migration in its own thread, since the initial 'donorStartMigration' command will
    // hang due to the failpoint.
    let migrationThread =
        new Thread(TenantMigrationUtil.runMigrationAsync, migrationOpts, donorRstArgs);
    migrationThread.start();
    writeConflictFp.wait();

    // Next, force the storage transaction for the insert to abort after inserting the WiredTiger
    // record and initializing the in-memory migration state.
    let opObserverFp = configureFailPoint(donorPrimary, "donorOpObserverFailAfterOnInsert");
    writeConflictFp.off();
    opObserverFp.wait();
    opObserverFp.off();

    // Verify that the migration completes successfully.
    let threadResult = migrationThread.returnData();
    jsTestLog(threadResult);
    assert.commandWorked(threadResult);
    tenantMigrationTest.waitForNodesToReachState(
        donorRst.nodes, migrationId, tenantId, TenantMigrationTest.State.kCommitted);

    assert.commandWorked(tenantMigrationTest.forgetMigration(migrationOpts.migrationIdString));
}

function testTimeoutBlockingState() {
    const donorPrimary = donorRst.getPrimary();

    const tenantId = `${kTenantIdPrefix}-blockingTimeout`;
    const migrationId = UUID();
    const migrationOpts = {
        migrationIdString: extractUUIDFromObject(migrationId),
        tenantId,
        recipientConnString: tenantMigrationTest.getRecipientConnString(),
    };

    const donorRstArgs = TenantMigrationUtil.createRstArgs(donorRst);

    // Fail point to pause right before entering the blocking mode.
    let afterDataSyncFp = configureFailPoint(donorPrimary, "pauseTenantMigrationAfterDataSync");

    // Run the migration in its own thread, since the initial 'donorStartMigration' command will
    // hang due to the fail point.
    let migrationThread =
        new Thread(TenantMigrationUtil.runMigrationAsync, migrationOpts, donorRstArgs);
    migrationThread.start();

    afterDataSyncFp.wait();
    // Fail point to pause the '_sendRecipientSyncDataCommand()' call inside the blocked
    // section until the cancellation token for the method is cancelled.
    let inCallFp =
        configureFailPoint(donorPrimary, "pauseScheduleCallWithCancelTokenUntilCanceled");
    afterDataSyncFp.off();

    tenantMigrationTest.waitForNodesToReachState(
        donorRst.nodes, migrationId, tenantId, TenantMigrationTest.State.kAborted);

    const stateRes = assert.commandWorked(migrationThread.returnData());
    assert.eq(stateRes.state, TenantMigrationTest.State.kAborted);
    assert.eq(stateRes.abortReason.code, ErrorCodes.ExceededTimeLimit);

    // This fail point is pausing all calls to recipient, so it has to be disabled to make
    // the 'forget migration' command to work.
    inCallFp.off();
    assert.commandWorked(tenantMigrationTest.forgetMigration(migrationOpts.migrationIdString));
}

/**
 * Starts a migration after enabling 'pauseFailPoint' (must pause the migration) and
 * 'setUpFailPoints' on the donor's primary. Forces the write to transition to 'nextState' after
 * reaching 'pauseFailPoint' to abort on the first few tries. Asserts that the migration still
 * completes successfully.
 */
function testAbortStateTransition(pauseFailPoint, setUpFailPoints, nextState) {
    jsTest.log(`Test aborting the write to transition to state "${
        nextState}" after reaching failpoint "${pauseFailPoint}"`);

    const donorPrimary = donorRst.getPrimary();
    const tenantId = `${kTenantIdPrefix}-${nextState}`;

    const migrationId = UUID();
    const migrationOpts = {
        migrationIdString: extractUUIDFromObject(migrationId),
        tenantId,
    };

    let failPointsToClear = [];
    setUpFailPoints.forEach(failPoint => {
        failPointsToClear.push(configureFailPoint(donorPrimary, failPoint));
    });
    let pauseFp = configureFailPoint(donorPrimary, pauseFailPoint);

    assert.commandWorked(tenantMigrationTest.startMigration(migrationOpts));
    pauseFp.wait();

    // Force the storage transaction for the write to transition to the next state to abort prior to
    // updating the WiredTiger record.
    let writeConflictFp = configureFailPoint(donorPrimary, "WTWriteConflictException");
    pauseFp.off();
    writeConflictFp.wait();

    // Next, force the storage transaction for the write to abort after updating the WiredTiger
    // record and registering the change.
    let opObserverFp = configureFailPoint(donorPrimary, "donorOpObserverFailAfterOnUpdate");
    writeConflictFp.off();
    opObserverFp.wait();
    opObserverFp.off();

    // Verify that the migration completes successfully.
    assert.commandWorked(tenantMigrationTest.waitForMigrationToComplete(migrationOpts));
    if (nextState === TenantMigrationTest.State.kAborted) {
        tenantMigrationTest.waitForNodesToReachState(
            donorRst.nodes, migrationId, tenantId, TenantMigrationTest.State.kAborted);
    } else {
        tenantMigrationTest.waitForNodesToReachState(
            donorRst.nodes, migrationId, tenantId, TenantMigrationTest.State.kCommitted);
    }
    failPointsToClear.forEach(failPoint => {
        failPoint.off();
    });

    assert.commandWorked(tenantMigrationTest.forgetMigration(migrationOpts.migrationIdString));
}

jsTest.log("Test aborting donor's state doc insert");
testAbortInitialState();

jsTest.log("Test timeout of the blocking state");
testTimeoutBlockingState();

jsTest.log("Test aborting donor's state doc update");
[{
    pauseFailPoint: "pauseTenantMigrationAfterDataSync",
    nextState: TenantMigrationTest.State.kBlocking
},
 {
     pauseFailPoint: "pauseTenantMigrationAfterBlockingStarts",
     nextState: TenantMigrationTest.State.kCommitted
 },
 {
     pauseFailPoint: "pauseTenantMigrationAfterBlockingStarts",
     setUpFailPoints: ["abortTenantMigrationAfterBlockingStarts"],
     nextState: TenantMigrationTest.State.kAborted
 }].forEach(({pauseFailPoint, setUpFailPoints = [], nextState}) => {
    testAbortStateTransition(pauseFailPoint, setUpFailPoints, nextState);
});

tenantMigrationTest.stop();
donorRst.stopSet();
}());
