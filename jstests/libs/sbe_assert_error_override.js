/**
 * For a number of errors, the engine will raise different error codes when SBE mode is enabled vs.
 * disabled. When SBE mode is enabled, these differences in error codes can cause tests to fail that
 * otherwise would have passed.
 *
 * To expedite efforts to enable more tests under SBE mode, this file provides overrides for the
 * assert.commandFailedWithCode() and assert.writeErrorWithCode() APIs so that they treat certain
 * groups of error codes as being equivalent to each other. Note that these overrides also affect
 * how assertError(), assertErrorCode(), and assertErrCodeAndErrMsgContains() behave.
 *
 * Below the 'equivalentErrorCodesList' variable contains all known groups of error codes that
 * should be treated as equivalent to each other. As new groups of equivalent error codes are
 * discovered, they should be added to the list below.
 *
 * Note: This file should _only_ be included in a test if it has been observed that the test fails
 * due to differences in error codes when SBE mode is enabled.
 */
(function() {
"use strict";

// Below is the list of known equivalent error code groups. As new groups of equivalent error codes
// are discovered, they should be added to this list.
const equivalentErrorCodesList = [
    [16020, 5066300],
    [16007, 5066300],
    [28765, 4822870],
    [31034, 4848972],
    [31095, 4848972],
    [40515, 4848979],
    [40517, 4848980],
    [40523, 4848972],
];

// This map is generated based on the contents of 'equivalentErrorCodesList'. This map should _not_
// be modified. If you need to change which error codes are considered equivalent to each other, you
// should modify 'equivalentErrorCodesList' above.
const equivalentErrorCodesMap = function() {
    let mapOfSets = {};
    for (const arr of equivalentErrorCodesList) {
        for (const errorCode1 of arr) {
            if (!mapOfSets.hasOwnProperty(errorCode1)) {
                mapOfSets[errorCode1] = new Set();
            }

            for (const errorCode2 of arr) {
                if (errorCode1 != errorCode2) {
                    mapOfSets[errorCode1].add(errorCode2);
                }
            }
        }
    }

    let mapOfLists = {};
    for (const errorCode1 in mapOfSets) {
        let arr = [];
        for (const errorCode2 of mapOfSets[errorCode1]) {
            arr.push(errorCode2);
        }
        mapOfLists[errorCode1] = arr;
    }

    return mapOfLists;
}();

const lookupEquivalentErrorCodes = function(errorCodes) {
    if (!Array.isArray(errorCodes)) {
        errorCodes = [errorCodes];
    }

    let result = [];
    for (const errorCode1 of errorCodes) {
        result.push(errorCode1);
        if (equivalentErrorCodesMap.hasOwnProperty(errorCode1)) {
            for (const errorCode2 of equivalentErrorCodesMap[errorCode1]) {
                result.push(errorCode2);
            }
        }
    }

    return result;
};

// Override the assert.commandFailedWithCode() function.
const assertCommandFailedWithCodeOriginal = assert.commandFailedWithCode;
assert.commandFailedWithCode = function(res, expectedCode, msg) {
    return assertCommandFailedWithCodeOriginal(res, lookupEquivalentErrorCodes(expectedCode), msg);
};

// Override the assert.writeErrorWithCode() function.
const assertWriteErrorWithCodeOriginal = assert.writeErrorWithCode;
assert.writeErrorWithCode = function(res, expectedCode, msg) {
    return assertWriteErrorWithCodeOriginal(res, lookupEquivalentErrorCodes(expectedCode), msg);
};
}());
