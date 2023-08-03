/**
 * @file
 * A workload designed to stress the cursor manager.
 *
 * ### *Test*
 * The purpose of this test is to expose scalability issues in the cursor manager. As part of the
 * "all cursors globally managed" project, in mongodb 4.2 all cursors are owned by a single cursor
 * manager (rather than by a per-collection cursor manager, as in 4.0 and earlier). Putting all
 * cursors into a single data structure created an obvious opportunity for a bottleneck on finds,
 * getMores and killCursors.
 *
 * The test inserts some data into several collections, and then has many threads run find and
 * getMore operations with a small batch size, in an attempt to create contention in the cursor
 * manager.
 *
 * ### *Setup*
 *
 *   Supports standalone
 *
 * ### *Owning-team*
 * mongodb/product-query
 * 
 * ### *Keywords*
 * 
 * @module workloads/cursor_manager
 */

var dbName = "cursor_manager";
var testDB = db.getSiblingDB(dbName);
var colls = [];

/**
 * The number of collections to create and run queries against
 */
var nCollections = 16;
for (var i = 0; i < nCollections; ++i) {
    colls.push(testDB.getCollection("cursor_manager_" + i));
}

var insertBatchSize = 1000;

/**
 * Number of documents per collection.
 */
var numDocs = 10000;

/**
 * The number of threads to run in parallel.
 * TODO PERF-1809: Determine thread levels for this workload.
 */
var thread_levels = thread_levels || [32, 64, 128, 256];

/**
 * Duration of the test.
 */
var test_duration_secs = 60 * 5;  // 5 minutes.

/**
 * Represents a thread which takes a collection name, and repeatedly opens and iterates a cursor on
 * the collection with a small batch size.
 */
var CursorIteratingThread = function(dbName, collName) {
    // Used for informing the thread to stop.
    var _continueCounter = null;
    // Used for unblocking the thread after it's been created.
    var _blockedCounter = null;
    // The underlying ScopedThread.
    var _thread = null;

    function _iterateCursor(continueCounter, blockedCounter, dbName, collName) {
        blockedCounter.await();

        var coll = db.getSiblingDB(dbName)[collName];
        var makeNewCursor = function() {
            return coll.find().batchSize(2);
        };

        var cursor = makeNewCursor();
        var docsReturned = 0;

        var shouldContinue = function() {
            return continueCounter.getCount() > 0;
        };

        while (shouldContinue()) {
            var cursor = makeNewCursor();
            while (shouldContinue() && cursor.hasNext()) {
                cursor.next();
                ++docsReturned;
            }
        }

        return docsReturned;
    }

    /**
     * Spawns a ScopedThread that will run _iterateCursor().
     */
    this.start = function() {
        assert.eq(_thread, null);

        _continueCounter = CountDownLatch(1);
        _blockedCounter = CountDownLatch(1);
        _thread =
            new ScopedThread(_iterateCursor, _continueCounter, _blockedCounter, dbName, collName);
        _thread.start();
    };

    /**
     * Unblocks the scoped thread from a busy wait, and allows it to start running find and getMore
     * commands.
     */
    this.unblock = function() {
        assert.neq(_thread, null);
        _blockedCounter.countDown();
    };

    /**
     * Informs the thread that it should stop. Call join() after this to wait until the thread
     * completes.
     */
    this.stop = function() {
        assert.neq(_thread, null);

        _continueCounter.countDown();
    };

    /**
     * Joins the thread.
     */
    this.join = function() {
        _thread.join();
        var totalDocsExamined = _thread.returnData();
        _thread = null;
        _continueCounter = null;
        return totalDocsExamined;
    };
};

/**
 * For each collection in the 'colls' array, insert 'numDocs' documents, each with just two integer
 * fields, e.g. {x: 1, y: 3}.
 */
function insertDocs(numDocs, colls) {
    assert.eq(
        numDocs % insertBatchSize,
        0,
        "numDocs must be divisible by the insert batch size in order for the right number of " +
            "documents to be inserted");
    for (var collIter = 0; collIter < colls.length; ++collIter) {
        jsTestLog("Inserting to collection " + collIter + " of " + colls.length);
        var coll = colls[collIter];

        for (var i = 0; i < numDocs / insertBatchSize; i++) {
            var bulk = coll.initializeUnorderedBulkOp();
            for (var j = 0; j < insertBatchSize; j++) {
                bulk.insert({x: i, y: j});
            }
            bulk.execute();
        }
    }
}

/**
 * Create some CursorIteratingThreads, evenly assigning them to the given collections.  Initially
 * the threads are in a "blocked" state, and, although they are running, do not do anything.
 */
function createQueryingThreads(nThreads, colls) {
    var threads = [];
    for (var i = 0; i < nThreads; ++i) {
        var collName = colls[i % colls.length].getName();

        var thread = new CursorIteratingThread(dbName, collName);
        thread.start();
        threads.push(thread);
    }

    return threads;
}

/**
 * Given a list of threads, unblock them all.
 */
function unblockQueryingThreads(threads) {
    for (var i = 0; i < threads.length; ++i) {
        threads[i].unblock();
    }
}

/**
 * Inform each thread to stop, and return when they've all completed.
 * Returns the total number of documents that were returned from the server across all threads.
 */
function joinThreads(threads) {
    var totalDocsReturned = 0;
    for (var i = 0; i < threads.length; ++i) {
        threads[i].stop();
    }

    for (var i = 0; i < threads.length; ++i) {
        totalDocsReturned += threads[i].join();
    }

    return totalDocsReturned;
}

function runQueriesAndReportThroughput(colls, nThreads) {
    var threads = createQueryingThreads(nThreads, colls);

    var start = Date.now();
    unblockQueryingThreads(threads);

    // Wait for the threads driving load against the server to run for some time.
    sleep(test_duration_secs * 1000);

    var docsReturned = joinThreads(threads);

    var end = Date.now();
    var secondsElapsed = (end - start) / 1000;

    print("Seconds elapsed was: " + tojson(secondsElapsed),
          " intended test duration was: " + tojson(test_duration_secs));

    reportThroughput("cursor_manager", docsReturned / secondsElapsed, {nThread: nThreads});
}

// Main function to run the test
function run_test(dbToTest, colls) {
    insertDocs(numDocs, colls);
    quiesceSystem();

    thread_levels.forEach(function(nThreads) {
        assert.eq(nThreads % colls.length, 0, "Should have even number of threads per collection");
        runQueriesAndReportThroughput(colls, nThreads);
        quiesceSystem();
    });
    dbToTest.dropDatabase();
}

// Driver
run_test(testDB, colls);
