/**
 * @file
 * A workload designed to profile indexing a single field.
 *
 * ### *Test*
 *
 * Index a collection of 10M documents of size ~1KB, the field indexed contains a random integer
 * field (in the range 0 to 100M -1).
 *
 * Results are reported as the number of docs indexed / second.
 *
 * ### *Setup*
 *
 *   Supports standalone, replica and shard
 *
 * ### *Notes*
 *
 *   * Insert 10M documents, each slightly larger than 1KB and containing the following fields:
 *     - *_id*: an ObjectId
 *     - *paddingField*: a string of 1K 'x's
 *     - *numericField*: a random integer numeric field in the range 0 to 100M -1.
 *
 * ### *Owning-team*
 * mongodb/product-query
 * 
 * ### *Keywords*
 * 
 * @module workloads/index_build
 */
/*global
  db sharded Random enableSharding shardCollection quiesceSystem
  benchRun benchStart benchFinish sh print printjson assert
  reportThroughput sleep server jsTest version
*/

/**
 * Whether or not to perform a {background: true} index build.
 */
var build_in_background = build_in_background || false;

var dbName = "indexBuild";
var collName = "numbers";

var testDB = db.getSiblingDB(dbName);
var coll = testDB.getCollection(collName);
var docSize =  1024;
var indexKeys = {"numericField" : 1};
var indexOptions = {};
var batchSize = 1000;
var numDocs = 10000000; // 10M documents

function cleanup(_db) {
    _db.dropDatabase();
}


// Create a padding field of size docSize with xs filled in
function makePaddingField(docSize) {
    var paddingFieldValue = "";
    for (var i = 0; i < docSize; i++) {
        paddingFieldValue += "x";
    }
    return paddingFieldValue;
}

// Insert numDocs documents each with _id, paddingField of docSize 'x's and a random integer
// numeric field. Each document will be slightly larger than 1KB
function insertDocs(docSize, numDocs, coll) {

    Random.setRandomSeed(341215145);
    var paddingFieldValue = makePaddingField(docSize);

    for (var i = 0; i < numDocs/batchSize; i++) {
        var bulk = coll.initializeUnorderedBulkOp();
        for (var j = 0; j < batchSize; j++) {
            bulk.insert({ "paddingField" : paddingFieldValue,
                          "numericField" : Random.randInt(100000000)});
        }
        bulk.execute();
    }
}

// Create and measure the time to create the index
function createIndexAndReportThroughput(coll, background) {

    var indexStart = Date.now();

    var options = indexOptions;
    options.background = background;
    coll.createIndex(indexKeys, options);

    var indexEnd = Date.now();
    var indexSecondsElapsed = (indexEnd - indexStart)/1000;
    print("Seconds to build the index on " + numDocs + " documents: " + indexSecondsElapsed);

    var name = "index_build" + ((background) ? "_background" : "");
    reportThroughput(name, numDocs/indexSecondsElapsed, {nThread: 1});
}


// Main function to run the test
function run_test(_db, coll, background) {
    jsTestLog('index_build: background=' + background);

    if (sharded()) {
        enableSharding(_db);
        shardCollection( _db, coll);
    }

    insertDocs(docSize, numDocs, coll);
    quiesceSystem();
    createIndexAndReportThroughput(coll, background);
    cleanup(_db);
}

// Driver
run_test(testDB, coll, build_in_background);
