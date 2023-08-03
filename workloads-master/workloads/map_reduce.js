/**
 * @file
 * A simple test of basic Map/Reduce fuctionality
 * <br>
 *
 * ### *Test*
 *
 * Execute an aggregation pipeline to sum all of the amounts grouped by uid. ~numJobs~ (default 200)
 * output documents are generated.
 * <br>
 *   The number of input documents equals the product of:
 *
 *       numJobs * batches * batchSize * statusRange
 *
 *   The default case is:
 *
 *       200 * 40 * 1000 * 5 = 40,000,000
 *
 * Results are reported as docs processed per second.
 *
 * ### *Setup*
 *
 *   All variants (standalone, replica, sharded)
 *
 * ### *Notes*
 *
 *   - This test stage will evenly distribute documents over 200 UID,
 *     the agg pipeline will calculate sum of amount based on uid.
 * 
 * ### *Owning-team*
 * mongodb/product-query
 * 
 * ### *Keywords*
 * 
 * @module workloads/map_reduce
 */

/* global db sharded Random enableSharding shardCollection quiesceSystem */
/* global benchRun benchStart benchFinish sh print printjson assert  */
/* global reportThroughput sleep server jsTest version emit */
/* global createJob runJobsInPool */

/**
 * the destination database name.
 *
 */
var db_name = "test";

/**
 * The thread pool size to use generating the documents. The default is 32.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var poolSize = poolSize || 32;

/**
 * The range of uids to generate. The default 200.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var numJobs = numJobs || 200;

/**
 * The number of batches. The default is 40.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var batches = batches || 40;

/**
 * The range of status values to use when generating documents. It default to 5.
 * So values 0 through 4 are generated in this case.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var statusRange = statusRange || 5;

/**
 * The {@link https://docs.mongodb.com/manual/reference/method/Bulk/#unordered-operations|unorderedBulkOp} batch size to use when generating the documents.
 * The default is 1000.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 */
var batchSize = batchSize || 1000;

/**
 * Create a range of documents for the map / reduce test.
 *
 * @param {string} db_name - The database name.
 * @param {integer} batches - the number of batches to insert.
 * @param {integer} batchSize - the number of documents per batch. Note: if this value is greater than 1000,
 * then the bulk operator will transparently create batches of 1000.
 * @param {integer} uid - the value of the uid field to insert.
 * @param {integer} statusRange - the range of status values (zero based).
 *
 * @returns {object} a json document with the following fields:
 *    ok: if 1 then all the insert batches were successful and nInserted is the expected value
 *    nInserted: the number of documents inserted
 *    results: if ok is 1 then this field is an empty array, otherwise it contains all the batch results
 */
var staging_data = function(db_name, batches, batchSize, uid, statusRange) {

    Random.srand(341215145 + this.threadId);

    var d = db.getSiblingDB(db_name);
    var nInserted =0;
    var results = [];
    var count =0;
    for(var i=0; i < batches; i++) {
        for(var j = 0; j < statusRange; j ++) {
            var bulk = d.mr.initializeUnorderedBulkOp();
            for(var k = 0; k < batchSize; k++){
                // status would also equal the following but a count is simpler
                // ((i * statusRange * batchSize) + (j * batchSize) + k + 1) % statusRange;
                bulk.insert({
                    uid: uid,
                    amount: Random.randInt(1000000),
                    status: count++ % statusRange});
            }
            var r = bulk.execute( {w: 1});
            nInserted += r.nInserted;
            results.push(r);
        }
    }
    var ok = results.every(function(r){return r.nInserted === batchSize;}) ? 1 : 0;
    if (ok === 1 && nInserted !== (batches * batchSize * statusRange)) {
        ok = 0;
    }
    if ( ok === 1){
        results = [];
    }
    return {ok:ok, nInserted:nInserted, results: results };
};

var totalDocs = numJobs * batches * batchSize * statusRange;
print("inserting " + localize(totalDocs) + " docs in " + poolSize + " threads");

/**
 * Create an array of jobs to insert the documents for the map reduce test. In this instance, the
 * job paramter are fixed except for the uid. Each job generates a set of documents for a given uid
 * in the desired range (0 to numJobs -1).
 *
 * @param {integer} numJobs - the range of uids to generate
 * @param {function} func - the staging data function
 * @param {string} db_name - the mr database name
 * @param {string} batches - the number of batches to invoke.
 * @param {string} batchSize - the size of a batch.
 * @param {string} statusRange - the range of status values (0 to statusRange -1).
 *
 * @returns {array}  returns an array of jobs that can be pased to {@link runJobsInPool|runJobsInPool}. A single
 * job is an array containing the function to call as the first element of the array and the remaining elements of
 * the array are the parameters to the function.
 */
var createJobs = function(staging_data, db_name, batches, batchSize, numJobs, statusRange){
    return Array(numJobs).fill().map(function(v,uid){
        return createJob(staging_data, db_name, batches, batchSize, uid, statusRange);
    });
};

var jobs = createJobs(staging_data, db_name, batches, batchSize, numJobs, statusRange);

// drop database, and re-create index
var d = db.getSiblingDB(db_name);
d.dropDatabase();
d.mr.createIndex({uid: 1});

print("Server is : " + server);

if( sharded() ) {
    enableSharding(d);
    shardCollection(d, d.mr);
}

// staging data
var s = Date.now();
print("Start staging data: " + s + " of " + numJobs + " distinct uids");
var results = runJobsInPool(poolSize, jobs);
quiesceSystem();
var e = Date.now();
print("Done staging data: " + e + ". Total time taken: " + (e - s) + "ms");

var majorVersion = db.version().split('.')[0];
var minorVersion = db.version().split('.')[1];

// Helper to construct the test name. Note, we are using different test names here because
// on versions less than 4.6+ we are falling back to the legacy MR command since, many users
// on those versions still rely on the map reduce command.
function getTestName(outPolicy, majorVersion, minorVersion) {
  var isLegacyMRVersion = (majorVersion == 4 && minorVersion < 5) || majorVersion < 4;
  switch (outPolicy) {
    case "inline":
      return isLegacyMRVersion ? "map_reduce_doc_inline" : "agg_doc_inline";
    case "reduce":
      return isLegacyMRVersion ? "map_reduce_doc_reduce" : "agg_doc_reduce";
    case "replace":
      return isLegacyMRVersion ? "map_reduce_doc_replace" : "agg_doc_replace";
    case "merge":
      return isLegacyMRVersion ? "map_reduce_doc_merge" : "agg_doc_merge";
    default:
      throw new Error("invalid ouput policy: " + outPolicy);
  }
}

// Set up test parameters
var outOptions = [
    {
      outPolicy: "inline",
      out: {inline: 1},
      testName: getTestName("inline", majorVersion, minorVersion),
    },
    {
      outPolicy: "reduce",
      out: {reduce: "totals", db: "mr_results"},
      testName: getTestName("reduce", majorVersion, minorVersion),
    },
    {
      outPolicy: "replace",
      out: {replace: "totals", db: "mr_results"},
      testName: getTestName("replace", majorVersion, minorVersion),
    },
    {
      outPolicy: "merge",
      out: {merge: "totals", db: "mr_results"},
      testName: getTestName("merge", majorVersion, minorVersion),
    }
];

// Helper to construct the out stage for each outPolicy.
function getLastPipelineStage(outPolicy, db, coll) {
    switch (outPolicy) {
      case "inline":
        return null;
      case "reduce":
        return {"$merge":{"into": {db: db, coll: coll}, "on":"_id","let":{"new":"$$ROOT"},"whenMatched":[{"$project":{"value":{"$function":{"args":["$_id",["$value","$$new.value"]],"body":"function(k, v) {\n        return Array.sum(v);\n    }","lang":"js"}}}}],"whenNotMatched":"insert"}};
      case "merge":
        return {"$merge":{"into": {db: db, coll: coll}, "on":"_id","whenMatched":"replace","whenNotMatched":"insert"}};
      case "replace":
        return {"$out": {db: db, coll: coll}};
      default:
        throw new Error("invalid ouput policy: " + outPolicy);
    }
}

// Actual test function
function runAggFold(outDoc, testName, outPolicy) {
    // Flags for reporting results
    var errMsg = "";
    var pass = true;

    // Run the legacy mapReduce command for db versions < 4.5, otherwise compute the workload
    // using an agg pipeline.
    if ((majorVersion == 4 && minorVersion < 5) || majorVersion < 4) {
      // Create the target database manually, since mapReduce is no longer allowed to implicitly create
      // the target db.
      d.getSiblingDB("mr_results").createCollection("totals");

      fmap = function() { emit( this.uid, this.amount ); };
      freduce = function(k, v) { return Array.sum(v) ;};
      query   = { out: outDoc };

      var start = Date.now();
      result = d.mr.mapReduce(fmap, freduce, query);
      var end = Date.now();

      print("Done MR job! Total time taken: " + (end - start) + "ms");

      // report results
      if (result.ok != 1) { errMsg = "MapReduce Job return ok != 1"; }
      throughput = totalDocs * 1000 / (end - start);

      reportThroughput(testName, throughput,
                       {nThread: 1, errMsg: errMsg});

    } else {
      var pipelineDB = d.getSiblingDB("agg_results");
      pipelineDB.totals.drop();
      pipelineDB.createCollection("totals");

      var outStage = getLastPipelineStage(outPolicy, "agg_results", "totals");
      var aggPipeline = [{$project: {uid: 1, amount: 1}}, {$group: {_id: "$uid", value: {$sum: "$amount"}}}]; 
      if (outPolicy != "inline") {
        aggPipeline.concat(outStage);
      }

      var start = Date.now();
      try {
        d.mr.aggregate(aggPipeline); 
      } catch(e) {
        // Set errMsg and failed flag in case of error
        pass = false;
        errMsg = "Agg pipeline Job return ok != 1";
      }
      var end = Date.now();

      print("Done word count agg pipeline! Total time taken: " + (end - start) + "ms");
      throughput = totalDocs * 1000 / (end - start);

      reportThroughput(testName, throughput,
                       {nThread: 1, pass: pass, errMsg: errMsg});
  }
}

outOptions.forEach(function(outOption, index) {
    runAggFold(outOption["out"], outOption["testName"], outOption["outPolicy"]);
});
