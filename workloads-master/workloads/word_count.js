/**
 *
 * @file
 * A simple test of basic Map/Reduce fuctionality.
 *
 * ### *Test*
 * Execute a {@link https://docs.mongodb.com/manual/core/map-reduce/|Map/Reduce} task to sum the
 * incidence of all words in a 10 word sentence from a set of documents.
 *
 * Results are reported as docs processed / sec.
 *
 * ### *Setup*
 *   All variants (standalone, replica, sharded)
 *
 * ### *Notes*
 *   - Each document contains a 10 word 'sentence'.
 *   - A word is generated from a random number between 0 and 999, but it should group the incidence
 *     of any given number towrds the center of the range. If the WORDS array is defined, then the
 *     word at this index is used, otherwise the index value is used.
 *   - Up to 1000 documents will be outputted (as the words are between 0 and 999).
 *   - We make sure to emphasize js perf by running the job in jsmode (except for sharded see
 *     {@link https://jira.mongodb.org/browse/SERVER-5448|SERVER-5448}).
 *   - As a result of {@link https://jira.mongodb.org/browse/SERVER-5448|SERVER-5448},
 *     {@link https://docs.mongodb.com/manual/reference/method/db.collection.mapReduce/|jsMode}
 *     only works for non-sharded workloads.
 * 
 * ### *Owning-team*
 * mongodb/product-query
 * 
 * ### *Keywords*
 * 
 * @module workloads/word_count
 */

/* global db sharded Random enableSharding shardCollection quiesceSystem */
/* global benchRun benchStart benchFinish sh print printjson assert  */
/* global reportThroughput sleep server jsTest version emit */
/* global createJob runJobsInPool WORDS */

/**
 * The thread pool size to use generating the documents. The default is 32.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 *
 */
var poolSize = poolSize || 32;

/**
 * The number of insertion jobs to schedule. The default is 100.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 *
 */
var numJobs = numJobs || 100;

/**
 * The number of batches. The default is 150.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 *
 */
var batches = batches || 150;

/**
 * The
 * {@link https://docs.mongodb.com/manual/reference/method/Bulk/#unordered-operations|unorderedBulkOp}
 * batch size to use when generating the documents. The default is 1000.
 *
 * The actual values in use are injected by run_workloads.py, which gets it from config file,
 * see {@link https://github.com/10gen/dsi/blob/138bbc5a39ca779e5b49d8d9242515329ba9d978/configurations/test_control/test_control.core.yml#L29-L31|this hello world example}.
 *
 */
var batchSize = batchSize || 1000;

/**
 * a constant (10) containing the number of words per sentence.
 *
 */
var wordsPerSentence = 10;

load('./utils/words_list.js');

/**
 * Create a range of documents for the word count test.
 *
 * @param {string} db_name - The database name.
 * @param {integer} batches - the number of batches to insert.
 * @param {integer} batchSize - the number of documents per batch. Note: if this value is greater than 1000,
 * then the bulk operator will transparently create batches of 1000.
 * @param {integer} wordsPerSentence - the number of words in a sentence.
 *
 * @returns {object} a json document with the following fields:
 *    ok: if 1 then all the insert batches were successful and nInserted is the expected value
 *    nInserted: the number of documents inserted
 *    results: if ok is 1 then this field is an empty array, otherwise it contains all the batch results
 */
function staging_data(db_name, batches, batchSize, wordsPerSentence, words) {
    Random.srand(341215145 + this.threadId);

    var mr_db = db.getSiblingDB(db_name);
    var nInserted =0;
    var results = [];
    var limit = 333;
    if (words && words.length) {
        limit = words.length / 3;
    }
    for(var i = 0; i < batches; i++) {
        var bulk = mr_db.word_count.initializeUnorderedBulkOp();

        // of batchSize docs each
        for(var j = 0; j < batchSize; j++){

            // accumulate wordsPerSentence words in each doc
            var str = [];
            for (var k = 0; k < wordsPerSentence; k++) {

                // each word is 3 random numbers [0,333] summed, to weight the
                // center of the distribution
                var val = 0;
                for (var l = 0; l < 3; l++) {
                    val += Random.randInt(limit);
                }

                str[k] = val;
                if (words && words.length) {
                    str[k] = words[str[k]];
                }
            }

            bulk.insert({t: str.join(" ")});
        }
        var r = bulk.execute( {w: 1});
        nInserted += r.nInserted;
    }
    var ok = results.every(function(r){return r.nInserted === batchSize;}) ? 1 : 0;
    if (ok === 1 && nInserted !== (batches * batchSize)) {
        ok = 0;
    }
    if (ok === 1) {
        results = [];
    }
    return {ok:ok , nInserted:nInserted, results: results};
}

/**
 * Create an array of jobs to insert the documents for the word count test.
 *
 * @param {function} staging_data - the staging data function
 * @param {integer} numJobs - the number of jobs to create
 * @param {string} db_name - the mr database name
 * @param {string} batches - the number of batches (batches) to invoke.
 * @param {string} batchSize - the size of a batch.
 * @param {string} wordsPerSentence - the number of words per sentence.
 * @param {array} words - an array of word to select from. If empty then the inde will be used.
 *
 * @returns {array}  returns an array of jobs that can be pased to {@link runJobsInPool|runJobsInPool}. A single
 * job is an array containing the function to call as the first element of the array and the remaining elements of
 * the array are the parameters to the function.
 */
var createJobs = function(staging_data, numJobs, db_name, batches, batchSize, wordsPerSentence, words){
    return Array(numJobs).fill().map(function(v,jobId){
        return [staging_data, db_name, batches, batchSize, wordsPerSentence, words];
    });
};

// Helper to construct the out stage for each outPolicy.
function getLastPipelineStage(outPolicy, db, coll) {
    switch (outPolicy) {
    case "reduce":
        return {"$merge":{"into": {db: db, coll: coll}, "on":"_id","let":{"new":"$$ROOT"},"whenMatched":[{"$project":{"value":{"$function":{"args":["$_id",["$value","$$new.value"]],"body":"function(k, v) {\n        return Array.sum(v);\n    }","lang":"js"}}}}],"whenNotMatched":"insert"}};
    default:
        throw new Error("invalid ouput policy: " + outPolicy);
    }
}

var majorVersion = db.version().split('.')[0];
var minorVersion = db.version().split('.')[1];

function runWCMapReduce(db, outDoc, testName, outPolicy, totalDocs) {
    // Flags for reporting results
    var errMsg = "";
    var pass = true;

    // Run the legacy mapReduce command for db versions < 4.5, otherwise compute the workload
    // using an agg pipeline.
    if ((majorVersion == 4 && minorVersion < 5) || majorVersion < 4) {
      var fmap = function() {
          var text = this.t;

          var tokens = text.split(" ");

          for (var i = 0; i < tokens.length; i++) {
              emit(tokens[i], 1);
          }
      };

      var freduce = function(k, v) {
          return Array.sum(v);
      };

      var query = {out: {reduce: "totals", db: "word_count_results"}};
      // SERVER-5448
      if (!sharded()) {
          query.jsMode = true;
      }
      // Create the target database manually, since mapReduce is no longer allowed to implicitly
      // create the target db.
      db.getSiblingDB("word_count_results").createCollection("totals");

      s = Date.now();
      var re = db.word_count.mapReduce(fmap, freduce, query);
      e = Date.now();
      print("Done word count job! Total time taken: " + (e - s) + "ms");

      // set errMsg and failed flag in case of error
      if (re.ok != 1) {
          pass = false;
          errMsg = "MapReduce Job return ok != 1";
      }

      var throughput = totalDocs * 1000 / (e - s);

      reportThroughput("word_count_doc", throughput,
                       {nThread: 1, pass: pass, errMsg: errMsg});
    } else {
      // Pipelined version
      var pipelineDBName = outDoc["db"];
      var outStage = getLastPipelineStage(outPolicy, pipelineDBName, "totals");
      var s = Date.now();
      try {
        db.word_count.aggregate([{$project: {emits: {$map: {input: {$split: ["$t", " "]}, as: "word", in: {k: "$$word" , v: 1}}}}}, {$unwind: {"path": "$emits"}}, {$group: {_id: "$emits.k", value: {$sum: "$emits.v"}}}, outStage]);
      } catch(e) {
        // Set errMsg and failed flag in case of error
        pass = false;
        errMsg = "Agg word count Job return ok != 1";
      }
      var e = Date.now();
      print("Done word count agg pipeline! Total time taken: " + (e - s) + "ms");

      throughput = totalDocs * 1000 / (e - s);

      reportThroughput(testName,  throughput,
                       {nThread: 1, pass: pass, errMsg: errMsg});
    }
}

function runWorkload() {
    var outputDBName = "word_count_results_reduce";
    var db_name = "test";
    var d = db.getSiblingDB(db_name);

    // drop database, and re-create index
    d.dropDatabase();
    if( sharded() ) {
        enableSharding(d);
        shardCollection(d, d.word_count);

        // Create the target database manually, as `$merge` does not create the output database in
        // sharded mode.
        db.getSiblingDB(outputDBName).createCollection("totals");
    }

    // staging data
    var s = Date.now();
    var totalDocs = numJobs * batches * batchSize;
    print("inserting " + localize(totalDocs) + " docs in " + poolSize + " threads");
    print("Start staging data: " + s);
    var jobs = createJobs(staging_data, numJobs, db_name, batches, batchSize, wordsPerSentence, WORDS);
    var results = runJobsInPool(poolSize, jobs);
    quiesceSystem();
    var e = Date.now();
    print("Done staging data: " + e + ". Total time taken: " + (e - s) + "ms");

    // run test here
    var outDocs = [
	    {
	      outPolicy: "reduce",
	      out: {reduce: "totals", db: outputDBName},
	      testName: "agg_word_count_results_reduce",
	    },
    ];
    outDocs.forEach(function(outOption, index) {
        runWCMapReduce(d, outOption["out"], outOption["testName"], outOption["outPolicy"], totalDocs);
    });
}

runWorkload();
