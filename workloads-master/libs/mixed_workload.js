/**
 * Provides a CRUDWorkload class that can execute an even mix of insert, read, update and deletes,
 * each in their own independent thread.
 * The workload is executed through benchRun and returns throughput information.
 *
 * ### *Notes*
 * - {@link https://jira.mongodb.org/browse/SERVER-29764|SERVER-29764} 'benchRun is limited to
 *   128 threads'
 * - To work around {@link https://jira.mongodb.org/browse/SERVER-29764|SERVER-29764}, 4 javascript
 *   scoped threads are launched to run 4 instances of benchRun. However, it turns out this was
 *   also a key property of this test without which
 *   {@link https://jira.mongodb.org/browse/BF-2385|BF-2385} wouldn't have happened: It is only
 *   because each type of CRUD operation is running in separate threads, that it is possible for
 *   the operations to become unbalanced.
 */

/*global
 db sharded Random shardCollection enableSharding quiesceSystem benchStart benchFinish sh print
 tojson sleep server toNumber
*/


/**
 * Creates a MixedCRUDWorkload that can execute workload on the multiple collections
 * and in separate threads.
 *
 * @constructor
 * @param {String} dbName - The database name.
 * @param {Array} collectionNames - The collection names.
 * @param {Number} nThreads - The number of threads to run. Must be a multiple of
 *        4 x collectionNames.length [x mongosHosts.length].
 * @param {Array} mongosHosts - The mongoses hosts if the workload should connect
 *        through multiple mongoses.
 * @param {Boolean} retryableWrites - Whether to enable retryable writes. Defaults to false.
 * @param {Number} docSize - The size in bytes of the documents in the workload. Defaults to 100.
 * @param {Number} numberDoc - The number of documents to insert into the collection(s) during
 * initialization. Defaults to 100,000.
 * @param {Boolean} shardCollections - Indicates if, when the target cluster is sharded, the
 * collections should be sharded. Defaults to true.
 * @param {Boolean} writeLoadOnly - Indicates if only write load should be generated. Defaults to
 *        false.
 */
var MixedCRUDWorkload = function(dbName, collectionNames, nThreads, retryableWrites, mongosHosts,
                                 docSize, numberDoc, shardCollections, writeLoadOnly = false) {
    var _docSize = docSize || 100;  // Document size + _id field.
    var _numberDoc = numberDoc || 100000;
    var _batchSize = 100;  // Only used for the collection initialization, not for the workload.
    var _shardCollections = shardCollections !== false;

    var _db = db.getSiblingDB(dbName);
    var _collections = [];
    var _benchRunTasks = [];
    var _targetHosts;
    var _writeLoadOnly = writeLoadOnly;
    if (mongosHosts && mongosHosts.length !== 0) {
        _targetHosts = mongosHosts;
    } else {
        _targetHosts = [server];
    }
    const numberOfWorkers = writeLoadOnly ? 3 : 4;
    assert.eq(0, nThreads % (numberOfWorkers * collectionNames.length * _targetHosts.length),
              "nThreads must be a multiple of " + numberOfWorkers +
              " * <nb collections>(" + collectionNames.length + ")" +
              " * <nb target hosts>(" + _targetHosts.length + ")");
    Random.setRandomSeed(0);

    // Run this once in the beginning
    if( sharded() ) {
        enableSharding(_db);
    }

    // Make a document of slightly more than the given size.
    function makeDocument(_docSize, minWriteId) {
        for (var i = 0; i < _docSize; i++) {
            var doc = { id: minWriteId + Random.randInt(_numberDoc - minWriteId),
                        a: Random.randInt(1000000),
                        c: "" };
            while(Object.bsonsize(doc) < _docSize) {
                doc.c += "x";
            }
            return doc;
        }
    }

    // Run this in the beginning of each iteration of threads
    function initColl(coll) {
        coll.drop();
        // Index our id (note, not _id, not unique) and shard key
        coll.createIndex({
            id: 1
        });

        // Index a non-id key.
        coll.createIndex({
            a: 1
        });

        if(sharded() && _shardCollections) {
            // IMPORTANT: Use "id" for shard key, not _id
            shardCollection(_db, coll, { shardKey: "id", chunks: 96});
        }

        // Insert the initial set of _numberDoc documents. Not measured for performance.
        for (i = 0; i < _numberDoc/_batchSize; i++) {
            var bulk = coll.initializeUnorderedBulkOp();
            for ( j = 0; j < _batchSize && i*_batchSize+j < _numberDoc; j++ ) {
                bulk.insert(makeDocument(_docSize, 0));
            }
            bulk.execute();
        }
    }

    /**
     * Initializes the collection(s) that are needed for the workload.
     * Must be called before start().
     */
    this.initialize = function () {
        var name;
        var coll;
        for (var i = 0; i < collectionNames.length; i++) {
            name = collectionNames[i];
            print("Initializing collection '" + name + "'");
            coll = _db.getCollection(name);
            _collections.push(coll);
            initColl(coll);
        }
        quiesceSystem();
        sleep(500);
        if (sharded()) {
            sh.status();  // check shard status before test run
        }
    };

    /**
     * Starts a workload for one collection on one host.
     *
     * @param {String} ns - The namespace (i.e. "<db>.<collection>").
     * @param {Number} nThreads - The number of threads to run. Must be a multiple of 4.
     * @param {String} targetHost - The host to connect to.
     */
    function startLoad(ns, nThreads, targetHost) {
        print("Starting a " + nThreads + " threads load on " + targetHost);
        var workers = [{ op: "insert",
                         writeCmd: true,
                         ns: ns,
                         doc:  { id: { "#RAND_INT" : [ 0, _numberDoc ]},
                                 a : { "#RAND_INT" : [ 0, 1000000 ]},
                                 c : { "#RAND_STRING": [ { "#RAND_INT": [ 1, _docSize ]}]} } },
                       { op: "remove",
                         writeCmd: true,
                         ns: ns,
                         query: { id: { "#RAND_INT" : [ 0, _numberDoc ]}}},
                       { op: "update",
                         writeCmd: true,
                         ns: ns,
                         query: { id: { "#RAND_INT" : [ 0, _numberDoc ]}},
                         update: {
							 $inc: { a: 1 },
							 $set: { c: { "#RAND_STRING": [ { "#RAND_INT": [ 1, _docSize ]}]}}}}];
        if (!_writeLoadOnly) {
            workers.push({op: "findOne",
                          readCmd: true,
                          ns: ns,
                          query: {id: { "#RAND_INT" : [0, _numberDoc ]}}});
        }

        workers.forEach(function(op) {
            var benchArgs = { "ops": [op],
                              "parallel": Math.floor(nThreads/workers.length),
                              "host": targetHost,
                              "username": username,
                              "password": password };

            if (retryableWrites) {
                benchArgs.useSessions = true;
                benchArgs.useIdempotentWrites = true;
            }

            _benchRunTasks.push(benchStart(benchArgs));
        });
    }

    /**
     * Starts the workload.
     */
    this.start = function() {
        var coll = null;
        var nThreadsPerCollection = nThreads / _collections.length;
        var nThreadsPerLoad = nThreadsPerCollection / _targetHosts.length;
        for (var i = 0; i < _collections.length; i++) {
            coll = _collections[i];
            for (var j = 0; j < _targetHosts.length; j++) {
                startLoad(coll.getFullName(), nThreadsPerLoad, _targetHosts[j]);
            }
        }
    };

    /**
     * Stops the workload and returns a throughput object containing the throughput rates
     * by operation (findOne, query, insert, update, delete) as well as the total.
     */
    this.stop = function() {
        var throughput = { findOne: 0, query:0, insert:0, update:0, delete:0, total:0};
        _benchRunTasks.forEach(function(benchRunTask) {
            var benchResults = benchFinish(benchRunTask);
            print("BenchRun Results for this instance:  " + tojson(benchResults));
            throughput.findOne += toNumber(benchResults.findOne);
            throughput.query   += toNumber(benchResults.query);
            throughput.insert  += toNumber(benchResults.insert);
            throughput.update  += toNumber(benchResults.update);
            throughput.delete  += toNumber(benchResults.delete);
        });
        throughput.total = throughput.findOne + throughput.query + throughput.insert +
                           throughput.update  + throughput.delete;
        return throughput;
    };
};
