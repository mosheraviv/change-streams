/**
 * @file
 * This file tests the time it takes for mongos to route reads with a varying sized catalog.
 *
 * ### *Test*
 *
 * Compare the latency of a fixed workload for various catalog sizes.
 *
 * The workload consists of creating a varying number of sharded collections. The insertion of the
 * collection and chunk entries happens directly into config.collections for performance reasons.
 * ({@link https://docs.mongodb.com/manual/reference/method/db.collection.findOne/|findOne}).
 *
 * The scenarios tested are:
 * 1. **fresh mongos**: read through a mongos that has not yet seen any data.
 * 1. **steady mongos**: read from a collection through a mongos that has already seen this
 * collection.
 * 1. **new collection**: read from a collection through a mongos that has not seen this collection,
 * but that has already seen all other collections in this db.
 *
 * Results are reported as the time in ms it takes to read in each scenario.
 *
 * ### *Setup*
 *   Requires a sharded cluster with two mongoses.
 *
 * ### *Notes*
 * * This test uses a different database each iteration.
 * * This test inserts a varying number of entries into config.collections and config.chunks at the
 * start of each iteration.
 * * This test uses a different database each iteration. The overall amount of data on the config
 * should not affect the time to read, only the amount for a particular database.
 *
 * ### *Owning-team*
 * mongodb/sharding
 *
 * ### *Keywords*
 *
 * @module workloads/mongos_large_catalog_cache
 */
/*global
  db sharded Random enableSharding shardCollection quiesceSystem
  benchRun benchStart benchFinish sh print printjson assert tojson
  reportThroughput sleep server jsTest version findPrimaryShard ScopedThread
*/

/**
 * The number of collections to "create" (insert directly into the catalog) before doing reads.
 */
var numCollectionsToCreate = numCollectionsToCreate || [0, 1000, 10000, 100000, 1000000];

/**
 * The mongos hosts assigned IP addresses. The expected format is a list of dictionaries containing
 * a "private_ip" field.
 * This parameter is used to connect through multiple mongoses in a sharded cluster.
 * If left unassigned, the workload and listeners will connect to the default mongos.
 * This test only runs on sharded clusters.
 */
var mongos_hosts = mongos_hosts || ["localhost:27017"];

/**
 * The major version of the server required to infer the FCV in use.
 */
 var majorVersion = db.version().split(".")[0];

/**
 * Create an entry to be inserted in both config.collections and config.chunks for a given
 * collection with one chunk. This mimics the catalog entries for a sharded collection.
 */
var createCollectionAndChunkEntries = function(bulkCollections, bulkChunks, dbName, shard_ids, i, chunksByNS) {
    var collName = "coll_" + i;
    var lastmodEpoch = ObjectId();
    var date = new Date();
    var uuid = UUID();

    var collectionEntry = {
        "_id": dbName + "." + collName,
        "lastmodEpoch": lastmodEpoch,
        "lastmod": date,
        "dropped": false,
        "key": {"_id": 1},
        "unique": false,
        "uuid": uuid
    };

    bulkCollections.insert(collectionEntry);

    var chunkEntry = chunksByNS ? {
        "_id": dbName + "." + collName + "-_id_MinKey",
        "ns": dbName + "." + collName,
        "min": {"_id": MinKey},
        "max": {"_id": MaxKey},
        "shard": shard_ids[0],
        "lastmod": date,
        "lastmodEpoch": lastmodEpoch,
        "history": [{"validAfter": Timestamp(0, 0), "shard": shard_ids[0]}]
    } : {
        "_id": dbName + "." + collName + "-_id_MinKey",
        "uuid": uuid,
        "min": {"_id": MinKey},
        "max": {"_id": MaxKey},
        "shard": shard_ids[0],
        "lastmod": date,
        "lastmodEpoch": lastmodEpoch,
        "history": [{"validAfter": Timestamp(0, 0), "shard": shard_ids[0]}]
    };

    bulkChunks.insert(chunkEntry);
};

/**
 * Insert nCollections entires directly in config.collections and config.chunks. This mimics the
 * entries created during shardCollection without actually calling it nCollections times, which
 * would take too long when nCollections is very large. Do this over nThreads to speed up the time
 * it takes to insert.
 */
var createCollections = function(mongos, nCollections, dbName, username, password) {
    var nThreads = 32;
    var nCollectionsPerThread = Math.floor(nCollections / nThreads);
    var numThreads = Array(nThreads).fill().map(function(_, i) { return i; });
    if (authEnabled){
        var mongosAuth = 'mongodb://'.concat(username, ':', password, '@', mongos);
    } else {
        mongosAuth = mongos;
    }

    // Insert nCollections using nThreads. If nCollections cannot be evenly divided
    // by nThreads, we insert the leftover collections on the last thread below.
    var threads = numThreads.map(function(threadNum) {
        function createCollectionsPerThread(mongos,
                                            nCollections,
                                            threadNum,
                                            dbName,
                                            nCollectionsPerThread,
                                            nThreads,
                                            chunksByNS,
                                            createCollectionAndChunkEntries,
                                            username,
                                            password,
                                            mongosAuth) {
            var config = new Mongo(mongosAuth).getDB("config");
            var shards = config.shards.find().toArray();
            var shard_ids = [];
            shards.forEach(function(shard) {
                shard_ids.push(shard._id);
            });

            var bulkCollections = config.collections.initializeUnorderedBulkOp();
            var bulkChunks = config.chunks.initializeUnorderedBulkOp();

            var min = threadNum * nCollectionsPerThread;
            var max = nCollections === 0 ? 0 : ((threadNum + 1) * nCollectionsPerThread);
            for (var i = min; i < max; i++) {
                createCollectionAndChunkEntries(bulkCollections, bulkChunks, dbName, shard_ids, i, chunksByNS);
            }

            // Insert the remaining collections on the last threads.
            if (threadNum === (nThreads - 1)) {
                for (var i = max; i < nCollections; i++) {
                    createCollectionAndChunkEntries(
                        bulkCollections, bulkChunks, dbName, shard_ids, i, chunksByNS);
                }
            }

            assert.writeOK(bulkCollections.execute());
            assert.writeOK(bulkChunks.execute());

            return;
        }

        var thread = new ScopedThread(createCollectionsPerThread,
                                      mongos,
                                      nCollections,
                                      threadNum,
                                      dbName,
                                      nCollectionsPerThread,
                                      nThreads,
                                      majorVersion < 5,
                                      createCollectionAndChunkEntries,
                                      username,
                                      password,
                                      mongosAuth);
        thread.start();
        return thread;
    });

    threads.forEach(function(thread) {
        thread.join();
    });

    assert.eq(nCollections,
              new Mongo(mongosAuth)
                  .getDB("config")
                  .collections.find({"_id": {"$regex": dbName + ".coll_.*"}})
                  .count());
};

/**
 * Read from a collection on a given shard so that the shard has the collection entry in its
 * cache. This way, when we read from mongos the shard will not have to refresh as well.
 */
var readOnShards = function(shards, coll_name, dbName) {
    shards.forEach(function(shard) {
        if (authEnabled){
            // Slice off the leading rs*/ from the list of members in the replica set
            shardHostParse = shard.host.slice(4);
            shardHostAuth = 'mongodb://'.concat(username, ':', password, '@', shardHostParse);
        } else {
            shardHostAuth = shard.host;
        }
        var shardConn = new Mongo(shardHostAuth);
        shardConn.getDB(dbName).getCollection(coll_name).findOne();
    });
};

var runTest = function() {
    if (!sharded()) {
        print("Workload 'mongos_read_single' must be run in a sharded environment.");
        return;
    }

    var mongos_targets = mongos_hosts.map(function(x) { return x.private_ip; });

    // Set up mongos connections
    assert.gte(mongos_targets.length, 2);
    var writingMongos = mongos_targets[0];
    var readingMongos = mongos_targets[1];
    var usernameFunc = username;
    var passwordFunc = password;

    if (authEnabled){
        // Slice off the leading rs*/ from the list of members in the replica set
        var writingMongosAuth = 'mongodb://'.concat(username, ':', password, '@', writingMongos);
        var readingMongosAuth = 'mongodb://'.concat(username, ':', password, '@', readingMongos);
    } else {
        var writingMongosAuth = writingMongos;
        var readingMongosAuth = readingMongos;
    }

    var writingMongosConn = new Mongo(writingMongosAuth);
    var readingMongosConn = new Mongo(readingMongosAuth);

    var config = writingMongosConn.getDB("config");
    var shards = config.shards.find().toArray();

    // Get latencies for reads through mongos for varying catalog sizes. Repeat 5 times for each
    // catalog size to get the average latency.
    numCollectionsToCreate.forEach(function(nCollections) {
        print("Running with collections: " + nCollections);

        var diff1_values = [];
        var diff2_values = [];
        var diff3_values = [];
        var diff4_values = [];
        for (var iteration = 0; iteration < 5; iteration++) {
            var largeDbName = "huge_sharding_catalog_db_" + nCollections + "_iter_" + iteration;
            var largeDb = writingMongosConn.getDB(largeDbName);
            var test_ns = largeDbName + '.test';
            var test_new_ns = largeDbName + '.test_new';

            // Force mongos to clear their cache.
            writingMongosConn.getDB("admin").runCommand({flushRouterConfig: 1});
            readingMongosConn.getDB("admin").runCommand({flushRouterConfig: 1});

            // Shard and insert into "test" collection.
            assert.commandWorked(writingMongosConn.adminCommand({enableSharding: largeDbName}));
            assert.commandWorked(
                writingMongosConn.adminCommand({shardCollection: test_ns, key: {"_id": 1}}));
            assert.commandWorked(writingMongosConn.getDB(largeDbName).test.insert({"x": 1}));

            // Insert nCollections into the catalog.
            createCollections(writingMongos, nCollections, largeDbName, usernameFunc, passwordFunc);

            // Make sure shard is aware of "test" collection that we will read from mongos later.
            readOnShards(shards, "test", largeDbName);

            quiesceSystem();

            // Time how long it takes for a fresh mongos to read in all the collections in the
            // catalog when doing a find on a collection it has never seen before.
            var t0 = new Date().getTime();
            var fresh_mongos = readingMongosConn.getDB(largeDbName).test.findOne({"x": 1});
            var t1 = new Date().getTime();
            diff1_values.push(t1 - t0);

            // Run the same find as above to time how long it takes for mongos to read from a
            // collection it has seen before - it should not have to do a refresh because it should
            // have already rereshed its cache.
            t0 = new Date().getTime();
            var steady_mongos = readingMongosConn.getDB(largeDbName).test.findOne({"x": 1});
            t1 = new Date().getTime();
            diff2_values.push(t1 - t0);

            // Shard and insert into a new collection "test_new" in the same database.
            assert.commandWorked(
                writingMongosConn.adminCommand({shardCollection: test_new_ns, key: {"_id": 1}}));
            assert.commandWorked(writingMongosConn.getDB(largeDbName).test_new.insert({"x": 1}));

            // Make sure shard is aware of "test_new" collection that we will read from mongos
            // later.
            readOnShards(shards, "test_new", largeDbName);

            quiesceSystem();

            // Time how long it takes for a mongos to read a collection it has not seen before, that
            // is in a database that this mongos has already seen. It should only have to load the
            // one collection, because it already loaded all of the others.
            t0 = new Date().getTime();
            var new_coll_mongos = readingMongosConn.getDB(largeDbName).test_new.findOne({"x": 1});
            t1 = new Date().getTime();
            diff3_values.push(t1 - t0);

            // Run the same find as above. This should have the same latency as diff2, because in
            // both cases we are reading from a collection we've already seen and have already
            // cached catalog information.
            t0 = new Date().getTime();
            var new_coll_steady_mongos =
                readingMongosConn.getDB(largeDbName).test_new.findOne({"x": 1});
            t1 = new Date().getTime();
            diff4_values.push(t1 - t0);

            // Negate the diff values because currently sys-perf sees higher values as better. In
            // this case, we are reporting latencies, so lower is better when not negated.
            reportThroughput("mongos_large_catalog_" + nCollections + "_colls_fresh_mongos",
                             -diff1_values[iteration],
                             {nThread: 1});
            reportThroughput("mongos_large_catalog_" + nCollections + "_colls_steady_mongos",
                             -diff2_values[iteration],
                             {nThread: 1});
            reportThroughput(
                "mongos_large_catalog_" + nCollections + "_colls_new_coll_same_db_mongos",
                -diff3_values[iteration],
                {nThread: 1});
            reportThroughput(
                "mongos_large_catalog_" + nCollections + "_colls_new_coll_same_db__steady_mongos",
                -diff4_values[iteration],
                {nThread: 1});
        }

        // Print (max - min) / median for the measured latencies for each of the different reads.
        var percent_range1 = (Math.max.apply(Math, diff1_values) - Math.min.apply(Math, diff1_values)) /
            (diff1_values.sort())[Math.floor(diff1_values.length / 2)] * 100;
        var percent_range2 = (Math.max.apply(Math, diff2_values) - Math.min.apply(Math, diff2_values)) /
            (diff2_values.sort())[Math.floor(diff2_values.length / 2)] * 100;
        var percent_range3 = (Math.max.apply(Math, diff3_values) - Math.min.apply(Math, diff3_values)) /
            (diff3_values.sort())[Math.floor(diff3_values.length / 2)] * 100;
        var percent_range4 = (Math.max.apply(Math, diff4_values) - Math.min.apply(Math, diff4_values)) /
            (diff4_values.sort())[Math.floor(diff4_values.length / 2)] * 100;
        print("range for fresh mongos with " + nCollections + " collections: " + percent_range1);
        print("range for steady mongos with " + nCollections + " collections: " + percent_range2);
        print("range for new collection same database with " + nCollections + " collections: " +
              percent_range3);
        print("range for new collection same database steady with " + nCollections +
              " collections: " + percent_range4);
    });
};

runTest();
