/**
 * @file
 * Test performance of chunk migrations
 * <br>
 *
 * ### *Test*
 *
 *   Move all 4 chunks, each containing 25k documents, from one shard to another.
 *
 *   Unless user specifies otherwise, uses default values for
 *   {@link https://docs.mongodb.com/manual/core/sharding-balancer-administration/index.html#chunk-migration-and-replication|_secondaryThrottle}
 *   (false for WiredTiger and true for MMAPv1), and
 *   {@link https://docs.mongodb.com/manual/tutorial/manage-sharded-cluster-balancer/#wait-for-delete|_waitForDelete}
 *   (false).
 *
 *   Uses
 *   {@link https://docs.mongodb.com/manual/core/sharding-balancer-administration/#chunk-migration-procedure|moveChunk}
 *   command
 *
 *   Results are reported as docs moved / sec.
 *
 * ### *Setup*
 *
 *   Requires a sharded cluster.
 *
 *   Test inserts 100k documents, split evenly into 4 chunks, all initially located on rs0.
 *   Field a is indexed, containing uniformly random numbers.
 *   Note that the test maxes out at four moves to avoid a large number of moves if the cluster has many shards.
 *
 * ### *Owning-team*
 * mongodb/sharding
 *
 * ### *Keywords*
 *
 * @module workloads/move_chunk
 */

/* global db sharded sh printjson assert reportThroughput sleep */

var default_val = "default";

/**
 * The value for the _secondaryThrottle parameter to the moveChunk command.
 */
var secondary_throttle = secondary_throttle === undefined ? default_val : secondary_throttle;

/**
 * The value for the _waitForDelete parameter to the moveChunk command.
 */
var wait_for_delete = wait_for_delete === undefined ? default_val : wait_for_delete;

/**
 * The major version of the server required to infer the FCV in use.
 */
 var majorVersion = db.version().split(".")[0];

(function () {

    if (!sharded()) {
        print("move_chunk will only be run with multi-shard setup.\nSkip...");
        return;
    }

    // stop balancer
    sh.stopBalancer();

    var _db = db.getSiblingDB("sbtest");
    var coll = _db.move_chunk;
    var config = _db.getSiblingDB("config");
    var shards = config.shards.find().toArray();
    var numShards = shards.length;
    var shard_ids = [];
    shards.forEach(function (shard) {
        shard_ids.push(shard._id);
    });


    var numberDoc = 100000;
    var numChunks = 4;
    var chunkSize = numberDoc / numChunks;

    var longStr = "";
    for (var i = 0; i < 100; i++) {
        longStr += "a";
    }

    var initColl = function () {
        _db.dropDatabase();

        // Enable sharding for database.
        printjson(_db.adminCommand({
            enableSharding: coll.getDB() + ""
        }));
        printjson(_db.adminCommand({
            movePrimary: coll.getDB() + "",
            to: shard_ids[0]
        }));

        var shardConfig = {
            shardCollection: coll + "",
            key: {
                _id: 1
            }
        };

        while (!_db.adminCommand(shardConfig).ok) {
            // wait until sharding of collection is done
        }

        for (i = 1; i < numChunks; i++) {
            sh.splitAt(coll + "", {
                _id: chunkSize * i
            });
        }

        // insert some doc
        var bulk = coll.initializeUnorderedBulkOp();
        for (i = 0; i < numberDoc; i++) {
            bulk.insert({
                _id: i,
                a: Math.floor(Math.random() * 1000000), // indexed
                c: longStr // not indexed
            });
        }
        bulk.execute();

        // Index a non-id key
        coll.createIndex({
            a: 1
        });
    };

    // Move all chunks that are not on target shard to it
    function moveAllShardsTo(shard) {
        var collUUID = config.collections.findOne({_id: coll.getFullName()}).uuid;
        var chunksFindQuery = majorVersion < 5 ? {ns: coll.getFullName(), shard: { $ne: shard }} : {uuid: collUUID, shard: {$ne: shard}};
        var chunksNotOnShard = config.chunks.find(chunksFindQuery).toArray();

        chunksNotOnShard.forEach(function (chunk) {
            // chunk example:
            // {
            //     "_id" : "shardDB.moveChunkTest-shardKey_MinKey",
            //     "lastmod" : Timestamp(2, 1),
            //     "lastmodEpoch" : ObjectId("54af2130f70456e0a7a67309"),
            //     "uuid" : `collection's uuid`,
            //     "min" : {
            //         "shardKey" : { "$minKey" : 1 }
            //     },
            //     "max" : {
            //         "shardKey" : 1
            //     },
            //     "shard" : "rs0"
            // }
            var moveChunkCommand = {
                moveChunk: coll + "",
                find: chunk.min, // "min" lives in this chunk.
                to: shard
            };
            // Use default secondaryThrottle unless it was explicitly specified in the config.
            if (secondary_throttle !== default_val) {
                moveChunkCommand["_secondaryThrottle"] = secondary_throttle;
            }
            if (wait_for_delete !== default_val) {
                moveChunkCommand["_waitForDelete"] = wait_for_delete;
            }

            var res = config.adminCommand(moveChunkCommand);
            printjson(res);
            assert(res.ok);
        });
    }

    var getTestName = function () {
        return "moveChunk_secondaryThrottle_" + secondary_throttle + "_waitForDelete_" + wait_for_delete;
    };

    // move chunk around, time it
    var testMoveChunk = function (_shard) {
        var d1, d2;

        print("\ntest moveChunk with\t _secondaryThrottle=" + secondary_throttle +
            " _waitForDelete=" + wait_for_delete);

        d1 = Date.now();
        moveAllShardsTo(_shard);
        d2 = Date.now();

        print("\n\nmoveChunk takes " + (d2 - d1) +
            " ms with\t _secondaryThrottle=" + secondary_throttle +
            " _waitForDelete=" + wait_for_delete);

        var _throughput = numberDoc * 1000 / (d2 - d1);

        reportThroughput(getTestName(),
            _throughput, {
                nThread: 1,
                pass: true
            });
    };

    var increaseMigrationWaitTimeout = function () {
        var configDB = db.getSiblingDB("config");
        var shards = configDB.shards.find().sort({_id: 1}).toArray();
        for (var i = 0; i < shards.length; i++) {
            var shard = shards[i];

            // Connect to each shard check binary version and increase the timeout.
            if (authEnabled){
                // Slice off the leading rs*/ from the list of members in the replica set
                shardHostParse = shard.host.slice(4);
                shardHostAuth = 'mongodb://'.concat(username, ':', password, '@', shardHostParse);
            } else {
                shardHostAuth = shard.host;
            }
            var mongo = new Mongo(shardHostAuth);
            var shardDB = mongo.getDB("admin");
            var buildInfo = shardDB.adminCommand({buildInfo: 1});
            if (buildInfo.versionArray[0] >= 5 || (buildInfo.versionArray[0] === 4 && buildInfo.versionArray[1] === 4)) {
                shardDB.adminCommand({setParameter: 1, receiveChunkWaitForRangeDeleterTimeoutMS: 3600000});
            }
        }
    };

    sh.status();  // check shard status before test run

    increaseMigrationWaitTimeout();
    initColl();
    quiesceSystem();
    sleep(5000);

    // The last move in this loop moves the chunk back to the first shard (rs0).
    // We limit number of moves in the case that we have many shards.
    maxMoves = numShards < 5 ? numShards : 5;
    for(var i = 1; i <= maxMoves; i++) {
        toShard = shard_ids[i % maxMoves];

        testMoveChunk(toShard);
    }
})();
