/*
 * Test: Hello World Stub workload
*/

print("parameter1: " + parameter1);
print("parameter2: " + parameter2);
print("scaleFactor: " + scaleFactor);
for ( var i = 0; i <= scaleFactor; i++ ) {
    print("hello");
}
sleep(1000);
print("world");
reportThroughput("hello_world_" + parameter1 + "_" + parameter2, 100.0, {nThread: 1});

