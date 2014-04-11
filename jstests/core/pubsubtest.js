// this test the pubsub

var pubsub = db.PubSub();
var topic = pubsub.getTopic("butts");
var sub = topic.subscribe();
topic.publish("hello!");

assert(sub.hasNext());
var msg = sub.next();
assert.eq(msg, {name: "butts", msg: "hello!"});
printjson(msg);

sub.addTopic("monkeys");
var sub2 = topic.subscribe();
for (i = 0; i < 10; i++) {
    topic.publish("hello " + i);
}

var topic2 = pubsub.getTopic("monkeys");
for (i = 0; i < 3; i++) {
    topic2.publish("hello " + i);
}

var count = 0;
jsTest.log("starting loop1");
while (sub2.hasNext()) {
    msg = sub2.next();
    count++;
    printjson(msg);
}
jsTest.log("finshing loop1");
assert.eq(count, 10);
    
count = 0;
jsTest.log("starting loop2");
while (sub.hasNext()) {
    msg = sub.next();
    count++;
    printjson(msg);
}
jsTest.log("finishing loop2");
assert.eq(count, 13);
