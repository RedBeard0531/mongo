var PubSub;

(function() {

if (PubSub === undefined) {
    PubSub = function(db) {
        this._db = db;
    }
}

Topic = function(topicName, db, opts) {
    this._name = topicName;
    this._db = db;
    this._opts = opts;
}

PubSub.prototype.getTopic = function(topicName) {
    return new Topic(topicName, this._db);
}

Topic.prototype.publish = function(msg, opts) {
    var msgObj = {pub: this._name, msg: msg};
    Object.extend(msgObj, opts);
    this.runCommand(msgObj);
}

Topic.prototype.runCommand = function(obj) {
    printjson(obj);
    return this._db.runCommand(obj);
}

Topic.prototype.subscribe = function() {
    return new Subscription(this.runCommand({sub: this._name}).cursorId, this._db);
}

Subscription = function(id, db) {
    if (id instanceof NumberLong) {
        this._id = id;
    }
    else {
        this._id = NumberLong(id);
    }
    this._db = db;
    this._cache = [];
}

Subscription.prototype.runCommand = Topic.prototype.runCommand;

Subscription.prototype.shellPrint = function() {
    this.hasNext();
    return this.next();
}

Subscription.prototype.unsubscribe = function() {
    return assert.commandWorked(this.runCommand({unsub: this._id}));
}

Subscription.prototype.addTopic = function(topicName) {
    assert.commandWorked(this.runCommand({sub: topicName, id: this._id}));
    return this;
}

Subscription.prototype.hasNext = function() {
    if (this._cache.length === 0) {
        this._cache = this._poll();
    }
    return this._cache.length !== 0;
}

Subscription.prototype.next = function() {
    return this._cache.pop();
}

Subscription.prototype._poll = function(timeout) {
    // TODO fix NumberLong troubles and other type things
    var pollObj = {poll: this._id};
    if (timeout) {
        pollObj.timeout = NumberLong(timeout);
    }
    else {
        pollObj.timeout = NumberLong(1000);
    }

    return assert.commandWorked(this.runCommand(pollObj)).messages;
}

}());
