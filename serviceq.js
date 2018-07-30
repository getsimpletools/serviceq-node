var amqp = require('amqplib/callback_api');

settings = {
    'username': 'guest',
    'password': 'guest',
    'host': 'localhost',
    'vhost': '/',
    'port': 5672
}

function serviceq(queue) {

    this._queue             = undefined;
    this._req               = undefined;
    this._status            = 200;
    this._serviceQRequest   = undefined;

    if(!queue)
        throw new Error("Please specify path to a service");

    if (!(this instanceof serviceq)) {
        return new serviceq(queue);
    }

    this._queue = queue;
};

serviceq.service = function(queue)
{
    return new serviceq(queue);
}

serviceq.settings = function(_settings)
{
    settings.host = _settings.host;
    settings.port = _settings.port;
    settings.vhost = _settings.vhost;
    settings.password = _settings.password;
    settings.username = _settings.username;

    return this;
}

serviceq.prototype.serve = function(callback)
{
    this.serve(callback);
    return this;
}

serviceq.prototype.connect = function(method)
{
    if(!this.connection) {
        var $this = this;
        amqp.connect('amqp://' + settings.username + ':' + encodeURIComponent(settings.password) + "@" + settings.host + ":" + settings.port + "/" + settings.vhost, function (err, conn) {

            if (err) {
                throw err;
            }

            $this.connection = conn;

            conn.createChannel(function(err, ch) {

                $this.channel = ch;
                method();

            });
        });
    }
    else
    {
        method();
    }
}

serviceq.prototype.status = function(code)
{
    this._status = code;
    return this;
}

serviceq.prototype.serve = function(callback)
{
    var $this = this;
    $this.connect(function(){
        $this.channel.assertQueue($this._queue, {durable: true});
        $this.channel.prefetch(1);
        $this.channel.consume($this._queue, function(req) {

            $this._status = 200; //reset
            $this._req = req;

            $this._serviceQRequest  = JSON.parse(req.content.toString());
            req                     = JSON.parse(req.content.toString());

            callback(req,$this);

        }, {noAck: false});
    });
}


serviceq.prototype.reply = function(msg)
{
    var $this = this;

    $this.channel.sendToQueue(
        $this._req.properties.replyTo,
        new Buffer.from($this._preparePayload(msg,"REPLY")),
        {correlationId: $this._req.properties.correlationId}
    );

    return this;
}

serviceq.prototype.acknowledge = function()
{
    if(this._req)
    {
        this.channel.ack(this._req);
        this._req = undefined;
        this._serviceQRequest = undefined;
    }
    else
    {
        throw new Error("This message has been already acknowledged");
    }

    return this;
}

serviceq.prototype._preparePayload = function(msg,method,etc)
{
    var payload                 = {};
    var mTime                   = new Date().getTime();

    payload.status          = this._status;
    payload.statusMsg       = ""; //TODO

    payload.meta            = {
        'creator': '', //todo //$this->_getTopmostScript(),
        'queue': {
            'name': this._queue
        },
        'method': method,
        'date': {
            'atom':  new Date().toISOString(),
            'mtimestamp': mTime / 1000,
            'timestamp': Math.floor(mTime / 1000)
        }
    }

    if(etc && etc.topic)
    {
        payload.meta.queue.topic = etc.topic;
    }

    if(etc && etc.expires)
    {
        payload.meta.expires = etc.expires;
    }

    if(this._serviceQRequest)
    {
        payload.meta.servingTimeSec = mTime / 1000 - this._serviceQRequest.meta.date.mtimestamp;
    }

    payload.body    = msg;

    return JSON.stringify(payload);
}

module.exports = serviceq;