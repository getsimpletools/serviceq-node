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
    this._defaultMsgTtl = 90;

    if(!queue)
        throw new Error("Please specify path to a service");

    if (!(this instanceof serviceq)) {
        return new serviceq(queue);
    }

    this._queue = queue;
};

serviceq._preparePayload = function (msg,method,etc)
{
    var $this = this;
    var payload                = {};
    var mTime                  = new Date().getTime();

    payload.status           = $this._status
    payload.statusMsg       = '';

    payload.meta            = {
        'creator'               : '',
        'queue'                 : {'name' : $this._queue},
        'method'                : method,
        'date'                  : {
            'atom'                  : new Date(mTime).ISOString(),
            'mtimestamp'            : mTime,
            'timestamp'             : Math.floor(mTime / 1000)
        }
    }


    if(typeof etc.topic !== undefined)
    {
        payload.meta.queue.topic = etc.topic;
    }

    if(typeof etc.expires !== undefined)
    {
        payload.meta.expires = parseFloat(etc.expires);
    }

    payload.body    = msg;

    return JSON.stringify(payload)
}

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

serviceq.prototype.publish = function(msg, ttl)
{
    var $this = this;

    if(typeof ttl === 'undefined') ttl =$this._defaultMsgTtl;
    $this.connect(function(){
        $this.channel.assertQueue($this._queue, {durable: true, exclusive: false, autoDelete: false});
        $this.channel.sendToQueue($this._queue, Buffer.from($this._preparePayload(msg,'PUBLISH')),{
            expiration: ttl*1000
        });
    });
}

serviceq.prototype.sleep = function(ms){
    return new Promise(resolve => setTimeout(resolve, ms));
}

serviceq.prototype.call = async function(message, timeout,ttl)
{
    var $this = this;
    var response= null;
    var queue = null;

    if(typeof timeout === 'undefined') timeout =60;
    if(typeof ttl === 'undefined') ttl = $this._defaultMsgTtl;

    $this.connect(function(){
        var correlationId = (new Date()).getTime().toString(36) + Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
        function maybeAnswer(msg) {
            //console.log('recived msg -', message.offset);
            if(!msg) return;

            if (msg.properties.correlationId === correlationId) {
                response = msg.content
                $this.channel.ack(msg);
                $this.channel.cancel(queue);
                setTimeout(()=>{
                    try{
                        $this.channel.deleteQueue(queue);
                    }catch (e){};
                },500)
            }
            else
            {
                $this.channel.ack(msg);
                $this.channel.cancel(queue);
                setTimeout(()=>{
                    try{
                        $this.channel.deleteQueue(queue);
                    }catch (e){};
                },500)
                throw new Error("Unexpected message");
        }
        }
        // console.log('assertQueue -',message.offset)
        $this.channel.assertQueue('',  {exclusive: true, durable: false,autoDelete: true}, function(err, ok) {
            if (err !== null)  throw err;
            queue = ok.queue;
            $this.channel.consume(queue, maybeAnswer, {noAck:false});
            // console.log('assigne msg -',message.offset)
            $this.channel.sendToQueue($this._queue, Buffer.from($this._preparePayload(message,'CALL')), {
                replyTo: queue,
                correlationId: correlationId,
                expiration: ttl*1000
            });
        });
    });

    for (let i = 0; i < timeout*10; i++) {
        if(response !== null)
            return response;

        await $this.sleep(100);
    }
    return null;
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