var KafkaSocket = require('../lib/network/kafkaSocket');
var BufferMaker = require('buffermaker');


var enums = require('../lib/utils/enums');

var clientId = 'smokeTest';

var topic = 'mytopic';

var topic2 = 'testing123';

var _buf = new Buffer(0);

//metadata request
var metadata = new BufferMaker()
                    .Int16BE(enums.KAFKA_APIS.METADATA) //API Key
                    .Int16BE(0) //API Version
                    .Int32BE(2222) //Correlation ID
                    .Int16BE(clientId.length) //Client ID
                    .string(clientId)
                    .Int32BE(1) // Number of topics, e.g. length of topic array
                    .Int16BE(topic.length)
                    .string(topic)
                    .make();
                                      
console.log('metadata length: %d', metadata.length);

var metadataRequest = new Buffer(metadata.length + 4);

metadataRequest.writeInt32BE(metadata.length,0);

metadata.copy(metadataRequest,4);

//fetch request
var fetch = new BufferMaker()
                    .Int16BE(enums.KAFKA_APIS.METADATA) //API Key
                    .Int16BE(0) //API Version
                    .Int32BE(33333) //Correlation ID
                    .Int16BE(clientId.length) //Client ID
                    .string(clientId)
                    .Int32BE(1) // Number of topics, e.g. length of topic array
                    .Int16BE(topic2.length)
                    .string(topic2)
                    .make();
                                      
console.log('fetch length: %d', fetch.length);

var fetchRequest = new Buffer(fetch.length + 4);

fetchRequest.writeInt32BE(fetch.length,0);

fetch.copy(fetchRequest,4);

var MetadataRequest = require('../lib/protocol/serializer/metadataRequest');

var buffer2 = new MetadataRequest()
                    .apiKey(enums.KAFKA_APIS.METADATA)
                    .apiVersion(0)
                    .correlationId(2222)
                    .clientId(clientId)
                    .topics(['mytopic'])
                    .serialize();

console.log('buffer2:\n',buffer2); 

console.log('metadataRequest:\n',metadataRequest);  


var connectArgs = {
    host: 'localhost2',
    port: 9093,
    protocol: enums.KAFKA_PROTOCOL.SSL   
};

var connectOptions = {
    tls: {
        ca: [ require('fs').readFileSync('/software/Apache/Kafka/kafka_2.11-0.9.0.0/certs/ca-cert') ]
    }
}

//var socket = new KafkaSocket();
var socket = new KafkaSocket(connectArgs, connectOptions, function(error,message){
    
        console.log(error ? error : message);
    
    
}); //TLS localhost
//var socket = new KafkaSocket(function(err,message){err ? err : message});

socket.connect();

socket.on('ready',function(){
    console.log('ready');
    //socket.write(metadataRequest);
    //socket.write(fetchRequest);
    socket.write(buffer2);
})

//socket.on('error', function(){console.log('connect error')});

socket.on('data',function(data){
    console.log('received data');
    
    _buf = Buffer.concat([_buf,data]);
    //need at least 4 bytes
    if (_buf.length > 4)
    {
        var hexy = require('hexy');
        console.log(hexy.hexy(_buf))
        console.log('buffer length: ', _buf.length)
        //decode the length of the response message
        var responseSize = _buf.readInt32BE(0);
        console.log('responseSize: ', responseSize);
        var correlationId = _buf.readInt32BE(4);
        console.log('correlationId: ', correlationId);
        _buf = _buf.slice(responseSize + 4);
        
    };
    
    
    
    
})


