var FetchRequest = require('../lib/api/0.9.0/protocol/serializer/fetchRequest');
var OffsetRequest = require('../lib/api/0.9.0/protocol/serializer/offsetRequest');
var KafkaConsumer = require('../lib/clients/kafkaConsumer');
var constants = require('../lib/api/0.9.0/constants');






//set up the consumer
var consumerConfig = {};

consumerConfig.bootstrapBrokers = [
    //{host: 'localhost' , port: 9093}
    //{host: 'localhost' , port: 9092} 

    //{host: 'kafka01-prod01.messagehub.services.us-south.bluemix.net' , port: 9093},
    //{host: 'kafka02-prod01.messagehub.services.us-south.bluemix.net' , port: 9093},
    //{host: 'kafka03-prod01.messagehub.services.us-south.bluemix.net' , port: 9093},
    //{host: 'kafka04-prod01.messagehub.services.us-south.bluemix.net' , port: 9093},
    {host: 'kafka05-prod01.messagehub.services.us-south.bluemix.net' , port: 9093}  

];

consumerConfig.secureConnection = true;
consumerConfig.clientId = 't4K1Mf8dojFblUDdXLwBu4NaEZInI9tmpAnNgJKY2G0Dibta';

var consumer = new KafkaConsumer(consumerConfig);


var topic = 'mytopic';
var partition = 0;
var brokerKey;

var crc32 = require('buffer-crc32');
var uuid = require('uuid');
function generateId(){
    return crc32.signed(uuid.v4());
}


var fetchResponseHandler = {
        parser: require('../lib/api/0.9.0/protocol/parser/fetchResponse'),
        callback: function(error,message){
            console.log('fetch callback');
            console.log(JSON.stringify(message,null,4));
            var offsetHigh = message.topics[topic].partitions[partition].highwaterMarkOffset;
            console.log('next offset: ',offsetHigh);
            //fetch again
            doFetch(brokerKey,offsetHigh);
        }
};

var offsetResponseHandler = {
        parser: require('../lib/api/0.9.0/protocol/parser/offsetResponse'),
        callback: function(error,message){
            console.log('offset callback');
            console.log(JSON.stringify(message,null,4));
        }
};



function doFetch(brokerKey,offset){
    console.log('making fetch request');
    //sample topic structure
    var topics = [
        {
            "topicName":topic,
            "partitions":[
                {
                    "partitionId":partition,
                    "fetchOffset":offset,
                    "maxBytes":2000000
                }
            ]
        }
    ];
 
    var correlationId = generateId();
    var fetchRequest = new FetchRequest()
                            .apiKey(constants.KAFKA_APIS.FETCH)
                            .apiVersion(0)
                            .correlationId(correlationId)
                            .clientId(consumerConfig.clientId )
                            .replica()
                            .maxWaitTime(10000)
                            .minBytes(0)
                            .topics(topics)
                            .serialize();
                            
    consumer.sendRequest(brokerKey, correlationId, fetchRequest, fetchResponseHandler)
    
}

                        
function getOffsets(brokerKey){
    console.log('getting offset');
    
    console.log('broker key: ',brokerKey)
    //get the latest offset
    
    var offsets = [
        {
            "topicName":topic,
            "partitions":[
                {
                    "partitionId":partition,
                    "time":constants.OFFSET_LATEST,
                    "maxNumberOfOffsets":constants.MAX_NUMBER_OF_OFFSETS
                }
            ]
        }
    ];
    var correlationId = generateId();
    var offsetRequest = new OffsetRequest()
                            .apiKey(constants.KAFKA_APIS.LIST_OFFSETS)
                            .apiVersion(0)
                            .correlationId(correlationId)
                            .clientId(consumerConfig.clientId)
                            .replica()
                            .topics(offsets)
                            .serialize();
                            
        var offsetResponseHandler = {
                parser: require('../lib/api/0.9.0/protocol/parser/offsetResponse'),
                callback: function(error,message){
                    console.log('offset callback');
                    console.log(JSON.stringify(message,null,4));
                    var offset = message.topics[topic].partitions[partition].offsets[0];
                    console.log('latest offset: ',offset);
                    doFetch(brokerKey,offset); 
                }
        };
        consumer.sendRequest(brokerKey, correlationId, offsetRequest, offsetResponseHandler);
    }

setTimeout(function(){
    brokerKey = consumer._metaBrokerKey;
    getOffsets(brokerKey);
    
},1000);


