var topics = [
    {
        "topicName":"mytopic",
        "partitions":[
            {
                "partitionId":"0",
                "fetchOffset":0,
                "maxBytes":2000000
            }
        ]
    }
];

var topics2 = [
    {
        "topicName":"Owl tesselation",
        "partitions":[
            {
                "partitionId":"1",
                "fetchOffset":200,
                "maxBytes":2000
            }
        ]
    }
];

var FetchRequest = require('../lib/api/0.9.0/protocol/serializer/fetchRequest');
var KafkaConsumer = require('../lib/clients/kafkaConsumer');
var constants = require('../lib/api/0.9.0/constants');

//var clientId = 't4K1Mf8dojFblUDdXLwBu4NaEZInI9tmpAnNgJKY2G0Dibta';
var clientId = 'Mr Flibble';

var fetchRequest = new FetchRequest()
                        .apiKey(constants.KAFKA_APIS.FETCH)
                        .apiVersion(0)
                        .correlationId(1234)
                        .clientId(clientId)
                        .replica()
                        .maxWaitTime(1000)
                        .minBytes(1000)
                        .topics(topics)
                        .serialize();
                        
var hexy = require('hexy');

console.log(hexy.hexy(fetchRequest));