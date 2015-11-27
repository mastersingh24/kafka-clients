var ProduceRequest = require('../lib/api/0.9.0/protocol/serializer/produceRequest');
var constants = require('../lib/api/0.9.0/constants');

var topics = 
[
    {
        'topicName':'test',
        'partitions':[
            {
                'partitionId':1,
                'messages':[
                    {
                        'key': '',
                        'value': 'Mr Flibble' 
                    },
                    {
                        'key': '',
                        'value': 'Fish/Cheese' 
                    },
                    {
                        'key': '',
                        'value': 'Cheese/Fish' 
                    }
                ]           
            }         
        ]
        
    }
];

var record =  [{
    "topicName": "mytopic",
    "partitions": [
        {
            "partitionId": 1,
            "messages": [
                {
                    "key": "mykey",
                    "value": "myvalue"
                },
                {
                    "key": "mykey2",
                    "value": "myvalue2"
                }
            ]
        }
    ]
}];

var clientId = 't4K1Mf8dojFblUDdXLwBu4NaEZInI9tmpAnNgJKY2G0Dibta';

/* produceRequest = new ProduceRequest()
                        .apiKey(constants.KAFKA_APIS.PRODUCE)
                        .apiVersion(0)
                        .correlationId(1234)
                        .clientId('Mr Flibble')
                        .requiredAcks(1)
                        .timeout(2000)
                        .topics(topics)
                        .serialize();
*/
                        
var record = new ProduceRequest()
                    .apiKey(constants.KAFKA_APIS.PRODUCE)
                    .apiVersion(0)
                    .correlationId(1)
                    .clientId('kafkaConnector')
                    .requiredAcks(1)
                    .timeout(12344)
                    .topics(record)
                    .serialize();
                        
console.log(record);

var hexy = require('hexy');

console.log('\n\n');

console.log(hexy.hexy(record));