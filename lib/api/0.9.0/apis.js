/**
 * Copyright 2015 IBM
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
*/

var debug = require('debug')('protocol:apis');
var hexy = require('hexy');
var uuid = require('uuid');
var crc32 = require('buffer-crc32');
var helper = require('../../utils/helpers');
var MetadataRequest = require('./protocol/serializer/metadataRequest');
var ProduceRequest = require('./protocol/serializer/produceRequest');
var FetchRequest = require('./protocol/serializer/fetchRequest');

var constants = require('./constants')

function KafkaAPI(){
    
    this.version = '0.9.0';
    this.MAGIC_BYTE = 0; //constant for this version of the API
    this.MESSAGE_ATTRIBUTES = 0;
    
    
};

KafkaAPI.prototype.metadataRequest = function(topics,callback){
    debug('metadataRequest API invoked');
    var correlationId = generateId();
    //var correlationId = 1111;
    debug('generated correlationId for metadataRequest: ',correlationId);
    var metadataRequest = new MetadataRequest()
                    .apiKey(constants.KAFKA_APIS.METADATA)
                    .apiVersion(0)
                    .correlationId(correlationId)
                    .clientId(this._clientId)
                    .topics(topics)
                    .serialize();
                    
    var responseHandler = {
        parser: require('./protocol/parser/metadataResponse'),
        callback: callback
    };
    
    this.sendRequest(this._metaBrokerKey, correlationId, metadataRequest, responseHandler);
        
};

/**
 * Convert Kafka metadata into our internal format
 */
KafkaAPI.parseTopicMetadata = function(metadata){
    
    var topicMetadata = {};
    //simplify broker array so we can access by key later
    var brokers = {};
    metadata.brokers.forEach(function(broker){       
        brokers[broker.nodeId] = {
            key: helper.hash(broker.host + ':' + broker.port),
            host:broker.host,
            port:broker.port
        }
    })
    
    //arrange (topic,partition) pairs into keys with format 'topic-partition'
    var partitions = {};
    metadata.topics.forEach(function(topic){
        var name = topic.topicName;
        topic.partitions.forEach(function(partition){
            var key = name + '-' + partition.partitionId;
            partitions[key] = {
                leader: brokers[partition.leader]
            }
        })
        
    });
    
    
    topicMetadata.brokers = brokers;
    topicMetadata.partitions = partitions;
    debug('internal topicMetadata:\n',JSON.stringify(topicMetadata,null,4));
    return topicMetadata;
    
};

KafkaAPI.prototype.send = function(brokerKey,producerRecord,callback){
    debug('send API invoked');
    var correlationId = generateId();
    debug('generated correlationId for send: ',correlationId);
    
    var record = {
        
        topicName: producerRecord.topic,
        partitions: [
            {
                partitionId: producerRecord.partition,
                messages:[
                    {
                        key: producerRecord.key,
                        value: producerRecord.value
                    }
                    
                ]
            }
        ]
        
    };
    
    debug('produce record:\n',JSON.stringify(record,null,4));
    
    
    var request = new ProduceRequest()
                    .apiKey(constants.KAFKA_APIS.PRODUCE)
                    .apiVersion(0)
                    .correlationId(correlationId)
                    .clientId(this._clientId)
                    .requiredAcks(1)
                    .timeout(2000)
                    .topics([record])
                    .serialize();
    
    debug('hex dump\n',hexy.hexy(request));
    
    var responseHandler = {
        parser: require('./protocol/parser/produceResponse'),
        callback: callback
    };
    
    this.sendRequest(brokerKey, correlationId, request, responseHandler);
    
};

KafkaAPI.prototype.fetch = function(brokerKey,fetchRecord,callback){
    
    debug('fetch API called');
    var correlationId = generateId();
    debug('generated correlationId for fetch: ',correlationId);
    
    var fetchRequest = new FetchRequest()
    
                        .apiKey(constants.KAFKA_APIS.FETCH)
                        .apiVersion(0)
                        .correlationId(correlationId)
                        .clientId(this._clientId )
                        .replica()
                        .maxWaitTime(10000)
                        .minBytes(0)
                        .topics(topics)
                        .serialize();
};


/**
 * Used to generate unique ids.  Currently only used for generating correlationId's for various Kafka requests
 */
function generateId(){
    return crc32.signed(uuid.v4());
}



module.exports = KafkaAPI;