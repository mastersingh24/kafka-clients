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

/**
 * @private
 */
var debug = require('debug')('clients:KafkaProducer');
var util = require('util');
var helper = require('../utils/helpers');
var KafkaAPI = require('../api/0.9.0/apis');

var NetworkClient = require('./networkClient');

/**
 * Create a new KafkaProducer
 * @constructor
 * @classdesc A Kafka client that publishes to an [Apache Kafka]{@link http://kafka.apache.org/documentation.html} cluster.
 * Requires Kafka 0.9.0 or later.
 * @augments NetworkClient
 * @param {Object} producerConfig - configuration properties for the Kafka client
 * @param {Array.<{host: string, port:number}>} producerConfig.bootstrapBrokers - list of Kafka brokers used to establish the initial connection to the Kafka cluster
 * @param {string} producerConfig.clientId - Identifier string to pass to the Kafka broker when making requests
 * @param {boolean} producerConfig.secureConnection - Indicates whether or not TLS connections are required to communicate with the Kafka cluster
 * @param {Object} producerConfig.tlsOptions - Options to pass to TLS connection @see {@link https://nodejs.org/docs/latest-v0.12.x/api/tls.html#tls_tls_connect_options_callback|NodeJS TLS connect options}
 * @param {boolean} producerConfig.saslRequired - Indicates whether or not SASL authentication is required to connect to the Kafka cluster
 * @param {Object} producerConfig.saslOptions
 */
function KafkaProducer(producerConfig){
    NetworkClient.call(this,producerConfig);
    
    //internal state
    this._kafkaAPI = new KafkaAPI();
    
    
};

util.inherits(KafkaProducer,NetworkClient);

/**
 * @name sendRequest
 * @function
 * @private
 */

/**
 * Publish a message to the specified topic and partition
 * @param {Object} producerRecord
 * @param {string} producerRecord.topic - The topic this record is being sent to
 * @param {number} [producerRecord.partition=0] - The partition to which the record will be sent
 * @todo Implement optional round-robin scheme for choosing partition if not provided in the producerRecord 
 * @param {Object} [producerRecord.key=null] - Optional key used to determine the partition to which the record will be sent
 * @todo Implement support for choosing partition based on key
 * @param {string|Buffer} producerRecord.value - The payload data 
 * @param function(error, message) callback
 */
KafkaProducer.prototype.send = function(producerRecord,callback){
    var self = this;
    if (this._ready)
    {
        //check to see if we have metadata for the topic
        if (!producerRecord.partition) producerRecord.partition = 0;
        var partition = producerRecord.partition;
        var key = producerRecord.topic + '-' + partition;
              
        function callSendAPI(){
            debug('find broker for partition key: %s', key);
            //we should now have metadata for the topic-partition
            var partition = self._clusterMetadata.partitions[key];
            if (partition){
                var brokerKey = partition.leader.key;
                debug('found broker key: %s for partition key: %s', brokerKey, key);
                self._kafkaAPI.send.call(self,brokerKey,producerRecord,callback);
                
            }
            else
            {
                //topic-partition does not exist
                debug('could not find clusterMetadata for partition: %s', key);
                var error = new Error();
                error.name = 'UnknownTopicOrPartition';
                error.message = 'topic and/or partition does not exist';
                callback(error);
            };
            
        }
        
        if (this._clusterMetadata.partitions[key])
        {
            //we know the broker for the topic/partition
            debug('found existing metadata for topic-partition: ',key);
            callSendAPI();
        }
        else
        {
            //issue metadata request and update internal topic metadata
            debug('getting metadata for topic: ',producerRecord.topic);
            this._metadataRequest([producerRecord.topic],function(error, metadataResponse){
               
                debug('got metadata for topic %s:\n',key,JSON.stringify(metadataResponse,null,4));
                //parse and add to internal topic metadata
                var metadata = KafkaAPI.parseTopicMetadata(metadataResponse);

                for (var key in metadata.partitions)
                {
                    //add or replace key in current internal metadata
                    self._clusterMetadata.partitions[key] = metadata.partitions[key];
                };
                for (var key in metadata.brokers)
                {
                    //add or replace key in current internal metadata
                    self._clusterMetadata.brokers[key] = metadata.brokers[key];
                };
                
                debug('updated topicMetadata:\n',JSON.stringify(self._clusterMetadata,null,4));
                callSendAPI();
            });
        }
    }
    else
    {
        throw Error('Producer is not yet connected');
    }
    
};

/**
 * Issue a metadata request to the cluster
 * @private
 * @param {string[]} topics
 * @param {function(error,message)} callback
 */
KafkaProducer.prototype._metadataRequest = function(topics,callback){
    
    this._kafkaAPI.metadataRequest.call(this,topics,callback);
    
};

module.exports = KafkaProducer;

