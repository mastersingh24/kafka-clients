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
var debug = require('debug')('clients:KafkaConsumer');
var util = require('util');
var helper = require('../utils/helpers');
var KafkaAPI = require('../api/0.9.0/apis');

var NetworkClient = require('./networkClient');

/**
 * Create a new KafkaConsumer
 * @constructor
 * @classdesc A Kafka client that consumes records from an [Apache Kafka]{@link http://kafka.apache.org/documentation.html} cluster. 
 * Requires Kafka 0.9.0 or later. 
 * It is modeled after the [Kafka Java consumer]{@link http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html}
 * @augments NetworkClient
 * @param {Object} consumerConfig - configuration properties for the Kafka client
 * @param {Array.<{host: string, port:number}>} consumerConfig.bootstrapBrokers - list of Kafka brokers used to establish the initial connection to the Kafka cluster
 * @param {string} consumerConfig.clientId - Identifier string to pass to the Kafka broker when making requests
 * @param {boolean} consumerConfig.secureConnection - Indicates whether or not TLS connections are required to communicate with the Kafka cluster
 * @param {Object} consumerConfig.tlsOptions - Options to pass to TLS connection @see {@link https://nodejs.org/docs/latest-v0.12.x/api/tls.html#tls_tls_connect_options_callback|NodeJS TLS connect options}
 * @param {boolean} consumerConfig.saslRequired - Indicates whether or not SASL authentication is required to connect to the Kafka cluster
 * @param {Object} consumerConfig.saslOptions
 */
function KafkaConsumer(consumerConfig){
    NetworkClient.call(this,consumerConfig);
    
    //internal state
    this._kafkaAPI = new KafkaAPI();
    
    
};
//base class
util.inherits(KafkaConsumer,NetworkClient);

/**
 * Use this method if you do not wish to have Kafka manage partition assignments for this consumer.  
 * Manual topic assignment through this method does not use the consumer's group management functionality. 
 * As such, there will be no rebalance operation triggered when group membership or cluster and topic metadata change. 
 * Note that it is not possible to use both manual partition assignment with [assign]{@link KafkaConsumer#assign} 
 * and group assignment with [subscribe]{@link KafkaConsumer#subscribe}  
 * @summary Manually assign a list of partitions to this consumer
 * @param {Array.<{topic: string, port:partition}>} partitions - The list of partitions to assign this consumer
 */
KafkaConsumer.prototype.assign = function(partitions){
    
};

/**
 * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. 
 * It is an error to not have subscribed to any topics or partitions before polling for data.
 * @param {numnber} timeout - The time, in milliseconds, spent waiting in poll if data is not available. 
 * If 0, returns immediately with any records that are available now. Must not be negative.
 */
KafkaConsumer.prototype.poll = function(timeout){
    
}

/**
 * Subscribe to the given list of topics to get dynamically assigned partitions. 
 * Topic subscriptions are not incremental. This list will replace the current assignment (if there is one). 
 * It is not possible to combine [topic subscription]{@link KafkaConsumer#subscribe} with group management 
 * with manual partition assignment through [assign]{@link KafkaConsumer#assign}
 * @param {string[]} topics - The list of topics to subscribe to
 */
KafkaConsumer.prototype.subscribe = function(topics){
    
}



module.exports = KafkaConsumer;
