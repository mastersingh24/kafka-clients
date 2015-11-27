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
var debug = require('debug')('clients:NetworkClient');
var helper = require('../utils/helpers');

/**
 * @private
 */
var KafkaSocket = require('../network/kafkaSocket');

/**
 * @private
 */
var constants = require('../utils/enums');

/**
 * Create a new NetworkClient
 * @constructor
 * @param {Object} clientConfig - configuration properties for the Kafka client
 * @param {Array.<{host: string, port:number}>} clientConfig.bootstrapBrokers - list of Kafka brokers used to establish the initial connection to the Kafka cluster
 * @param {string} clientConfig.clientId - Identifier string to pass to the Kafka broker when making requests
 * @param {boolean} clientConfig.secureConnection - Indicates whether or not TLS connections are required to communicate with the Kafka cluster
 * @param {Object} clientConfig.tlsOptions - Options to pass to TLS connection @see {@link https://nodejs.org/docs/latest-v0.12.x/api/tls.html#tls_tls_connect_options_callback|NodeJS TLS connect options}
 * @param {boolean} clientConfig.saslRequired - Indicates whether or not SASL authentication is required to connect to the Kafka cluster
 * @param {Object} clientConfig.saslOptions
 */

function NetworkClient(clientConfig) {
    
    debug('Constructing client with the following config:\n',JSON.stringify(clientConfig));
    
    
    //set up internal state
    this._clientId = clientConfig.clientId || 'kafka-network-client';
    this._metaBrokerKey; //key of the broker to use for initial communication with the Kafka cluster
    this._kafkaBrokers = {}; //maintain internal list of connected brokers
    this._connectOptions = {}; //keep the connectOptions around for future connections
    this._protocol = constants.KAFKA_PROTOCOL.PLAINTEXT; //keep the protocol around for future connections
    this._ready = false;
    
    //cluster metadata
    this._clusterMetadata = {}; 
    this._clusterMetadata.partitions = {} //map each (topic,partition) pair to the the appropriate broker(s)
    this._clusterMetadata.brokers = {}; //maintain a list of brokers from the cluster metadata
    
    
    var self = this;
    var connectArgs = [];
    
    
    //determine which protocol to use
    if (clientConfig.secureConnection)
    {
        this._protocol = (clientConfig.saslRequired) ? constants.KAFKA_PROTOCOL.SASL_SSL : constants.KAFKA_PROTOCOL.SSL;
    }
    else if (clientConfig.saslRequired)
    {
       this._protocol = constants.KAFKA_PROTOCOL.SASL_PLAINTEXT; 
    };
    
    //check to see if we have a list of bootstrap servers
    if (clientConfig.bootstrapBrokers)
    {
           //check to see if this is an array
           if (Array.isArray(clientConfig.bootstrapBrokers))
           {
               //build array of connectArgs
               clientConfig.bootstrapBrokers.forEach(function(broker){
                   broker.protocol = self._protocol;
                   connectArgs.push(broker);
               })
           }
    }
    else
    {
        //defaults
        connectArgs.push({protocol: this._protocol});
    };
    
    this._connectOptions.tls = clientConfig.tlsOptions || {};
    this._connectOptions.sasl = clientConfig.saslOptions || {};
    
    //connect to a broker in bootstrap list
    var connectCallback = function(err, msg){
        if (!err)
        {
            self._ready=true;
        } 
    };
    //we may actually end up connecting to multiple brokers
    for (var idx=0;idx < connectArgs.length;++idx)
    {
        if (self._ready) break;
        debug('Connecting to broker with args ',connectArgs[idx]);
        self.connectBroker(connectArgs[idx],this._connectOptions,connectCallback);
    }
};

NetworkClient.prototype.connectBroker = function(connectArgs,connectOptions,callback){
    
    var self = this;
    var _socket = new KafkaSocket(connectArgs,connectOptions,function(error,message){
        if (error)
        {
            debug('error connecting to Kafka broker %s:%d',message.host,message.port);
            callback(error);
               
        }
        else
        {
            debug('connected to Kafka broker %s:%d ',message.host,message.port);
            var key = helper.hash(message.host + ':' + message.port);
            if(!self._metaBrokerKey) self._metaBrokerKey = key; //use the first broker we can connect to as the metaBroker
            self._kafkaBrokers[key]=_socket; //add the broker to our internal list
            callback(null,message);
        }
    });
    _socket.connect();
};

/**
 * Send a request message to Kafka
 * @protected
 * @param {string} brokerKey - The key of the Kafka broker to use to send the request
 * @param {number} correlationId - The correlationId of the Kafka RequestMessage
 * @param {Buffer} requestMessage - The Kafka RequestMessage
 * @param {Object} responseHandler - Handler to register to process the response to this request
 */
NetworkClient.prototype.sendRequest = function(brokerKey, correlationId, requestMessage, responseHandler){
    
    var self = this; 
    //inline function
    function send(){
        self._kafkaBrokers[brokerKey].addResponseHandler(correlationId,responseHandler);
        self._kafkaBrokers[brokerKey].write(requestMessage);
    }
    //check to see if we are connected to the Kafka broker
    if (this._kafkaBrokers[brokerKey])
    {
        debug('reusing existing connection to Kafka broker %s:%d',this._kafkaBrokers[brokerKey].host,this._kafkaBrokers[brokerKey].port );
        send();
    }
    else
    {
        //need to lookup the connection info for the target Kafka broker
        var connectArgs = {};
        var connectOptions = self._connectOptions;
        for (var key in self._clusterMetadata.brokers){
            if (self._clusterMetadata.brokers[key].key == brokerKey)
            {
                connectArgs.host = self._clusterMetadata.brokers[key].host,
                connectArgs.port = self._clusterMetadata.brokers[key].port
                connectArgs.protocol = self._protocol;
            }
        }
        debug('creating new connection to Kafka broker %s:%d using %s',connectArgs.host,connectArgs.port,connectArgs.prototol);
        
         
        //connect to the broker
        this.connectBroker(connectArgs,connectOptions,function(error,message){
            if (error)
            {
                if(responseHandler.callback && typeof responseHandler.callback === 'function'){
                    var error = new Error();
                    error.name = 'LeaderNotAvailable';
                    error.message = 'Failed to establish connection to Kafka broker';
                    responseHandler.callback(error);
                }
            }
            else
            {
                debug('Added connection to Kafka broker %s:%d',self._kafkaBrokers[brokerKey].host,self._kafkaBrokers[brokerKey].port );
                send();
            }
            
        })
    }
}

module.exports = NetworkClient;