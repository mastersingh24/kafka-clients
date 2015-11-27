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

'use strict'

var net = require('net');
var tls = require('tls');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var debug = require('debug')('network:kafkaSocket');
var enums = require('../utils/enums');


/**
 *  @constructor
 *  @param {object} [connectArgs]
 *  @param {string} [connectArgs.host="localhost"]
 *  @param {number} [connectArgs.port=9092]
 *  @param {string} [connectArgs.protocol="PLAINTEXT"]
 *  @param {object} [connectOptions]
 *  @param {number} [connectOptions.connectTimeout=5000] - Connection timeout in milliseconds
 *  @param {object} [connectOptions.tls]
 *  @param {object} [connectionOptions.sasl]
 *  @param {function(error, message)} [readyCallback]
 */
function KafkaSocket(connectArgs, connectOptions, readyCallback){
    
    //setup
    EventEmitter.call(this);
    
    //internal member variables
    this._onDataHandlers = {}; //used to hold callback handlers based on correlationId since responses come back asyncrhously from Kafka
    this._responseBuffer = new Buffer(0);
    
        
    var _connectArgs = {};
    var _connectOptions = {};
    
    //determine how many args we have
    var numArgs = arguments.length;
    debug('Constructor called with %d arguments',numArgs);
    
  
    switch(numArgs){
        case 0:
            //use all defaults and no callback
            break;
            
        case 1:
            //check to see if function was passed in
            if (typeof arguments[0] === 'function')
            {
                //use all defaults and set callback
                this._readyCallback = arguments[0];
                
            }
            else
            {                
               _connectArgs = connectArgs;
            }
            break;
        case 2:
            
            _connectArgs = connectArgs;
            //check to see if function was passed in second argument
            if (typeof arguments[1] === 'function')
            {
                //use all defaults and set callback
                this._readyCallback = arguments[0];
                
            }
            else
            {                
               _connectOptions = connectOptions;
            }
            break;

        case 3:
            _connectArgs = connectArgs;
            _connectOptions = connectOptions;
            this._readyCallback = readyCallback; 
            break;   
    };
    
    debug('_connectArgs.protocol: ',_connectArgs.protocol);
    //set member variables
    this.host = _connectArgs.host || enums.KAFKA_DEFAULTS.HOST;
    this.port = _connectArgs.port || enums.KAFKA_DEFAULTS.PORT;
    this.protocol = _connectArgs.protocol || enums.KAFKA_DEFAULTS.PROTOCOL;
    this.tls_options = _connectOptions.tls || {};
    this.sasl_options = _connectOptions.sasl || {};
    this.connectTimeout = _connectOptions.connectTimeout || 10000;
    
    debug('_connectArgs: ',_connectArgs);
    
    //check whether TLS is required
    if (this.protocol==enums.KAFKA_PROTOCOL.PLAINTEXT||this.protocol==enums.KAFKA_PROTOCOL.SASL_PLAINTEXT)
    {
        this._requireTLS = false;
        debug('No TLS required');
    }
    else
    {
        this._requireTLS = true;
        debug('TLS required')
    }
    
    debug('Connection properties:\n\thost: %s\n\tport: %s\n\tprotocol: %s\ntls_options: %s\nsasl_options: %s\n',
                this.host,this.port,this.protocol,this.tls_options,this.sasl_options);

};

util.inherits(KafkaSocket, EventEmitter);

/**
 * Connect to a Kafka broker
 */
 
KafkaSocket.prototype.connect = function(){
    //handle socket events
    if (!this.socket) this._addEventListeners();
    
    //create the socket connection to the broker
    this._connectSocket();
      
};

/**
 * Reconnect to a Kafka broker
 */
KafkaSocket.prototype.reconnect = function(){
    
};

/**
 * Disconnect from a Kafka broker
 */
KafkaSocket.prototype.disconnect = function(){
    
    
};

/**
 * @private
 * Handle all socket events internally
 */

KafkaSocket.prototype._addEventListeners = function(){
    
    var self = this;
    
    //connect event
    var connectEvent = (this._requireTLS) ? 'secureConnect' : 'connect';
    
    self.addListener(connectEvent,function(){
        debug('socket connected to %s:%d',this.host,this.port);
        self.emit('ready');
        self._readyCallback(null,{host:this.host,port:this.port,status:'connected'});
    });
    
    //data event
    self.addListener('data',function(data){
        debug('received %d bytes from %s:%d: ',data.length,this.host,this.port,data);
        self._handleResponse(data);
    });
    
    //error event
    self.addListener('error',function(error){
        debug('error for connection %s:%d: ',this.host,this.port,error);
        self._readyCallback(error,{host:this.host,port:this.port,status:'connect error'});
    });
    
    //close event
    self.addListener('close',function(had_error){
        debug('connection to %s:%d closed (had_error: %s)',this.host,this.port,had_error);
    });
    
};

/**
 * @private
 * Handles the actual socket connection
 */
KafkaSocket.prototype._connectSocket = function(){
    var self = this;
    
    //callback to reset the socket timeout
    var resetConnectionTimeout = function () {
        debug && debug('connected so resetting connection timeout');
        this.setTimeout(0);
    };
    
    if (this._requireTLS)
    {
        this.socket = tls.connect(this.port,this.host,this.tls_options,resetConnectionTimeout);
    }
    else
    {
        this.socket = net.connect(this.port,this.host,resetConnectionTimeout);
    }
    
    //set timeout on the socket
    debug('setting connection timeout to ',this.connectTimeout);
    this.socket.setTimeout(this.connectTimeout,function(){
        debug('connectTimeout');
        this.destroy();
        var error = new Error('connection timeout');
        error.name = 'ConnectionTimeout';
        self.emit('error',error);
    })
       
    
    //bind all socket level events to ourself so we can handle internally
    var socketEvents = ['close', 'connect', 'data', 'drain', 'error', 'end', 'secureConnect', 'timeout'];
    socketEvents.forEach(function(event){
        self.socket.on(event, self.emit.bind(self,event));
    });
    
    //apply socket methods we'll use internally
    var socketMethods = ['destroy', 'write', 'pause', 'resume', 'setEncoding', 'ref', 'unref', 'address'];
    socketMethods.forEach(function(method){
        self[method] = function(){
            self.socket[method].apply(self.socket, arguments);
        };
    })
};

KafkaSocket.prototype._handleResponse = function(data){

    
    //append data to the responseBuffer
    this._responseBuffer = Buffer.concat([this._responseBuffer,data]);
    
    var responseLength = this._responseBuffer.length;
    //we need at least 4 bytes of data
    if (responseLength > 4)
    {
        //first 4 bytes determines the length of the response message
        var responseSize = this._responseBuffer.readInt32BE(0);
        //make sure we have all of the data
        if ((responseLength-4) >= responseSize)
        {
            //ok - we have all the data we need
            //find the response handler based on the correlationId
            var correlationId = this._responseBuffer.readInt32BE(4);
            if (this._onDataHandlers[correlationId])
            {
                debug('found handler for correlationId: ',correlationId);
                //parse the response
                var kafkaResponse = this._onDataHandlers[correlationId].parser.parse(this._responseBuffer);
                //invoke the callback
                if(this._onDataHandlers[correlationId].callback && typeof this._onDataHandlers[correlationId].callback === 'function')
                {
                    this._onDataHandlers[correlationId].callback(null,kafkaResponse);
                    //remove the handler
                    delete this._onDataHandlers[correlationId];
                };
            }
            else
            {
                debug('could not find handler for correlationId: ',correlationId);
            }
            
            //prepare buffer for next pass
            this._responseBuffer = this._responseBuffer.slice(responseSize + 4);
        }
    }
}

KafkaSocket.prototype.addResponseHandler = function(key, handler){
    this._onDataHandlers[key]=handler;
}

module.exports = KafkaSocket;

