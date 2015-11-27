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

var BufferMaker = require('buffermaker');

/**
 * @constructor
 * This is the base class all Kafka requests should extend
 */
function RequestMessage() {
    
    this._builder = new BufferMaker();       
};

RequestMessage.prototype.apiKey = function(apiKey){
    this._builder.Int16BE(apiKey);
    return this;        
};

RequestMessage.prototype.apiVersion = function(apiVersion){
    this._builder.Int16BE(apiVersion);
    return this;        
};

RequestMessage.prototype.correlationId = function(correlationId){
    this._builder.Int32BE(correlationId);
    return this;        
};
    
RequestMessage.prototype.clientId = function(clientId){
    this._encodeString(clientId);
    return this;           
};


/**
 * Serializes the Kafka request message for transmission
 */
RequestMessage.prototype.serialize = function(){
    //serialize message bytes
    var message = this._builder.make();
    //create new buffer to add size as first 4 bytes
    var buffer = new Buffer(message.length + 4);
    buffer.writeInt32BE(message.length,0);
    //copy message into new buffer
    message.copy(buffer,4);
    return buffer;
}

//utility functions
    
/**
 * String encoder: {size=>Int16 string}
 * @param {string} value - String value to encode
 */
RequestMessage.prototype._encodeString = function(value){

    this._builder.Int16BE(value.length);
    this._builder.string(value);
};

/**
 * Array encoder: {array.length=>Int32 array[0]...array[length-1]}
 * @param {Object[]} array  - Array to encode
 * @param {function} encoder - Encoder to apply to elements of the array
 */
RequestMessage.prototype._encodeArray = function(array,encoder){
    
    var self = this;
    //first append the size of the array
    this._builder.Int32BE(array.length);
    //apply encoder function to each element
    array.forEach(function(element){
        encoder.apply(self,[element]);
    });
        
};

module.exports = RequestMessage;