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


var debug = require('debug')('protocol:serializer:produceRequest');
var hexy = require('hexy');
var util = require('util');
var crc32 = require('buffer-crc32');
var Int64 = require('node-int64');
var BufferMaker = require('buffermaker');
var constants = require('../../constants');
var RequestMessage = require('./requestMessage');

/*
ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
  RequiredAcks => int16
  Timeout => int32
  Partition => int32
  MessageSetSize => int32
  
MessageSet => [Offset MessageSize Message]
  Offset => int64
  MessageSize => int32
  
Message => Crc MagicByte Attributes Key Value
  Crc => int32
  MagicByte => int8
  Attributes => int8
  Key => bytes
  Value => bytes

*/


function ProduceRequest(){
    //super constructor
    RequestMessage.call(this);   
};

util.inherits(ProduceRequest,RequestMessage);

ProduceRequest.prototype.requiredAcks = function(acks){
    this._builder.Int16BE(acks);
    return this;
};

ProduceRequest.prototype.timeout = function(timeout){
    this._builder.Int32BE(timeout);
    return this;
};

/**
 * @param {object[]} topics
 */
ProduceRequest.prototype.topics = function(topics){
   
    this._encodeArray(topics,this.topic);
    return this;
    
}

/**
 * @param {topicName: string, partitions: object[]}
 */

ProduceRequest.prototype.topic = function(topic){
    var self = this;
    self._encodeString(topic.topicName);

    self._builder.Int32BE(topic.partitions.length);
    topic.partitions.forEach(function(partition){
        var partitionBuffer = self.partition(partition);
        self._builder.string(partitionBuffer);        
    });    

};

/**
 * @param {partitionId: number, messages: object[]} partition
 * 
 * Partition => int32
   MessageSetSize => int32
   MessageSet
 */
ProduceRequest.prototype.partition = function(partition){
    
    var builder = new BufferMaker();
    var messageSetBuffer = this.messageSet(partition.messages);
    var messageSetSize = messageSetBuffer.length;
    builder.Int32BE(partition.partitionId).Int32BE(messageSetSize).string(messageSetBuffer);
    return builder.make();
};

/**
 * @param {Array.<{key: string, value: string}>} messages
 * 
 * MessageSet => [Offset MessageSize Message]
    Offset => int64
    MessageSize => int32
    Message
 */
ProduceRequest.prototype.messageSet = function(messages){
    var self = this;
    var builder = new BufferMaker();
    messages.forEach(function(message){
        
        //offset
        builder.string(new Int64(-1).toBuffer());
        var messageBuffer = self.message(message);
        var messageSize = messageBuffer.length;
        
       
        builder.Int32BE(messageSize).string(messageBuffer);
    });
    return builder.make();
    
};

/**
 * @param {{key: string, value: string}} message
 * 
 * Message => Crc MagicByte Attributes Key Value
    Crc => int32
    MagicByte => int8
    Attributes => int8
    Key => bytes
    Value => bytes
 */
ProduceRequest.prototype.message = function(message){
    
    var builder = new BufferMaker();
    
    //MagicByte and Attributes
    builder.Int8(constants.MAGIC_BYTE).Int8(constants.MESSAGE_ATTRIBUTES_DEFAULT);
    
    //Key   
    //check to see if we have a key
    if (message.key)
    {
        debug('key.length: %d | key: %s',message.key.length,message.key);
        builder.Int32BE(message.key.length).string(message.key);
    }
    else
    {
        //null = -1
        builder.Int32BE(-1);
    };
    
    //Value
    debug('value.length: %d | value: %s',message.value.length,message.value);
    builder.Int32BE(message.value.length).string(message.value);
    
    //Crc 
    var messageBuffer = builder.make(); //need the bytes for everything after Crc
    debug('\n',hexy.hexy(messageBuffer));
    var crc = crc32.signed(messageBuffer);
    debug('\ncrc: ',crc)
    builder = new BufferMaker();
    
    return builder.Int32BE(crc).string(messageBuffer).make();
    
}

module.exports = ProduceRequest;