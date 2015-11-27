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



var util = require('util');
var Int64 = require('node-int64');
var BufferMaker = require('buffermaker');
var constants = require('../../constants');
var RequestMessage = require('./requestMessage');


/*
OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
  ReplicaId => int32
  TopicName => string
  Partition => int32
  Time => int64
  MaxNumberOfOffsets => int32
*/


function OffsetRequest(){
    //super constructor
    RequestMessage.call(this);
    
};

//base class
util.inherits(OffsetRequest,RequestMessage);


OffsetRequest.prototype.replica = function(replicaId){

    if (replicaId)
    {
        this._builder.Int32BE(replicaId);
    }
    else
    {
        //Normal client consumers should always specify this as -1 as they have no node id
        this._builder.Int32BE(-1);
    };
    return this;   
};

/*
[TopicName [Partition Time MaxNumberOfOffsets]]
*/
OffsetRequest.prototype.topics = function(topics){
    this._encodeArray(topics,this.topic);
    return this;
};

/*
TopicName [Partition Time MaxNumberOfOffsets]
*/
OffsetRequest.prototype.topic = function(topic){
    var self = this;
    self._encodeString(topic.topicName);

    self._builder.Int32BE(topic.partitions.length);
    topic.partitions.forEach(function(partition){
        var partitionBuffer = self.partition(partition);
        self._builder.string(partitionBuffer);        
    });  
};

/*
Partition Time MaxNumberOfOffsets
  Partition => int32
  Time => int64
  MaxNumberOfOffsets => int32
*/
OffsetRequest.prototype.partition = function(partition){
    var builder = new BufferMaker();
    builder.Int32BE(partition.partitionId)
           .string(new Int64(partition.time).toBuffer())
           .Int32BE(partition.maxNumberOfOffsets)
           
    return builder.make();
};


module.exports = OffsetRequest;