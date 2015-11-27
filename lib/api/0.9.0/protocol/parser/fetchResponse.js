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

var debug = require('debug')('parser:fetchResponse');
var parserUtils = require('./parserUtils');
var Int64 = require('node-int64');

/*
FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
  TopicName => string
  Partition => int32
  ErrorCode => int16
  HighwaterMarkOffset => int64
  MessageSetSize => int32
  
*/
exports.parse = function(buffer){
    
    debug('buffer:\n',buffer);
    
    var offset = 8;
    var fetchResponse = {};
    fetchResponse.topics = {};
    var numTopics = buffer.readInt32BE(offset);
    
    debug('numTopics',numTopics);
    
    offset+=4;
    //loop through topics
    for (var int=0;int<numTopics;++int)
    {
        var topic = {};
        var result = parserUtils.parseString(buffer,offset); //get the topicName as string
        var topicName = result.value;
        topic.partitions = {};
        var numPartitions = buffer.readInt32BE(result.offset);
        debug('numPartitions',numPartitions);
        offset=result.offset+4;
        //loop through partitions
        for(var int=0;int<numPartitions;++int)
        {
            var partition={};
            var partitionId =buffer.readInt32BE(offset);
            offset+=4;
            partition.errorCode=buffer.readInt16BE(offset);
            offset+=2;
            partition.highwaterMarkOffset = new Int64(buffer,offset).toNumber(true);
            offset+=8;
            partition.messageSetSize = buffer.readInt32BE(offset);
            offset+=4;
                       
            offset+=partition.messageSetSize;
            //skip past MessageSet for now
            topic.partitions[partitionId]= partition;
        }
        fetchResponse.topics[topicName] = topic;
    }
    
    
    return fetchResponse;
};

function parseMessageSet(messageSetBuffer,messageSetSize){
    
}