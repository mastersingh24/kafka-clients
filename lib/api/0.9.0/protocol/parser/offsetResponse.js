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

var debug = require('debug')('parser:offsetResponse');
var parserUtils = require('./parserUtils');
var Int64 = require('node-int64');

/*
OffsetResponse => [TopicName [PartitionOffsets]]
  PartitionOffsets => Partition ErrorCode [Offset]
  Partition => int32
  ErrorCode => int16
  Offset => int64
  
*/
exports.parse = function(buffer){
    
    debug('buffer:\n',buffer);
    
    var offset = 8;
    var offsetResponse = {};
    offsetResponse.topics = {};
    var numTopics = buffer.readInt32BE(offset);
    
    debug('numTopics',numTopics);
    
    offset+=4;
    /**
     * loop through topics
     * [TopicName [PartitionOffsets]]
     */
    for (var int=0;int<numTopics;++int)
    {
        var topic = {};
        var result = parserUtils.parseString(buffer,offset); //get the topicName as string
        var topicName = result.value;
        topic.partitions = {};
        var numPartitionOffsets = buffer.readInt32BE(result.offset);
        debug('numPartitionOffsets',numPartitionOffsets);
        offset=result.offset+4;
        /**
         * loop through partitionOffsets
         * PartitionOffsets => Partition ErrorCode [Offset]
            Partition => int32
            ErrorCode => int16
            Offset => int64
         */
        for(var int=0;int<numPartitionOffsets;++int)
        {
            var partition={};
            var partitionId = buffer.readInt32BE(offset);
            offset+=4;
            partition.errorCode = buffer.readInt16BE(offset);
            offset+=2;
            /**
             * loop through offsets
             * [Offset]
                 Offset => int64
             */
             partition.offsets = [];
             var numOffsets = buffer.readInt32BE(offset);
             offset+=4;
             for (var int=0;int<numOffsets;++int){
                 var partitionOffset = new Int64(buffer,offset).toNumber(true);
                 partition.offsets.push(partitionOffset);
                 offset+=8;
             }

            topic.partitions[partitionId] = partition;
        }
        offsetResponse.topics[topicName]=topic;
    }
    
    
    return offsetResponse;
};

function parseMessageSet(messageSetBuffer,messageSetSize){
    
}