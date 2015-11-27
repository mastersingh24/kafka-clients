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
var parserUtils = require('./parserUtils');

/**
 MetadataResponse => [Broker][TopicMetadata]
  Broker => NodeId Host Port  (any number of brokers may be returned)
    NodeId => int32
    Host => string
    Port => int32
  TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
    TopicErrorCode => int16
  PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
    PartitionErrorCode => int16
    PartitionId => int32
    Leader => int32
    Replicas => [int32]
    Isr => [int32]
 */

var debug = require('debug')('parser:metadataResponse');

exports.parse = function(buffer){
    
    var metadataResponse = {};
    var offset = 8;  //first 8 bytes are size and correlation id
    
    //parse the broker list
    metadataResponse.brokers= [];
    metadataResponse.topics= [];
    
    //Parse the broker metadata
    //number of brokers
    var numBrokers = buffer.readInt32BE(offset);
    offset+=4; //Int32 takes 4 bytes
    for (var int = 0;int < numBrokers;++int)
    {
        var broker = {};
        broker.nodeId = buffer.readInt32BE(offset);
        offset+=4;
        var result = parserUtils.parseString(buffer,offset); //parse host as string
        broker.host = result.value;
        broker.port = buffer.readInt32BE(result.offset);
        metadataResponse.brokers.push(broker);
        offset=result.offset+4;
    }
    
    //parse the topic metadata
    //number of topics
    var numTopics = buffer.readInt32BE(offset);
    debug('numTopics:', numTopics)
    offset+=4;
    for (var int = 0;int < numTopics;++int)
    {
        var topic = {};
        topic.topicErrorCode = buffer.readInt16BE(offset);
        offset+=2;
        var result = parserUtils.parseString(buffer,offset);
        topic.topicName = result.value;
        offset = result.offset;
        //partition metadata
        topic.partitions = [];
        //number of partitions
        var numPartitions = buffer.readInt32BE(offset);
        debug('numPartitions',numPartitions);
        offset+=4;
        for(var int=0;int < numPartitions;++int)
        {
            var partition = {};
            partition.partitionErrorCode = buffer.readInt16BE(offset);
            offset+=2;
            partition.partitionId = buffer.readInt32BE(offset);
            offset+=4;
            partition.leader = buffer.readInt32BE(offset);
            offset+=4;
            //replicas
            var numReplicas = buffer.readInt32BE(offset);
            debug('numReplicas',numReplicas);
            offset+=4;
            partition.replicas = [];
            for(var int=0;int < numReplicas;++int)
            {
                partition.replicas.push(buffer.readInt32BE(offset));
                offset+=4;
            }
            
            //Lsrs
            var numLsrs = buffer.readInt32BE(offset);
            debug('numLsrs', numLsrs);
            offset+=4;
            partition.lsrs = [];
            for(var int=0;int < numReplicas;++int)
            {
                partition.lsrs.push(buffer.readInt32BE(offset));
                offset+=4;
            }
            
            topic.partitions.push(partition);
        }
        
        
        
        metadataResponse.topics.push(topic);
    }
    return metadataResponse;
    
}