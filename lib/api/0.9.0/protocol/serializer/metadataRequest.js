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
var RequestMessage = require('./requestMessage');

function MetadataRequest(){
    //super constructor
    RequestMessage.call(this);
    
};

//base class
util.inherits(MetadataRequest,RequestMessage);

/**
 * Encode the list of topics
 * @param {string[]} topics - List of topics to query for metadata
 */
MetadataRequest.prototype.topics = function(topics){
    this._encodeArray(topics,this._encodeString);
    return this;
};


module.exports = MetadataRequest;