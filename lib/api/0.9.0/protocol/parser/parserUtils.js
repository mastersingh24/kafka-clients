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

exports.parseString = function(buffer, offset){
    //length is encoded as Int16
    var length = buffer.readInt16BE(offset);
    var value; 
    
    //length = -1 indicates null value
    if (length == -1)
    {
        length = 0;
        value = '' //return empty string
    }
    else
    {
        //strings should be UTF8 encoded
        value = buffer.toString('utf8',offset+2,offset+2+length);
    }
    
    //return value and next offset to parse from
    return({
        value: value,
        offset: offset+2+length
    });
    
};