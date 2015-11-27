var RequestMessage = require('../lib/protocol/serializer/requestMessage');

var buffer = new RequestMessage()
                    .apiKey(0)
                    .apiVersion(0)
                    .correlationId(1111)
                    .clientId('myclient')
                    .serialize();
                    
console.log('buffer:\n',buffer);


var MetadataRequest = require('../lib/protocol/serializer/metadataRequest');

var buffer2 = new MetadataRequest()
                    .apiKey(0)
                    .apiVersion(0)
                    .correlationId(1112)
                    .clientId('myclient2')
                    .topics()
                    .serialize();

console.log('buffer2:\n',buffer2);                    
