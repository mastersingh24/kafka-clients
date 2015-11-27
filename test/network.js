var NetworkClient = require('../lib/clients/networkClient');

var clientConfig = {};

clientConfig.bootstrapBrokers = [
    //{host: 'localhost' , port: 9093},
    //{host: 'localhost' , port: 9091} ,
    {host: 'kafka01-prod01.messagehub.services.us-south.bluemix.net' , port: 9093}  
    
];

clientConfig.secureConnection = true;

var client = new NetworkClient(clientConfig);