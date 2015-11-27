var KafkaProducer = require('../lib/clients/kafkaProducer');

var producerConfig = {};

producerConfig.bootstrapBrokers = [
    //{host: 'localhost' , port: 9093}
    //{host: 'localhost' , port: 9092} 

    {host: 'kafka01-prod01.messagehub.services.us-south.bluemix.net' , port: 9093},
    {host: 'kafka02-prod01.messagehub.services.us-south.bluemix.net' , port: 9093},
    {host: 'kafka03-prod01.messagehub.services.us-south.bluemix.net' , port: 9093},
    {host: 'kafka04-prod01.messagehub.services.us-south.bluemix.net' , port: 9093},
    //{host: 'kafka05-prod01.messagehub.services.us-south.bluemix.net' , port: 9093}  

];

producerConfig.secureConnection = true;
producerConfig.clientId = 't4K1Mf8dojFblUDdXLwBu4NaEZInI9tmpAnNgJKY2G0Dibta';

var producer = new KafkaProducer(producerConfig);

//producer.send();

setTimeout(function(){
    setInterval(function(){
        var now = Date.now();
        producer.send({topic:'mytopic',partition:0,value:'myvalue' + now},function(error,message){
    
            if (error){
                console.log('Error:\n',error);
            }
            else
            {
                console.log(JSON.stringify(message,null,4));
            }
            
        });
    },10)
},1000)

/*
setTimeout(function(){
    console.log('\n\n\n\n\n\n\n\n\n\n\n');
    producer.send({topic:'mytopic'});
},5000)
*/