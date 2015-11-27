
var helper = require('../lib/utils/helpers');

var string1 = 'kafka01-prod01.messagehub.services.us-south.bluemix.net:9093'
var string2= 'kafka01-prod01.messagehub.services.us-south.bluemix.net:9093'


console.log(helper.hash(string1));
console.log(helper.hash(string2));