
var cassandra = require('cassandra-driver');
var clientCassandra = new cassandra.Client({contactPoints: ['localhost:9042'], keyspace: 'demo'});

var kafka = require('kafka-node');
var client = new kafka.Client("localhost:2181/");
var Consumer = kafka.Consumer;
var consumer = new Consumer(
   client,
   [
       { topic: 'twitter', partition: 0 }
   ],
   {
       autoCommit: false
   }
);
consumer.on('message', function (message) {
 console.log(message);
 var twitterJSON=message.value;
 var twitterJSONstring=JSON.stringify(twitterJSON);
 console.log('consumer received data');

var yourQuery = "INSERT INTO demo.twitter_table2 JSON '"+twitterJSON +"'";
    clientCassandra.execute(yourQuery, function(err, result){
    	if (!err){
               

               console.log(" data added");

           }
		   else{
			   console.log("details not added");
		   }
    });


});