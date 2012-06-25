require('./harness');

var received_count = 0;

connection.addListener('ready', function () {
  puts("connected to " + connection.serverProperties.product);

  var exchange = connection.exchange('node-json-fanout', {type:'fanout'});

  var q = connection.queue('node-json-queue', {autoDelete:false}, function () {
    var origMessage1 = {one:1},
      origMessage2 = {three:2},
      origMessage3 = {three:3};
    rejected_count = 0;

    q.bind(exchange, "*");

    q.subscribe({ack:true, prefetchCount:3}, function (json, headers, deliveryInfo, m) {
      received_count++;
      if (received_count === 3) {
        m.nack(true,true)
      }else if (received_count === 6) {
        m.acknowledge(true)
      }
    })
      .addCallback(function () {
        exchange.publish('reject', origMessage1);
        exchange.publish('accept', origMessage2);
        exchange.publish('accept', origMessage3);

        setTimeout(function () {
          // wait one second to receive the message, then quit
          // make sure to delete our queue on our way out.
          q.destroy();
          connection.end();
        }, 1000);
      })
  });
});


process.addListener('exit', function () {
  assert.equal(received_count, 6);
});
