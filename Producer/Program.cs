using RabbitMQ.Client;
using System.Text;

var factory = new ConnectionFactory
{
    HostName = "localhost"
};

using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

var exchangeName = "pedidos.exchange";

// Declarar exchange
channel.ExchangeDeclare(
    exchange: exchangeName,
    type: ExchangeType.Topic,
    durable: true
);

var message = "Pedido creado: ID 123";
var body = Encoding.UTF8.GetBytes(message);

// Mensaje persistente
var properties = channel.CreateBasicProperties();
properties.Persistent = true;

// Routing key
var routingKey = "pedido.creado";

channel.BasicPublish(
    exchange: exchangeName,
    routingKey: routingKey,
    basicProperties: properties,
    body: body
);

Console.WriteLine("Evento publicado: pedido.creado");
Console.ReadLine();
