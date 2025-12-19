using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

var factory = new ConnectionFactory
{
    HostName = "localhost"
};

using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

// ===== Nombres =====
var mainExchange = "pedidos.exchange";
var retryExchange = "pedidos.retry";
var dlxExchange = "pedidos.dlx";

var mainQueue = "cola-pagos";
var retryQueue = "cola-pagos-retry";
var dlqQueue = "cola-pagos-dlq";

// ===== Exchanges =====
channel.ExchangeDeclare(mainExchange, ExchangeType.Topic, durable: true);
channel.ExchangeDeclare(retryExchange, ExchangeType.Direct, durable: true);
channel.ExchangeDeclare(dlxExchange, ExchangeType.Direct, durable: true);

// ===== DLQ =====
channel.QueueDeclare(
    queue: dlqQueue,
    durable: true,
    exclusive: false,
    autoDelete: false
);

channel.QueueBind(
    queue: dlqQueue,
    exchange: dlxExchange,
    routingKey: "pagos.error"
);

// ===== Retry Queue (TTL) =====
var retryQueueArgs = new Dictionary<string, object>
{
    { "x-message-ttl", 5000 }, // 5 segundos
    { "x-dead-letter-exchange", mainExchange },
    { "x-dead-letter-routing-key", "pedido.creado" }
};

channel.QueueDeclare(
    queue: retryQueue,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: retryQueueArgs
);

channel.QueueBind(
    queue: retryQueue,
    exchange: retryExchange,
    routingKey: "retry"
);

// ===== Cola principal =====
var mainQueueArgs = new Dictionary<string, object>
{
    { "x-dead-letter-exchange", retryExchange },
    { "x-dead-letter-routing-key", "retry" }
};

channel.QueueDeclare(
    queue: mainQueue,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: mainQueueArgs
);

channel.QueueBind(
    queue: mainQueue,
    exchange: mainExchange,
    routingKey: "pedido.creado"
);

// ===== QoS =====
channel.BasicQos(0, 1, false);

// ===== Consumer =====
var consumer = new EventingBasicConsumer(channel);

int maxRetries = 3;

consumer.Received += (sender, ea) =>
{
    var message = Encoding.UTF8.GetString(ea.Body.ToArray());
    Console.WriteLine($"[Pagos] Procesando: {message}");

    int retryCount = 0;

    if (ea.BasicProperties.Headers != null &&
        ea.BasicProperties.Headers.TryGetValue("x-death", out var deathHeader))
    {
        var deaths = deathHeader as IList<object>;
        retryCount = deaths?.Count ?? 0;
    }

    try
    {
        // Simular error
        throw new Exception("Error simulado en pago");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[ERROR] {ex.Message}");

        if (retryCount >= maxRetries)
        {
            Console.WriteLine("Máximo de reintentos alcanzado → DLQ");

            channel.BasicPublish(
                exchange: dlxExchange,
                routingKey: "pagos.error",
                basicProperties: ea.BasicProperties,
                body: ea.Body
            );

            channel.BasicAck(ea.DeliveryTag, false);
        }
        else
        {
            Console.WriteLine($"Reintentando ({retryCount + 1}/{maxRetries})");

            channel.BasicNack(
                deliveryTag: ea.DeliveryTag,
                multiple: false,
                requeue: false
            );
        }
    }
};

channel.BasicConsume(
    queue: mainQueue,
    autoAck: false,
    consumer: consumer
);

Console.WriteLine("Servicio de Pagos activo (Retry + DLQ)...");
Console.ReadLine();
