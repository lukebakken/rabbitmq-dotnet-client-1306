using System.Diagnostics;
using System.Timers;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory()
{
    HostName = "rabbitmq-ssl",
    VirtualHost = "/",
    UserName = "guest",
    Password = "guest",
    ClientProvidedName = "TestClient",
    RequestedHeartbeat = TimeSpan.FromSeconds(10),
    AutomaticRecoveryEnabled = false,
    TopologyRecoveryEnabled = false,
};


factory.Ssl.Enabled = true;
factory.Ssl.ServerName = factory.HostName;
factory.Ssl.Version = System.Security.Authentication.SslProtocols.Tls12;
factory.Ssl.AcceptablePolicyErrors = System.Net.Security.SslPolicyErrors.RemoteCertificateNameMismatch;

ushort currentMax = 1;

IConnection? connection = null;
IModel? channel = null;

const string exchangeName = "MyExchange";
const string queueName = "MyQueue";

bool stopping = false;
var stopEvent = new ManualResetEventSlim();
var channelShutdownEvent = new ManualResetEventSlim();

var handles = new WaitHandle[2];
handles[0] = stopEvent.WaitHandle;
handles[1] = channelShutdownEvent.WaitHandle;

try
{
    connection = factory.CreateConnection();
    channel = CreateModel(connection);
    connection.CallbackException += OnConnectionCallbackException;
    connection.ConnectionBlocked += OnConnectionBlocked;
    connection.ConnectionUnblocked += OnConnectionUnblocked;
    connection.ConnectionUnblocked += OnConnectionShutdown;

    DeclareEntities(channel, queueName);

    var consumerTag = StartConsuming(channel, queueName);

    Console.CancelKeyPress += (object? sender, ConsoleCancelEventArgs e) =>
    {
        Console.WriteLine("[INFO] CTRL-C pressed, exiting!");
        stopEvent.Set();
        e.Cancel = true;
    };

    Task.Run(() =>
    {
        // Update prefetch after 7 seconds
        currentMax = 4;
        Thread.Sleep(7000);
        channel.BasicQos(0, currentMax, false);
        Console.WriteLine($"Prefetch set to {currentMax}");

        // Need to cancel consumer and create again in order to work

        //channel.BasicCancel(consumerTag);
        //consumerTag = StartConsuming(channel, queueName);
    });

    while (!stopping)
    {
        int i = WaitHandle.WaitAny(handles);
        switch (i)
        {
            case 0:
                // stopEvent was set
                stopping = true;
                break;
            case 1:
                // channelShutdown was set
                Console.WriteLine("[INFO] re-starting channel and consumer...");
                channel = CreateModel(connection);
                DeclareEntities(channel, queueName);
                consumerTag = StartConsuming(channel, queueName);
                break;
            default:
                continue;
        }
    }
}
finally
{
    Console.WriteLine("[INFO] disposing resources on exit...");
    channel?.Close();
    channel?.Dispose();
    connection?.Close();
    connection?.Dispose();
}

void DeclareEntities(IModel channel, string queue)
{
    var arguments = new Dictionary<string, object> {
    { "x-queue-type", "quorum" },
    };
    channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Direct, durable: true);
    channel.QueueDeclare(queue: queue, durable: true, exclusive: false, autoDelete: false, arguments: arguments);
    channel.QueueBind(queue: queue, exchange: exchangeName, routingKey: queue);
}

IModel CreateModel(IConnection connection)
{
    channelShutdownEvent.Reset();
    var ch = connection.CreateModel();
    ch.BasicQos(0, currentMax, false);
    ch.ModelShutdown += OnModelShutdown;
    ch.CallbackException += OnModelCallbackException;
    ch.BasicAcks += OnModelBasicAcks;
    ch.BasicNacks += OnModelBasicNacks;
    ch.BasicReturn += OnModelBasicReturn;
    ch.FlowControl += OnModelFlowControl;
    return ch;
}

void OnConnectionShutdown(object? sender, EventArgs e)
{
    Console.WriteLine("[INFO] connection shutdown");
}

void OnConnectionUnblocked(object? sender, EventArgs e)
{
    Console.WriteLine("[INFO] connection unblocked");
}

void OnConnectionBlocked(object? sender, ConnectionBlockedEventArgs e)
{
    Console.WriteLine($"[INFO] connection blocked, IsOpen: {connection.IsOpen}");
}

void OnConnectionCallbackException(object? sender, CallbackExceptionEventArgs e)
{
    Console.Error.WriteLine($"[ERROR] connection callback exception: {e.Exception.Message}");
}

void OnModelShutdown(object? sender, ShutdownEventArgs e)
{
    Console.WriteLine($"[INFO] channel shutdown: {e.ReplyCode} - {e.ReplyText}");
    channelShutdownEvent.Set();
}

void OnModelCallbackException(object? sender, CallbackExceptionEventArgs e)
{
    Console.Error.WriteLine($"[ERROR] channel callback exception: {e.Exception.Message}");
}

void OnModelBasicAcks(object? sender, BasicAckEventArgs e)
{
    Console.WriteLine($"Channel saw basic.ack, delivery tag: {e.DeliveryTag} multiple: {e.Multiple}");
}

void OnModelBasicNacks(object? sender, BasicNackEventArgs e)
{
    Console.WriteLine($"Channel saw basic.nack, delivery tag: {e.DeliveryTag} multiple: {e.Multiple}");
}

void OnModelBasicReturn(object? sender, BasicReturnEventArgs e)
{
    Console.WriteLine($"Channel saw basic.return {e.ReplyCode} - {e.ReplyText}");
}

void OnModelFlowControl(object? sender, FlowControlEventArgs e)
{
    Console.WriteLine($"Channel saw flow control, active: {e.Active}");
}

string StartConsuming(IModel channel, string queueName)
{
    Console.WriteLine($"[INFO] consumer is starting, queueName: {queueName}");
    var consumer = new EventingBasicConsumer(channel);
    consumer.Received += (objConsumer, ea) =>
    {
        if (stopEvent.IsSet)
        {
            Console.Error.WriteLine("[WARNING] consumer received message while cancellation requested!");
            channel.BasicCancel(consumer.ConsumerTags[0]);
            return;
        }

        Console.WriteLine($"[INFO][{Environment.CurrentManagedThreadId}][{ea.ConsumerTag}][{DateTime.Now:HH:mm:ss.fff}] consuming message, deliveryTag: {ea.DeliveryTag}, redelivered: {ea.Redelivered}.");

        // Consume in parallel
        Task.Run(() =>
        {
            Thread.Sleep(5000);

            try
            {
                if(objConsumer is EventingBasicConsumer c)
                {
                    c.Model.BasicAck(ea.DeliveryTag, false);
                    Console.WriteLine($"[INFO][{Environment.CurrentManagedThreadId}][{ea.ConsumerTag}][{DateTime.Now:HH:mm:ss.fff}] acked message {ea.DeliveryTag}");
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ERROR][{Environment.CurrentManagedThreadId}][{ea.ConsumerTag}][{DateTime.Now:HH:mm:ss.fff}] Error confirming message {ea.DeliveryTag}, ex: {ex.Message}");
            }
        })
        .ConfigureAwait(false);
    };

    consumer.Unregistered += OnConsumerUnregistered;
    consumer.ConsumerCancelled += OnConsumerCancelled;
    consumer.Shutdown += OnConsumerShutdown;

    var consumerTag = channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);
    Console.WriteLine($"[INFO] consumer started, consumerTag: {consumerTag}");
    return consumerTag;
}

void OnConsumerUnregistered(object? sender, ConsumerEventArgs e)
{
    Console.WriteLine($"[INFO] consumer unregistered, consumer tag: {e.ConsumerTags[0]}");
}

void OnConsumerCancelled(object? sender, ConsumerEventArgs e)
{
    Console.WriteLine($"[INFO] consumer cancelled, consumer tag: {e.ConsumerTags[0]}");
}

void OnConsumerShutdown(object? sender, ShutdownEventArgs e)
{
    Console.WriteLine($"[INFO] consumer shutdown {e.ReplyCode} - {e.ReplyText}");
}
