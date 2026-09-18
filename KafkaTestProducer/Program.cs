using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System.Text.Json;


string BootstrapServers = "localhost:9092";

string Topic = "demo-topic";

await EnsureTopicAsync();

    var config = new ProducerConfig
    {
        BootstrapServers = BootstrapServers,
        Acks = Acks.All,              // подтверждение от всех ISR
        EnableIdempotence = true,     // безопаснее для acks=all
        MessageTimeoutMs = 5000,
        LingerMs = 5
    };

    using var producer = new ProducerBuilder<Null, string>(config).Build();
    Console.WriteLine($"Продюсер запущен. Отправка в '{Topic}' каждые 2 секунды. Ctrl+C — выход.");

    var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

    var rnd = new Random();

    try
    {
        while (!cts.IsCancellationRequested)
        {
            var message = new MessagePayload
            {
                Id = Guid.NewGuid().ToString(),
                Timestamp = DateTimeOffset.UtcNow,
                Payload = $"hello-{rnd.Next(1000, 9999)}"
            };

            string json = JsonSerializer.Serialize(message);

            try
            {
                var result = await producer.ProduceAsync(
                    Topic,
                    new Message<Null, string> { Value = json });

                Console.WriteLine(
                    $"[SENT] id={message.Id} payload={message.Payload} " +
                    $"→ partition={result.Partition.Value} offset={result.Offset.Value}");
            }
            catch (ProduceException<Null, string> ex)
            {
                Console.WriteLine($"[ERROR] {ex.Error.Reason}");
            }

            try { await Task.Delay(TimeSpan.FromSeconds(2), cts.Token); }
            catch (TaskCanceledException) { break; }
        }
    }
    finally
    {
        producer.Flush(TimeSpan.FromSeconds(5));
        Console.WriteLine("Продюсер остановлен.");
    }

// Явное создание топика с 1 партицией
async Task EnsureTopicAsync()
{
    var adminConfig = new AdminClientConfig { BootstrapServers = BootstrapServers };
    using var admin = new AdminClientBuilder(adminConfig).Build();

    try
    {
        await admin.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = Topic,
                NumPartitions = 1,
                ReplicationFactor = 1
            }
        });
        Console.WriteLine($"Топик '{Topic}' создан (1 партиция).");
    }
    catch (CreateTopicsException ex)
    {
        if (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
            Console.WriteLine($"Топик '{Topic}' уже существует.");
        else
            throw;
    }
}
public class MessagePayload
{
    public string Id { get; set; } = default!;
    public DateTimeOffset Timestamp { get; set; }
    public string Payload { get; set; } = default!;
}