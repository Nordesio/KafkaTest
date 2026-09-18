using Confluent.Kafka;
using System.Text.Json;


string BootstrapServers = "localhost:9092";
string Topic = "demo-topic";

// Позволяет запускать несколько копий: dotnet run -- group-A
Console.WriteLine("Введите id группы");
string groupId = "demo-group-" + Console.ReadLine();
Console.WriteLine(groupId);
var config = new ConsumerConfig
{
    BootstrapServers = BootstrapServers,
    GroupId = groupId,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,       // ручной commit
    EnableAutoOffsetStore = false   // коммитим только после обработки
};

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

using var consumer = new ConsumerBuilder<Null, string>(config).Build();
consumer.Subscribe(Topic);

Console.WriteLine($"Consumer запущен. group.id={groupId}, topic='{Topic}'. Ctrl+C — выход.");

try
{
    while (!cts.IsCancellationRequested)
    {
        try
        {
            var cr = consumer.Consume(cts.Token);

            MessagePayload? payload = null;
            try
            {
                payload = JsonSerializer.Deserialize<MessagePayload>(cr.Message.Value);
            }
            catch (JsonException)
            {
                Console.WriteLine($"[WARN] Не удалось распарсить JSON: {cr.Message.Value}");
            }

            // Здесь была бы ваша бизнес-логика
            Console.WriteLine(
                $"[RECV] id={payload?.Id} payload={payload?.Payload} " +
                $"partition={cr.Partition.Value} offset={cr.Offset.Value}");

            // Ручной commit после успешной обработки
            consumer.Commit(cr);
        }
        catch (ConsumeException ex)
        {
            Console.WriteLine($"[ERROR] {ex.Error.Reason}");
        }
    }
}
catch (OperationCanceledException) { }
finally
{
    consumer.Close(); // фиксирует позицию и выходит из группы
    Console.WriteLine("Consumer остановлен.");
}
public class MessagePayload
{
    public string Id { get; set; } = default!;
    public DateTimeOffset Timestamp { get; set; }
    public string Payload { get; set; } = default!;
}