using Confluent.Kafka;
using Confluent.Kafka.Admin;

class Program
{
    static async Task Main(string[] args)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = "localhost:9092"
        };

        const string topicName = "demo-topic";

        using var adminClient = new AdminClientBuilder(config).Build();

        // 1. Удаляем существующий топик (если есть)
        try
        {
            await adminClient.DeleteTopicsAsync(new[] { topicName });
            Console.WriteLine($"Топик '{topicName}' удалён. Ждём, пока Kafka применит изменения...");

            // Kafka удаляет топик асинхронно — нужно подождать,
            // иначе создание с тем же именем упадёт с ошибкой.
            await WaitUntilTopicDeleted(adminClient, topicName, TimeSpan.FromSeconds(30));
            Console.WriteLine("Удаление подтверждено.");
        }
        catch (DeleteTopicsException ex)
        {
            var err = ex.Results.FirstOrDefault()?.Error;
            if (err?.Code == ErrorCode.UnknownTopicOrPart)
            {
                Console.WriteLine($"Топик '{topicName}' не существует — пропускаем удаление.");
            }
            else
            {
                Console.WriteLine($"Ошибка удаления топика: {err?.Reason}");
                throw;
            }
        }

        // 2. Небольшая пауза — метаданные должны разойтись по брокеру
        await Task.Delay(2000);

        // 3. Создаём топик заново с 3 партициями
        try
        {
            await adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = 3,
                    ReplicationFactor = 1   // для локального однонодового Kafka
                }
            });

            Console.WriteLine($"Топик '{topicName}' создан с 3 партициями.");
        }
        catch (CreateTopicsException ex)
        {
            var err = ex.Results.FirstOrDefault()?.Error;
            if (err?.Code == ErrorCode.TopicAlreadyExists)
            {
                Console.WriteLine($"Топик '{topicName}' уже существует.");
            }
            else
            {
                Console.WriteLine($"Ошибка создания топика: {err?.Reason}");
                throw;
            }
        }

        // 4. Проверяем результат — выводим список партиций
        await Task.Delay(1000);
        var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
        var topicMeta = metadata.Topics.First(t => t.Topic == topicName);

        Console.WriteLine("\n--- Итоговая конфигурация ---");
        Console.WriteLine($"Топик: {topicMeta.Topic}");
        Console.WriteLine($"Партиций: {topicMeta.Partitions.Count}");
        foreach (var p in topicMeta.Partitions)
        {
            Console.WriteLine($"  Partition {p.PartitionId}, Leader: {p.Leader}");
        }
    }

    /// <summary>
    /// Ждём, пока Kafka реально удалит топик (проверяем по метаданным).
    /// </summary>
    static async Task WaitUntilTopicDeleted(IAdminClient adminClient, string topic, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
                var exists = metadata.Topics.Any(t => t.Topic == topic);
                if (!exists) return;
            }
            catch
            {
                // при перебалансировке метаданных могут быть кратковременные ошибки — игнорируем
            }

            await Task.Delay(1000);
        }

        throw new TimeoutException($"Топик '{topic}' не был удалён за {timeout.TotalSeconds} сек.");
    }
}