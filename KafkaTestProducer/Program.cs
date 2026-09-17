using Confluent.Kafka;
using System;

// 1. Конфигурация продюсера
var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092", // Адрес брокера
    // Для теста можно оставить минимальные настройки.
    Acks = Acks.All, 
    EnableIdempotence = true
};

const string topic = "test-topic";

// 2. Создание продюсера
using (var producer = new ProducerBuilder<Null, string>(config).Build())
{
    Console.WriteLine("Введите сообщения для отправки в Kafka (или 'exit' для выхода):");
    string? input;
    while ((input = Console.ReadLine()) != "exit")
    {
        if (string.IsNullOrWhiteSpace(input)) continue;

        try
        {
            // 3. Отправка сообщения
            var deliveryResult = await producer.ProduceAsync(
                topic,
                new Message<Null, string> { Value = input }
            );

            Console.WriteLine($"[OK] Отправлено в {deliveryResult.TopicPartitionOffset}: {deliveryResult.Value}");
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"[ОШИБКА] Не удалось доставить: {e.Error.Reason}");
        }
    }

    // 4. Дожидаемся отправки всех сообщений
    producer.Flush(TimeSpan.FromSeconds(5));
    Console.WriteLine("Продюсер завершил работу.");
}
