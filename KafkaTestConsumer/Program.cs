using Confluent.Kafka;
using System;
using System.Threading;

// 1. Конфигурация консьюмера
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "test-consumer-group", // Идентификатор группы потребителей
    AutoOffsetReset = AutoOffsetReset.Earliest // Читать с самого начала, если нет сохранённого offset
};

const string topic = "test-topic";

// 2. Механизм graceful shutdown по Ctrl+C
CancellationTokenSource cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // Отменяем завершение процесса по умолчанию
    cts.Cancel();
    Console.WriteLine("Завершение работы консьюмера...");
};

// 3. Создание консьюмера и подписка
using (var consumer = new ConsumerBuilder<Null, string>(config).Build())
{
    consumer.Subscribe(topic);
    Console.WriteLine($"Ожидание сообщений из топика '{topic}' (нажмите Ctrl+C для выхода)...");

    try
    {
        while (!cts.IsCancellationRequested)
        {
            try
            {
                // 4. Чтение сообщения
                var consumeResult = consumer.Consume(cts.Token);
                Console.WriteLine($"[ПОЛУЧЕНО] {consumeResult.Message.Value}");
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"[ОШИБКА] Ошибка потребления: {e.Error.Reason}");
            }
        }
    }
    catch (OperationCanceledException)
    {
        // Ожидаемое исключение при нажатии Ctrl+C
    }
    finally
    {
        // 5. Корректное закрытие консьюмера (фиксация offset)
        consumer.Close();
        Console.WriteLine("Консьюмер остановлен.");
    }
}