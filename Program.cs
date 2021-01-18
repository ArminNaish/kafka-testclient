using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // https://github.com/confluentinc/confluent-kafka-dotnet

            // Messages in Kafka are always persistent and kept in the "log" base on log.retention and log.cleanup settings of the topic

            // Confluent Kafka has integrated retries -> we don't need polly!!
            // Confluent ensures the correct order of messages

            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                MessageSendMaxRetries = 5,
                EnableIdempotence = true,
                Acks = Acks.All,
            };

            var payload = JsonSerializer.Serialize(new { Time = DateTime.Now });

            using var producer = new Producer<string, string>(config);
            await producer.ProduceAsync(
                topic: "test",
                key: Guid.NewGuid().ToString(),
                value: payload,
                headers: new Dictionary<string, string>
                {
                    ["correlation-id"] = Guid.NewGuid().ToString()
                }
            );
        }
    }
}
