using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;

using static System.Text.Encoding;

namespace KafkaApp
{
    public class Producer<TKey, TValue> : IDisposable
    {
        private readonly ProducerConfig _config;
        private readonly IProducer<TKey, TValue> _producer;

        private bool _disposed = false;

        public Producer(ProducerConfig config)
        {
            _config = config;
            _producer = BuildProducer(config);
        }

        public async Task ProduceAsync(string topic, TKey key, TValue value, IDictionary<string, string> headers = null)
        {
            try
            {
                var message = new Message<TKey, TValue>
                {
                    Key = key,
                    Value = value,
                    Headers = headers is not null ? MakeHeaders(headers) : null
                };

                // Encapsulates the result of a successful produce request.
                var deliveryResult = await _producer.ProduceAsync(topic, message);
                Console.WriteLine($"Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'");
            }
            catch (ProduceException<TKey, TValue> ex)
            {
                Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
            }
        }

        private IProducer<TKey, TValue> BuildProducer(ProducerConfig config)
        {
            var builder = new ProducerBuilder<TKey, TValue>(config);
            return builder.Build();
        }

        private static Headers MakeHeaders(IDictionary<string, string> dict)
        {
            var headers = new Headers();
            foreach (var (key, value) in dict)
                headers.Add(key, UTF8.GetBytes(value));
            return headers;
        }


        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                    _producer?.Dispose();
                _disposed = true;
            }
        }
    }
}