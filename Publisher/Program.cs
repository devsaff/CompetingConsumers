using Microsoft.Azure.ServiceBus;
using System;
using System.Text;
using System.Threading.Tasks;

namespace competingConsumersPub
{
    class Program
    {
        const string ServiceBusConnectionString = "[connection String]";
        const string TopicName = "[topic name]";
        const string QueueName = "TestQueue";
        static ITopicClient topicClient;
        static IQueueClient queueClient;

        public static async Task Main(string[] args)
        {
            queueClient = new QueueClient(ServiceBusConnectionString, QueueName);
            topicClient = new TopicClient(ServiceBusConnectionString, TopicName);

            Console.WriteLine("======================================================");
            Console.WriteLine($"Press ENTER To send a message. ESC to exit.");
            Console.WriteLine("======================================================");

            while (Console.ReadKey().Key != ConsoleKey.Escape)
                await SendMessageAsync();

            await queueClient.CloseAsync();
            await topicClient.CloseAsync();
        }


        static async Task SendMessageAsync()
        {
            try
            {
                string messageBody = $"Message {DateTime.Now}";
                var message = new Message(Encoding.UTF8.GetBytes(messageBody)) { Label = "ProcessCPE" };

                Console.WriteLine($"Sending message: {messageBody}");

                await topicClient.SendAsync(message);
                await queueClient.SendAsync(message);
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }
    }
}
