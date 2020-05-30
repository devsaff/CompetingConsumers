using Microsoft.Azure.ServiceBus;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Subscriber
{
    class Program
    {
        const string ServiceBusConnectionString = "[connection String]";
        const string TopicName = "[topic name]";
        const string QueueName = "TestQueue";
        const string SubscriptionName = "catch_all";

        static ISubscriptionClient subscriptionClient;
        static IQueueClient queueClient;

        static bool UseQueue = true;

        static async Task Main(string[] args)
        {
            if (UseQueue)
            {
                queueClient = new QueueClient(ServiceBusConnectionString, QueueName)
                {
                    PrefetchCount = 0
                };
            }
            else
            {
                subscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName)
                {
                    PrefetchCount = 0
                };
            }

            string type = UseQueue ? "QUEUE" : "SUBSCRIPTION";

            Console.WriteLine("======================================================");
            Console.WriteLine($"LISTENING ({type}) . Press ENTER key to exit");
            Console.WriteLine("======================================================");

            RegisterOnMessageHandlerAndReceiveMessages();

            Console.ReadKey();
            if (UseQueue)
                await queueClient.CloseAsync();
            else
                await subscriptionClient.CloseAsync();
        }

        static void RegisterOnMessageHandlerAndReceiveMessages()
        {
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 10,
                AutoComplete = false
            };

            if (UseQueue)
                queueClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
            else
                subscriptionClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");
            await Task.Delay(1000);

            if (UseQueue)
                await queueClient.CompleteAsync(message.SystemProperties.LockToken);
            else
                await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
        }

        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }
    }
}

