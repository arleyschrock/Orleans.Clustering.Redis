using HelloWorld.Grains;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Clustering.Redis;
using Orleans.Configuration;
using Orleans.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace RedisGatewayHost
{
    public class Program
    {
        private static readonly AutoResetEvent Closing = new AutoResetEvent(false);

        public static async Task<int> Main(string[] args)
        {
            try
            {
                var host = await StartSilo();
                Console.WriteLine("Silo is ready!");

                Console.CancelKeyPress += OnExit;
                Closing.WaitOne();

                Console.WriteLine("Shutting down...");

                await host.StopAsync();

                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return 1;
            }
        }

        private static async Task<IHost> StartSilo()
        {
            var builder = new HostBuilder()
                .UseOrleans((context, siloBuilder) =>
                {
                    siloBuilder.Configure<ClusterOptions>(options =>
                        {
                            options.ClusterId = "testcluster";
                            options.ServiceId = "testcluster";
                        })
                        .ConfigureEndpoints(new Random(1).Next(30001, 30100), new Random(1).Next(20001, 20100),
                            listenOnAnyHostAddress: true)
                        .AddMemoryGrainStorageAsDefault()
                        .UseRedisClustering(opt =>
                        {
                            opt.ConnectionString = "localhost:6379";
                            opt.Database = 0;
                        })
                        .ConfigureLogging(logging => logging.AddConsole());
                });

            var host = builder.Build();
            await host.StartAsync();
            return host;
        }

        private static void OnExit(object sender, ConsoleCancelEventArgs args)
        {
            Closing.Set();
        }
    }
}