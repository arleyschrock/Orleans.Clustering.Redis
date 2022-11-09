﻿using HelloWorld.Interfaces;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace RedisClient
{
    /// <summary>
    /// Orleans test silo client
    /// </summary>
    public class Program
    {
        private static readonly AutoResetEvent Closing = new AutoResetEvent(false);

        static async Task<int> Main(string[] args)
        {
            try
            {
                var client = await StartClientWithRetries();
                await DoClientWork(client);

                Console.CancelKeyPress += OnExit;
                Closing.WaitOne();

                Console.WriteLine("Shutting down...");

                return 0;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return 1;
            }
        }

        private static async Task<IClusterClient> StartClientWithRetries(int initializeAttemptsBeforeFailing = 5)
        {
            int attempt = 0;
            IClusterClient client;
            while (true)
            {
                try
                {
                    var host = new HostBuilder()
                        .ConfigureLogging(logging => logging.AddConsole())
                        .UseOrleansClient((context, cl) =>
                        {
                            cl.Configure<ClusterOptions>(options =>
                                {
                                    options.ClusterId = "testcluster";
                                    options.ServiceId = "testcluster";
                                })
                                .UseRedisClustering(opt =>
                                {
                                    opt.ConnectionString = "localhost:6379";
                                    opt.Database = 0;
                                });
                        }).Build();

                    await host.StartAsync();
                    Console.WriteLine("Client successfully connect to silo host");
                    client = host.Services.GetRequiredService<IClusterClient>();
                    break;
                }
                catch (SiloUnavailableException)
                {
                    attempt++;
                    Console.WriteLine(
                        $"Attempt {attempt} of {initializeAttemptsBeforeFailing} failed to initialize the Orleans client.");
                    if (attempt > initializeAttemptsBeforeFailing)
                    {
                        throw;
                    }

                    await Task.Delay(TimeSpan.FromSeconds(4));
                }
            }

            return client;
        }

        private static async Task DoClientWork(IClusterClient client)
        {
            var friend = client.GetGrain<IHello>(0);
            for (int i = 0; i < 10; i++)
            {
                var response = await friend.SayHello("Good morning, my friend!");
                Console.WriteLine("\n\n{0}\n\n", response);
            }
        }

        private static void OnExit(object sender, ConsoleCancelEventArgs args)
        {
            Closing.Set();
        }
    }
}