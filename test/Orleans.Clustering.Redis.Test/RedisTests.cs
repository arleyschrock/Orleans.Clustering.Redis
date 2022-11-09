﻿using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Clustering.Redis;
using Orleans.Clustering.Redis.Test;
using Orleans.Configuration;
using Orleans.Messaging;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

// <summary>
// Tests for operation of Orleans Membership Table using Redis
// </summary>
// 
public class RedisTests : MembershipTableTestsBase, IClassFixture<MultiplexerFixture>
{
    public RedisTests(ITestOutputHelper output, MultiplexerFixture multiplexerFixture) : base(output, multiplexerFixture, CreateFilters())
    {

    }

    private static LoggerFilterOptions CreateFilters()
    {
        var filters = new LoggerFilterOptions();
        return filters;
    }

    protected override IMembershipTable CreateMembershipTable(ILoggerFactory loggerFactory)
    {
        return new RedisMembershipTable(Options.Create(multiplexerFixture.DatabaseOptions), Options.Create(new ClusterOptions { ClusterId = this.clusterId, ServiceId = this.serviceId }));
    }

    protected override IGatewayListProvider CreateGatewayListProvider(IMembershipTable membershipTable, ILoggerFactory logger)
    {
        return new RedisGatewayListProvider((RedisMembershipTable)membershipTable, Options.Create(new GatewayOptions()));
    }

    protected override Task<string> GetConnectionString()
    {
        return Task.FromResult("");
    }

    [Fact]
    public async Task GetGateways()
    {
        await MembershipTable_GetGateways();
    }

    [Fact]
    public async Task ReadAll_EmptyTable()
    {
        await MembershipTable_ReadAll_EmptyTable();
    }

    [Fact]
    public async Task InsertRow()
    {
        await MembershipTable_InsertRow();
    }

    [Fact(Skip = "Something unclear")]
    public async Task ReadRow_Insert_Read()
    {
        await MembershipTable_ReadRow_Insert_Read();
    }

    [Fact(Skip = "Something unclear")]
    public async Task ReadAll_Insert_ReadAll()
    {
        await MembershipTable_ReadAll_Insert_ReadAll();
    }

    [Fact]
    public async Task UpdateRow()
    {
        await MembershipTable_UpdateRow();
    }

    [Fact]
    public async Task UpdateIAmAlive()
    {
        await MembershipTable_UpdateIAmAlive();
    }
}