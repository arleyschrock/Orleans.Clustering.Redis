using System;
using System.Threading.Tasks;
using Orleans.Runtime;
using StackExchange.Redis;
using Orleans.Configuration;
using Newtonsoft.Json;
using System.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;
using System.Collections.Generic;

[assembly: InternalsVisibleTo("Orleans.Clustering.Redis.Test")]

namespace Orleans.Clustering.Redis
{
    internal class RedisMembershipTable : IMembershipTable
    {
        private readonly IDatabase _db;
        private readonly RedisOptions _redisOptions;
        private readonly ClusterOptions _clusterOptions;
        public ILoggerFactory LoggerFactory { get; }
        public ILogger Logger { get; }
        private RedisKey ClusterKey => $"{_clusterOptions.ClusterId}";
        private RedisKey ClusterKeyEtag => $"{_clusterOptions.ClusterId}_ETag";
        private RedisKey ClusterVersionKey => $"{_clusterOptions.ClusterId}_Version";
        private RedisKey ClusterVersionEtagKey => $"{_clusterOptions.ClusterId}_VersionEtag";


        public RedisMembershipTable(IConnectionMultiplexer multiplexer, IOptions<RedisOptions> redisOptions, IOptions<ClusterOptions> clusterOptions, ILoggerFactory loggerFactory)
        {
            _redisOptions = redisOptions.Value;
            _db = multiplexer.GetDatabase(_redisOptions.Database);
            _clusterOptions = clusterOptions.Value;
            LoggerFactory = loggerFactory;
            Logger = loggerFactory?.CreateLogger<RedisMembershipTable>();
            Logger?.LogInformation("In RedisMembershipTable constructor");
        }

        public async Task DeleteMembershipTableEntries(string clusterId)
        {
            var batch = _db.CreateBatch();
            List<Task> batL = new List<Task>();
            batL.Add(batch.KeyDeleteAsync(ClusterKey));
            batL.Add(batch.KeyDeleteAsync(ClusterKeyEtag));
            batL.Add(batch.KeyDeleteAsync(ClusterVersionKey));
            batL.Add(batch.KeyDeleteAsync(ClusterVersionEtagKey));
            batch.Execute();

            await Task.WhenAll(batL);
        }

        public async Task InitializeMembershipTable(bool tryInitTableVersion)
        {
            Logger?.Debug($"{nameof(InitializeMembershipTable)}: {tryInitTableVersion}");

            if (tryInitTableVersion)
            {
                var trn = _db.CreateTransaction();
                List<Task> trnT = new List<Task>();
                trn.AddCondition(Condition.KeyNotExists(ClusterVersionEtagKey));
                trnT.Add(trn.StringSetAsync(ClusterVersionKey, 0));
                trnT.Add(trn.StringSetAsync(ClusterVersionEtagKey, Guid.NewGuid().ToString()));
                await trn.ExecuteAsync();
            }

            await Task.CompletedTask;
        }

        public Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            return UpdateRow(entry, "", tableVersion);
        }

        public async Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            Logger?.Debug($"{nameof(InsertRow)}: {Serialize(entry)}, {Serialize(tableVersion)}");

            var newVersion = new TableVersion(tableVersion.Version, Guid.NewGuid().ToString());
            var newEntry = (RedisValue)Serialize(new VersionedEntry(entry, newVersion));

            var trn = _db.CreateTransaction();
            List<Task> trnT = new List<Task>();
            trn.AddCondition(Condition.StringEqual(ClusterVersionEtagKey, tableVersion.VersionEtag));
            if (!string.IsNullOrEmpty(etag))
            {
                trn.AddCondition(Condition.HashEqual(ClusterKeyEtag, entry.SiloAddress.ToString(), etag));
            }
            else
            {
                trn.AddCondition(Condition.HashNotExists(ClusterKeyEtag, entry.SiloAddress.ToString()));
            }
            trnT.Add(trn.StringSetAsync(ClusterVersionKey, newVersion.Version));
            trnT.Add(trn.StringSetAsync(ClusterVersionEtagKey, newVersion.VersionEtag));
            trnT.Add(trn.HashSetAsync(ClusterKey, entry.SiloAddress.ToString(), newEntry));
            trnT.Add(trn.HashSetAsync(ClusterKeyEtag, entry.SiloAddress.ToString(), newVersion.VersionEtag));
            var res = await trn.ExecuteAsync();
            return res;
        }

        public async Task<MembershipTableData> ReadRow(SiloAddress key)
        {
            var batch = _db.CreateBatch();
            var _vesrionId = batch.StringGetAsync(ClusterVersionKey);
            var _vesrionEtag = batch.StringGetAsync(ClusterVersionEtagKey);
            var _row = batch.HashGetAsync(ClusterKey, key.ToString());
            batch.Execute();

            var TableVersion = new TableVersion((int)await _vesrionId, await _vesrionEtag);
            var rows = new List<Tuple<MembershipEntry, string>>();
            var rowJson = await _row;
            if (!rowJson.IsNullOrEmpty)
            {
                var ent = Deserialize<VersionedEntry>(rowJson);
                rows.Add(Tuple.Create(ent.Entry, ent.GetVersion().VersionEtag));
            }

            if (rows.Count == 0)
            {
                return new MembershipTableData(TableVersion);
            }

            return new MembershipTableData(rows.ToList(), TableVersion);
        }

        private string Serialize<T>(T value)
        {
            return JsonConvert.SerializeObject(value,
                new IPEndPointJsonConverter(), new SiloAddressJsonConverter());
        }

        private T Deserialize<T>(string json)
        {
            return JsonConvert.DeserializeObject<T>(json,
                new IPEndPointJsonConverter(), new SiloAddressJsonConverter());
        }


        public async Task<MembershipTableData> ReadAll()
        {

            var batch = _db.CreateBatch();
            var _vesrionId = batch.StringGetAsync(ClusterVersionKey);
            var _vesrionEtag = batch.StringGetAsync(ClusterVersionEtagKey);
            var _rows = batch.HashGetAllAsync(ClusterKey);
            batch.Execute();
            List<SiloAddress> Dead = new List<SiloAddress>();
            var TableVersion = new TableVersion((int)await _vesrionId, await _vesrionEtag);
            var rows = (await _rows).Select(x =>
            {
                var ent = Deserialize<VersionedEntry>(x.Value);
                if (ent.Entry.Status == SiloStatus.Dead && (DateTime.UtcNow - ent.Entry.IAmAliveTime).TotalMinutes > 5)
                {
                    Dead.Add(ent.Entry.SiloAddress);
                    return null;
                }

                return Tuple.Create(ent.Entry, ent.GetVersion().VersionEtag);
            }).Where(x => x != null).ToList();
            // Drop Long Dead Silos, From Redis
            await Task.WhenAll(Dead.Select(x => _db.HashDeleteAsync(this.ClusterKey, (RedisValue)x.ToString())));

            if (rows.Count == 0)
            {
                return new MembershipTableData(TableVersion);
            }

            return new MembershipTableData(rows.ToList(), TableVersion);
        }



        public async Task UpdateIAmAlive(MembershipEntry _entry)
        {
            var row = await _db.HashGetAsync(ClusterKey, _entry.SiloAddress.ToString());
            if (row.IsNull) return;
            var entry = Deserialize<VersionedEntry>(row);
            entry.Entry.IAmAliveTime = _entry.IAmAliveTime;
            var trn = _db.CreateTransaction();
            List<Task> trnT = new List<Task>();
            trn.AddCondition(Condition.HashEqual(ClusterKeyEtag, entry.Entry.SiloAddress.ToString(), entry.GetVersion().VersionEtag));
            trnT.Add(trn.HashSetAsync(ClusterKey, entry.Entry.SiloAddress.ToString(), Serialize(entry)));
            await trn.ExecuteAsync();
        }


    }
}