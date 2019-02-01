using Newtonsoft.Json;

namespace Orleans.Clustering.Redis
{
    internal class VersionedEntry
    {
        public MembershipEntry Entry { get; set; }
        public int Version { get; set; }
        public string VersionEtag { get; set; }

        public VersionedEntry(MembershipEntry entry, TableVersion rowVersion)
        {
            Entry = entry;
            this.Version = rowVersion.Version;
            this.VersionEtag = rowVersion.VersionEtag;
        }

        public TableVersion GetVersion()
        {
            return new TableVersion(this.Version, this.VersionEtag);
        }

        public VersionedEntry()
        {

        }
    }
}