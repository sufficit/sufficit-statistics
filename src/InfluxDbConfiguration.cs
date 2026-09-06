using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Sufficit.Statistics
{
    /// <summary>
    /// InfluxDB configuration options
    /// </summary>
    public class InfluxDbConfiguration
    {
        public const string SECTIONNAME = "Sufficit:Statistics:InfluxDB";

        /// <summary>
        /// InfluxDB server URL (e.g., "http://influx.sufficit.com.br:8086")
        /// </summary>
        public string Url { get; set; } = string.Empty;

        /// <summary>
        /// API token for authentication with InfluxDB v3
        /// </summary>
        public string Token { get; set; } = string.Empty;

        /// <summary>
        /// Database name in InfluxDB v3 (equivalent to bucket in v2)
        /// </summary>
        public string Database { get; set; } = "sufficit";

        /// <summary>
        /// Organization name in InfluxDB (optional in v3, kept for compatibility)
        /// </summary>
        public string? Organization { get; set; }

        /// <summary>
        /// Bucket name for storing metrics (for v2 compatibility, maps to Database in v3)
        /// </summary>
        public string Bucket => Database;

        /// <summary>
        /// Connection timeout in milliseconds (defaults to 30 seconds)
        /// </summary>
        public int TimeoutMs { get; set; } = 30000;

        /// <summary>
        /// Batch size for bulk operations (defaults to 100 to avoid large payload timeouts)
        /// For jobs processing millions of metrics, use WriteBulkAsync overload with custom batchSize parameter
        /// </summary>
        public int BatchSize { get; set; } = 100;

        /// <summary>
        /// Default precision for timestamps (defaults to milliseconds)
        /// </summary>
        public string Precision { get; set; } = "millisecond";

        /// <summary>
        /// Authorization header prefix (defaults to "Bearer" for v3, can be "Token" for v2)
        /// </summary>
        public string AuthorizationScheme { get; set; } = "Bearer";
    }
}