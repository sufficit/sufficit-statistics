namespace Sufficit.Statistics
{
    /// <summary>
    /// Configuration options for composite metrics provider
    /// </summary>
    public class CompositeMetricsConfiguration
    {
        public const string SECTIONNAME = "Sufficit:Statistics:Composite";

        /// <summary>
        /// Primary provider type (e.g., "VictoriaMetrics", "EntityFramework")
        /// </summary>
        public string PrimaryProvider { get; set; } = "VictoriaMetrics";

        /// <summary>
        /// Secondary provider type for fallback (optional)
        /// </summary>
        public string? SecondaryProvider { get; set; }

        /// <summary>
        /// Enable automatic failover to secondary provider
        /// </summary>
        public bool EnableFailover { get; set; } = true;

        /// <summary>
        /// Health check interval in minutes (defaults to 5)
        /// </summary>
        public int HealthCheckIntervalMinutes { get; set; } = 5;

        /// <summary>
        /// Number of consecutive failures before switching to secondary
        /// </summary>
        public int FailureThreshold { get; set; } = 3;

        /// <summary>
        /// Enable dual write mode (write to both providers)
        /// </summary>
        public bool EnableDualWrite { get; set; } = false;

        /// <summary>
        /// Prefer which provider for read operations when both available
        /// </summary>
        public string PreferredReadProvider { get; set; } = "Primary";
    }
}