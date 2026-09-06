using System;

namespace Sufficit.Statistics
{
    /// <summary>
    /// VictoriaMetrics configuration options.
    /// </summary>
    public class VictoriaMetricsConfiguration
    {
        public const string SECTIONNAME = "Sufficit:Statistics:VictoriaMetrics";

        /// <summary>
        /// VictoriaMetrics server URL.
        /// </summary>
        public string Url { get; set; } = string.Empty;

        /// <summary>
        /// Logical database name used by the Influx-compatible write endpoint.
        /// VictoriaMetrics stores it as the db label.
        /// </summary>
        public string Database { get; set; } = "sufficit";

        /// <summary>
        /// Authentication token or already-encoded Basic payload.
        /// </summary>
        public string Token { get; set; } = string.Empty;

        /// <summary>
        /// Authorization scheme used in the Authorization header.
        /// Defaults to Basic because the current Sufficit VictoriaMetrics host uses native Basic Auth.
        /// </summary>
        public string AuthorizationScheme { get; set; } = "Basic";

        /// <summary>
        /// Optional username used to compose the Basic authorization payload when Token is not provided.
        /// </summary>
        public string? Username { get; set; }

        /// <summary>
        /// Optional password used to compose the Basic authorization payload when Token is not provided.
        /// </summary>
        public string? Password { get; set; }

        /// <summary>
        /// Connection timeout in milliseconds.
        /// </summary>
        public int TimeoutMs { get; set; } = 30000;

        /// <summary>
        /// Batch size for bulk write operations.
        /// </summary>
        public int BatchSize { get; set; } = 100;

        /// <summary>
        /// Default precision for timestamps.
        /// </summary>
        public string Precision { get; set; } = "millisecond";

        /// <summary>
        /// Prefix for the Prometheus-compatible read API.
        /// </summary>
        public string PrometheusApiPrefix { get; set; } = "/prometheus/api/v1";

        /// <summary>
        /// Allows using internal network aliases whose hostname is not present in the public TLS
        /// certificate, while still requiring the certificate chain itself to be valid.
        /// This is intended for trusted Sufficit private networks only.
        /// </summary>
        public bool AllowInvalidCertificateName { get; set; } = false;
    }
}
