using System;
using System.IO;
using System.Reflection;

namespace Sufficit.Statistics
{
    public sealed class MetricsQueueOptions
    {
        // Dedicated persistent directory per application instance; configure explicitly in production.
        public string DirectoryPath { get; set; } = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "Sufficit", Assembly.GetEntryAssembly()?.GetName().Name ?? "Statistics", "metrics-queue");
        public long MaxDatabaseBytes { get; set; } = 256L * 1024 * 1024;
        public int MaxMetricBytes { get; set; } = 256 * 1024;
        public int BatchSize { get; set; } = 100;
        public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);
        public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(60);

        internal void Validate()
        {
            if (string.IsNullOrWhiteSpace(DirectoryPath) || !Path.IsPathRooted(DirectoryPath))
                throw new ArgumentException("Metrics queue requires an absolute persistent directory.");
            if (MaxDatabaseBytes < 65536 || MaxMetricBytes < 1 || MaxMetricBytes > MaxDatabaseBytes / 4 ||
                BatchSize < 1 || BatchSize > 1000 || PollInterval <= TimeSpan.Zero ||
                MaxRetryDelay < PollInterval || MaxRetryDelay.TotalMilliseconds > int.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(MetricsQueueOptions));
        }
    }

    public sealed class MetricsQueueStatus
    {
        public long PendingCount { get; internal set; }
        public long DatabaseBytes { get; internal set; }
        public DateTime? OldestPendingUtc { get; internal set; }
        public DateTime? LastDeliveryUtc { get; internal set; }
        public long WriteFailures { get; internal set; }
        public long DeliveryFailures { get; internal set; }
    }
}
