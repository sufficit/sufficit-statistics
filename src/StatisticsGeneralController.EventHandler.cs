using Microsoft.Extensions.Logging;
using Sufficit.Events;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Sufficit.Statistics
{
    /// <summary>
    /// Event handler part of StatisticsGeneralController
    /// Handles Metric events from the event bus and delegates to cache system for optimized batch processing
    /// </summary>
    public partial class StatisticsGeneralController : IEventHandler<Metric>
    {
        #region IEventHandler<Metric> Implementation

        /// <summary>
        /// Handles incoming Metric events from the event bus
        /// FAST: Just caches the metric internally for later batch processing
        /// This keeps the event bus flowing smoothly without blocking on I/O operations
        /// </summary>
        /// <param name="metric">The metric to be cached</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public Task HandleAsync(Metric metric, CancellationToken cancellationToken = default)
        {
            try
            {
                // Quick validation - only return early if invalid
                if (string.IsNullOrWhiteSpace(metric?.Measurement) || metric.Fields == null || metric.Fields.Count == 0)
                {
                    _logger.LogWarning("Received invalid metric, skipping - Measurement: '{Measurement}', Fields: {FieldCount}", 
                        metric?.Measurement ?? "null", metric?.Fields?.Count ?? 0);
                    return Task.CompletedTask;
                }

                // FAST: Cache it and trigger smart processing!
                CacheMetricAndProcess(metric);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to cache metric: {Measurement}", metric?.Measurement ?? "unknown");
            }

            return Task.CompletedTask;
        }

        #endregion
    }
}
