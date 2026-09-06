using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Sufficit.Statistics
{
    /// <summary>
    /// General controller for statistics operations built on the shared metrics contract
    /// Uses the modern Metric class with Fields dictionary for maximum flexibility
    /// This controller provides a unified interface for all statistics operations
    /// and works with any IMetricsProvider implementation (VictoriaMetrics, Entity Framework, etc.)
    /// </summary>
    public partial class StatisticsGeneralController : IMetricController
    {
        #region Private Fields

        private readonly IMetricsProvider _metricsProvider;
        private readonly ILogger<StatisticsGeneralController> _logger;

        #endregion
        #region IMPLEMENTATION OF IMetricController

        void IMetricController.Write(Metric metric) => CacheMetricAndProcess(metric);

        #endregion
        #region Constructor

        /// <summary>
        /// Initialize the controller with a metrics provider
        /// </summary>
        /// <param name="metricsProvider">Provider for metrics operations</param>
        /// <param name="logger">Logger instance</param>
        public StatisticsGeneralController(
            IMetricsProvider metricsProvider,
            ILogger<StatisticsGeneralController> logger)
        {
            _metricsProvider = metricsProvider ?? throw new ArgumentNullException(nameof(metricsProvider));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        #endregion

        #region Direct Write Operations (No Cache)

        /// <summary>
        /// (Direct) Write a single metric point directly
        /// </summary>
        /// <param name="metric">The metric to write</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public async Task WriteSingleAsync(Metric metric, CancellationToken cancellationToken)
        {
            try
            {
                await _metricsProvider.WriteAsync(metric, cancellationToken);
                _logger.LogDebug("Successfully wrote metric {Measurement}", metric.Measurement);
            }
            catch (OperationCanceledException)
            {
                // Don't log errors for OperationCanceledException as it's normal when operations are cancelled
                _logger.LogDebug("Write operation was cancelled for metric {Measurement}", metric.Measurement);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write metric {Measurement}", metric.Measurement);
                throw;
            }
        }

        /// <summary>
        /// Write multiple individual metrics in bulk operation
        /// </summary>
        public Task WriteBulkAsync(
            IEnumerable<Metric> metrics, 
            CancellationToken cancellationToken = default)
            => WriteBulkAsync(metrics, 100, cancellationToken);  

        /// <summary>
        /// (Direct) Write multiple metrics in a single batch operation
        /// </summary>
        /// <param name="items">Collection of metrics to write</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public async Task WriteBulkAsync(
            IEnumerable<Metric> items, 
            int? batchSize = null,
            CancellationToken cancellationToken = default)
        {
            var count = -1;

            if (items is ICollection<Metric> collection)
            {
                count = collection.Count;
            }
            else if (items is IReadOnlyCollection<Metric> readOnlyCollection)
            {
                count = readOnlyCollection.Count;
            }

            if (count == 0)
                return;

            try
            {
                await _metricsProvider.WriteBulkAsync(items, batchSize, cancellationToken);

                if (count >= 0)
                    _logger.LogDebug("Successfully wrote {Count} metrics in bulk operation", count);
                else
                    _logger.LogDebug("Successfully wrote metrics in bulk operation");
            }
            catch (OperationCanceledException)
            {
                // Don't log errors for OperationCanceledException as it's normal when operations are cancelled
                if (count >= 0)
                    _logger.LogDebug("Bulk write operation was cancelled for {Count} metrics", count);
                else
                    _logger.LogDebug("Bulk write operation was cancelled");
            }
            catch (Exception ex)
            {
                if (count >= 0)
                    _logger.LogError(ex, "Failed to write {Count} metrics in bulk operation", count);
                else
                    _logger.LogError(ex, "Failed to write metrics in bulk operation");
                throw;
            }
        }

        #endregion

        #region Search Operations

        /// <summary>
        /// Search metrics using detailed search parameters
        /// </summary>
        /// <param name="searchParameters">Complete search parameters</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Collection of metric results</returns>
        public async Task<IEnumerable<Metric>> SearchAsync(
            MetricsSearchParameters searchParameters,
            CancellationToken cancellationToken = default)
        {
            try
            {
                var results = await _metricsProvider.SearchAsync(searchParameters, cancellationToken);
                _logger.LogDebug("Successfully retrieved {Count} metrics from {Provider}",
                    results.Count(), _metricsProvider.GetType().Name);
                return results;
            }
            catch (OperationCanceledException)
            {
                // Don't log errors for OperationCanceledException as it's normal when operations are cancelled
                _logger.LogDebug("Search operation was cancelled using {Provider}",
                    _metricsProvider.GetType().Name);
                return Array.Empty<Metric>();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to search metrics using {Provider}",
                    _metricsProvider.GetType().Name);
                throw;
            }
        }

        /// <summary>
        /// Simple search by measurement name and optional time range
        /// </summary>
        /// <param name="measurement">Measurement name to search</param>
        /// <param name="timestamp">Optional timestamp range filter</param>
        /// <param name="tags">Optional tags for filtering</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Collection of metric results</returns>
        public async Task<IEnumerable<Metric>> SearchAsync(
            string measurement,
            DateTimeRangeNew? timestamp = null,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var searchParameters = new MetricsSearchParameters
            {
                Measurement = new TextFilter { Text = measurement, ExactMatch = true },
                Timestamp = timestamp,
                Tags = tags ?? new Dictionary<string, string>()
            };

            return await SearchAsync(searchParameters, cancellationToken);
        }

        /// <summary>
        /// Search metrics using raw query string (for advanced scenarios)
        /// </summary>
        /// <param name="query">Raw query string (provider-specific format)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Collection of metric results</returns>
        public async Task<IEnumerable<Metric>> SearchRawAsync(
            string query,
            CancellationToken cancellationToken = default)
        {
            try
            {
                var results = await _metricsProvider.QueryAsync(query, cancellationToken);
                return results;
            }
            catch (OperationCanceledException)
            {
                // Don't log errors for OperationCanceledException as it's normal when operations are cancelled
                _logger.LogDebug("Raw query operation was cancelled using {Provider}",
                    _metricsProvider.GetType().Name);
                return Array.Empty<Metric>();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to execute raw query using {Provider}",
                    _metricsProvider.GetType().Name);
                throw;
            }
        }

        #endregion

        #region Health Check Operations

        /// <summary>
        /// Check if the metrics provider is healthy and operational
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>True if the provider is healthy, false otherwise</returns>
        public async Task<bool> IsHealthyAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                return await _metricsProvider.IsHealthyAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Don't log errors for OperationCanceledException as it's normal when operations are cancelled
                _logger.LogDebug("Health check operation was cancelled for {Provider}",
                    _metricsProvider.GetType().Name);
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Health check failed for {Provider}",
                    _metricsProvider.GetType().Name);
                return false;
            }
        }

        /// <summary>
        /// Get information about the current metrics provider
        /// </summary>
        /// <returns>Provider information</returns>
        public ProviderInfo GetProviderInfo()
        {
            var providerType = _metricsProvider.GetType();
            return new ProviderInfo
            {
                Name = GetProviderDisplayName(providerType.Name),
                Type = providerType.Name,
                FullTypeName = providerType.FullName ?? providerType.Name
            };
        }

        /// <summary>
        /// Get a friendly display name for the provider
        /// </summary>
        private string GetProviderDisplayName(string typeName)
        {
            var lowerTypeName = typeName.ToLowerInvariant();
            return lowerTypeName switch
            {
                _ when lowerTypeName.Contains("influx") => "InfluxDB",
                _ when lowerTypeName.Contains("prometheus") => "Prometheus",
                _ when lowerTypeName.Contains("sql") => "SQL Server",
                _ => "Unknown"
            };
        }

        #endregion
    }    
}