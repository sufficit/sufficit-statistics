using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Sufficit.Statistics
{
    /// <summary>
    /// Composite metrics provider that manages multiple providers with fallback capabilities
    /// Supports primary/secondary provider configuration with automatic failover
    /// </summary>
    public class CompositeMetricsProvider : IMetricsProvider
    {
        #region Fields

        private readonly ILogger<CompositeMetricsProvider> _logger;
        private readonly CompositeMetricsConfiguration _config;
        private readonly IMetricsProvider _primaryProvider;
        private readonly IMetricsProvider? _secondaryProvider;
        private bool _primaryProviderHealthy = true;

        #endregion
        #region Constructor

        /// <summary>
        /// Initialize composite metrics provider
        /// </summary>
        /// <param name="logger">Logger instance</param>
        /// <param name="config">Configuration options</param>
        /// <param name="primaryProvider">Primary metrics provider</param>
        /// <param name="secondaryProvider">Secondary metrics provider (optional)</param>
        public CompositeMetricsProvider(
            ILogger<CompositeMetricsProvider> logger,
            CompositeMetricsConfiguration config,
            IMetricsProvider primaryProvider,
            IMetricsProvider? secondaryProvider = null)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _primaryProvider = primaryProvider ?? throw new ArgumentNullException(nameof(primaryProvider));
            _secondaryProvider = secondaryProvider;

            _logger.LogInformation("🔄 CompositeMetricsProvider initialized - Primary: {PrimaryProvider}, Secondary: {SecondaryProvider}, Failover: {EnableFailover}", 
                _config.PrimaryProvider, _config.SecondaryProvider ?? "None", _config.EnableFailover);
        }

        #endregion

        #region IMetricsProvider Implementation

        /// <summary>
        /// Write a single metric value with provider failover support
        /// </summary>
        public async Task WriteAsync<T>(
            string measurement, 
            T value, 
            Dictionary<string, string>? tags = null, 
            DateTime? timestamp = null, 
            CancellationToken cancellationToken = default) where T : struct
        {
            if (_config.EnableDualWrite && _secondaryProvider != null)
            {
                // Dual write mode - write to both providers
                await WriteToBothProvidersAsync(
                    () => _primaryProvider.WriteAsync(measurement, value, tags, timestamp, cancellationToken),
                    () => _secondaryProvider.WriteAsync(measurement, value, tags, timestamp, cancellationToken));
            }
            else
            {
                // Single write with failover
                await WriteWithFailoverAsync(
                    () => _primaryProvider.WriteAsync(measurement, value, tags, timestamp, cancellationToken),
                    () => _secondaryProvider?.WriteAsync(measurement, value, tags, timestamp, cancellationToken) ?? Task.CompletedTask);
            }
        }

        /// <summary>
        /// Write a single complete metric with provider failover support
        /// </summary>
        public async Task WriteAsync(Metric metric, CancellationToken cancellationToken = default)
        {
            if (_config.EnableDualWrite && _secondaryProvider != null)
            {
                // Dual write mode - write to both providers
                await WriteToBothProvidersAsync(
                    () => _primaryProvider.WriteAsync(metric, cancellationToken),
                    () => _secondaryProvider.WriteAsync(metric, cancellationToken));
            }
            else
            {
                // Single write with failover
                await WriteWithFailoverAsync(
                    () => _primaryProvider.WriteAsync(metric, cancellationToken),
                    () => _secondaryProvider?.WriteAsync(metric, cancellationToken) ?? Task.CompletedTask);
            }
        }

        /// <summary>
        /// Write multiple metrics in bulk with provider failover support
        /// </summary>
        public async Task WriteBulkAsync(
            IEnumerable<Metric> metrics, 
            CancellationToken cancellationToken = default)
            => await WriteBulkAsync(metrics, null, cancellationToken);

        /// <summary>
        /// Write multiple metrics in bulk with provider failover support
        /// </summary>
        public async Task WriteBulkAsync(
            IEnumerable<Metric> metrics, 
            int? batchSize = null,
            CancellationToken cancellationToken = default)
        {
            if (_config.EnableDualWrite && _secondaryProvider != null)
            {
                // Dual write mode - write to both providers
                await WriteToBothProvidersAsync(
                    () => _primaryProvider.WriteBulkAsync(metrics, batchSize, cancellationToken),
                    () => _secondaryProvider.WriteBulkAsync(metrics, batchSize, cancellationToken));
            }
            else
            {
                // Single write with failover
                await WriteWithFailoverAsync(
                    () => _primaryProvider.WriteBulkAsync(metrics, batchSize, cancellationToken),
                    () => _secondaryProvider?.WriteBulkAsync(metrics, batchSize, cancellationToken) ?? Task.CompletedTask);
            }
        }

        /// <summary>
        /// Query metrics with provider selection based on configuration
        /// </summary>
        public async Task<IEnumerable<Metric>> QueryAsync(
            string query, 
            CancellationToken cancellationToken = default)
        {
            var provider = GetPreferredReadProvider();
            
            try
            {
                var result = await provider.QueryAsync(query, cancellationToken);
                _logger.LogDebug("🔍 CompositeMetricsProvider: Query executed successfully on {ProviderType}", GetProviderType(provider));
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ CompositeMetricsProvider: Query failed on {ProviderType}, trying fallback", GetProviderType(provider));
                
                // Try fallback provider
                var fallbackProvider = provider == _primaryProvider ? _secondaryProvider : _primaryProvider;
                if (fallbackProvider != null)
                {
                    var result = await fallbackProvider.QueryAsync(query, cancellationToken);
                    _logger.LogInformation("✅ CompositeMetricsProvider: Query succeeded on fallback {ProviderType}", GetProviderType(fallbackProvider));
                    return result;
                }
                
                throw;
            }
        }

        /// <summary>
        /// Search metrics using structured parameters with provider selection
        /// </summary>
        public async Task<IEnumerable<Metric>> SearchAsync(
            MetricsSearchParameters parameters, 
            CancellationToken cancellationToken = default)
        {
            var provider = GetPreferredReadProvider();
            
            try
            {
                var result = await provider.SearchAsync(parameters, cancellationToken);
                _logger.LogDebug("🔍 CompositeMetricsProvider: Search executed successfully on {ProviderType}", GetProviderType(provider));
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ CompositeMetricsProvider: Search failed on {ProviderType}, trying fallback", GetProviderType(provider));
                
                // Try fallback provider
                var fallbackProvider = provider == _primaryProvider ? _secondaryProvider : _primaryProvider;
                if (fallbackProvider != null)
                {
                    var result = await fallbackProvider.SearchAsync(parameters, cancellationToken);
                    _logger.LogInformation("✅ CompositeMetricsProvider: Search succeeded on fallback {ProviderType}", GetProviderType(fallbackProvider));
                    return result;
                }
                
                throw;
            }
        }

        /// <summary>
        /// Check health of all configured providers
        /// </summary>
        public async Task<bool> IsHealthyAsync(CancellationToken cancellationToken = default)
        {
            var primaryHealthy = await _primaryProvider.IsHealthyAsync(cancellationToken);
            var secondaryHealthy = _secondaryProvider != null ? await _secondaryProvider.IsHealthyAsync(cancellationToken) : true;

            _primaryProviderHealthy = primaryHealthy;

            var overallHealthy = primaryHealthy || (secondaryHealthy && _config.EnableFailover);
            
            _logger.LogDebug("💓 CompositeMetricsProvider: Health check - Primary: {PrimaryHealthy}, Secondary: {SecondaryHealthy}, Overall: {OverallHealthy}", 
                primaryHealthy, secondaryHealthy, overallHealthy);

            return overallHealthy;
        }

        #endregion

        #region Internal Methods

        /// <summary>
        /// Write to both providers in dual write mode
        /// </summary>
        private async Task WriteToBothProvidersAsync(Func<Task> primaryWrite, Func<Task> secondaryWrite)
        {
            var primaryTask = WriteToProviderSafelyAsync("Primary", primaryWrite);
            var secondaryTask = WriteToProviderSafelyAsync("Secondary", secondaryWrite);

            await Task.WhenAll(primaryTask, secondaryTask);

            // Check if at least one write succeeded
            if (!primaryTask.Result && !secondaryTask.Result)
            {
                throw new InvalidOperationException("Both providers failed during dual write operation");
            }

            if (!primaryTask.Result)
            {
                _logger.LogWarning("⚠️ CompositeMetricsProvider: Primary provider failed in dual write mode");
            }

            if (!secondaryTask.Result)
            {
                _logger.LogWarning("⚠️ CompositeMetricsProvider: Secondary provider failed in dual write mode");
            }
        }

        /// <summary>
        /// Write with automatic failover to secondary provider
        /// </summary>
        private async Task WriteWithFailoverAsync(Func<Task> primaryWrite, Func<Task> secondaryWrite)
        {
            if (_primaryProviderHealthy)
            {
                try
                {
                    await primaryWrite();
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "⚠️ CompositeMetricsProvider: Primary provider write failed, trying secondary");
                    _primaryProviderHealthy = false;
                }
            }

            // Try secondary provider if available and failover is enabled
            if (_secondaryProvider != null && _config.EnableFailover)
            {
                try
                {
                    await secondaryWrite();
                    _logger.LogInformation("✅ CompositeMetricsProvider: Write succeeded on secondary provider");
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "❌ CompositeMetricsProvider: Secondary provider write also failed");
                }
            }

            throw new InvalidOperationException("All configured providers failed for write operation");
        }

        /// <summary>
        /// Safely write to a provider and return success status
        /// </summary>
        private async Task<bool> WriteToProviderSafelyAsync(string providerName, Func<Task> writeOperation)
        {
            try
            {
                await writeOperation();
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ CompositeMetricsProvider: {ProviderName} provider write failed", providerName);
                return false;
            }
        }

        /// <summary>
        /// Get preferred provider for read operations
        /// </summary>
        private IMetricsProvider GetPreferredReadProvider()
        {
            if (_config.PreferredReadProvider.Equals("Secondary", StringComparison.OrdinalIgnoreCase) && _secondaryProvider != null)
            {
                return _secondaryProvider;
            }

            // Default to primary provider
            return _primaryProvider;
        }

        /// <summary>
        /// Get provider type name for logging
        /// </summary>
        private string GetProviderType(IMetricsProvider provider)
        {
            if (provider == _primaryProvider) return _config.PrimaryProvider;
            if (provider == _secondaryProvider) return _config.SecondaryProvider ?? "Unknown";
            return provider.GetType().Name;
        }

        #endregion
    }
}
