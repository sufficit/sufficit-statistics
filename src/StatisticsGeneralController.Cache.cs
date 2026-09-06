using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Sufficit.Statistics
{
    /// <summary>
    /// Cache management part of StatisticsGeneralController
    /// Handles internal caching system for optimized batch processing of metrics
    /// </summary>
    public partial class StatisticsGeneralController
    {
        #region Internal Metrics Cache

        /// <summary>
        /// Maximum number of metrics allowed in the cache.
        /// When full, oldest metrics are dropped to prevent unbounded memory growth.
        /// This protects against scenarios where the metrics backend (e.g. InfluxDB) is unavailable.
        /// </summary>
        private const int MAX_METRICS_CACHE_CAPACITY = 10_000;

        /// <summary>
        /// Internal cache for metrics waiting to be processed
        /// Uses ConcurrentQueue for thread-safe, high-performance queueing
        /// </summary>
        private readonly ConcurrentQueue<Metric> _metricsCache = new ConcurrentQueue<Metric>();

        /// <summary>
        /// Counter for cached metrics - used for monitoring and decision making
        /// </summary>
        private int _cachedMetricsCount = 0;

        /// <summary>
        /// Flag to prevent multiple simultaneous processing operations
        /// </summary>
        private int _isProcessing = 0;

        /// <summary>
        /// Total number of metrics dropped due to cache being full.
        /// Used for throttled warning to avoid log spam.
        /// </summary>
        private long _droppedMetricsTotal = 0;

        /// <summary>
        /// Ticks of the last time a "cache full" warning was logged.
        /// Used to throttle warnings to at most once every 30 seconds.
        /// </summary>
        private long _lastDropWarningTicks = 0;

        /// <summary>
        /// Minimum interval (seconds) between repeated "cache full" warning logs.
        /// </summary>
        private const int DROP_WARNING_INTERVAL_SECONDS = 30;

        /// <summary>
        /// Adds a metric to internal cache and triggers smart processing
        /// This operation is extremely fast and doesn't block the event bus
        /// </summary>
        /// <param name="metric">Metric to cache</param>
        private void CacheMetricAndProcess(Metric metric)
        {
            // Enforce capacity limit: drop oldest metric if cache is full.
            // This prevents unbounded growth when the metrics backend is unavailable.
            if (_cachedMetricsCount >= MAX_METRICS_CACHE_CAPACITY)
            {
                if (_metricsCache.TryDequeue(out _))
                {
                    var dropped = Interlocked.Decrement(ref _cachedMetricsCount);
                    var totalDropped = Interlocked.Increment(ref _droppedMetricsTotal);

                    // Throttle warning: log only on first drop, or every 30 seconds.
                    // Avoids log spam when InfluxDB is unavailable and metrics keep arriving.
                    var now = DateTime.UtcNow.Ticks;
                    var last = Interlocked.Read(ref _lastDropWarningTicks);
                    var elapsedSeconds = TimeSpan.FromTicks(now - last).TotalSeconds;
                    if (last == 0 || elapsedSeconds >= DROP_WARNING_INTERVAL_SECONDS)
                    {
                        Interlocked.Exchange(ref _lastDropWarningTicks, now);
                        _logger.LogWarning(
                            "⚠️ Metrics cache full ({Capacity}), dropping oldest metrics. Depth: {Depth} | Total dropped so far: {TotalDropped}",
                            MAX_METRICS_CACHE_CAPACITY, dropped, totalDropped);
                    }
                }
            }

            _metricsCache.Enqueue(metric);
            var count = Interlocked.Increment(ref _cachedMetricsCount);
            
            _logger.LogDebug("📦 Metric '{Measurement}' cached - Total in cache: {CachedCount}", 
                metric.Measurement, count);

            // Trigger processing only if not already running to avoid spawning a Task per metric.
            if (Interlocked.CompareExchange(ref _isProcessing, 0, 0) == 0)
            {
                _logger.LogTrace("🎯 Triggering background processing for cache with {CachedCount} metrics", count);
                _ = Task.Run(async () => await SmartProcessCachedMetrics());
            }
        }

        /// <summary>
        /// Smart processing that adapts batch size based on queue length
        /// </summary>
        private async Task SmartProcessCachedMetrics()
        {
            // Generate unique execution ID for this processing run
#if NET8_0_OR_GREATER
            var executionId = Guid.NewGuid().ToString("N")[..8]; // Short 8-char ID (Range operator)
#else
            var executionId = Guid.NewGuid().ToString("N").Substring(0, 8); // Short 8-char ID (Compatible)
#endif
            
            // Prevent multiple simultaneous processing
            if (Interlocked.CompareExchange(ref _isProcessing, 1, 0) == 1)
            {
                _logger.LogTrace("⏭️ [EXEC:{ExecutionId}] Processing already in progress, skipping", executionId);
                return;
            }

            try
            {
                var currentCount = GetCachedMetricsCount();
                if (currentCount == 0)
                {
                    _logger.LogTrace("⏭️ [EXEC:{ExecutionId}] No metrics to process", executionId);
                    return;
                }

                _logger.LogDebug("🚀 [EXEC:{ExecutionId}] Starting metrics processing - {CurrentCount} metrics in cache", 
                    executionId, currentCount);

                // SMART BATCH SIZE CALCULATION
                var batchSize = CalculateOptimalBatchSize(currentCount);

                _logger.LogDebug("🧠 [EXEC:{ExecutionId}] Smart processing: {CurrentCount} metrics → batch size: {BatchSize}", 
                    executionId, currentCount, batchSize);

                // Process the batch
                var processed = await ProcessCachedMetricsBatch(batchSize, executionId);

                if (processed > 0)
                {
                    var remaining = GetCachedMetricsCount();
                    
                    // MAIN LOG: Information level for monitoring cache efficiency
                    _logger.LogInformation("✅ [EXEC:{ExecutionId}] Metrics processed: {ProcessedCount} | Cache remaining: {Remaining}", 
                        executionId, processed, remaining);

                    // CONTINUOUS PROCESSING: If there are still metrics, process again
                    if (remaining > 0)
                    {                        
                        _logger.LogDebug("🔄 [EXEC:{ExecutionId}] More metrics pending - triggering new processing", executionId);
                        // Small delay to avoid overwhelming the system, then process again
                        await Task.Delay(50);
                        _ = Task.Run(async () => await SmartProcessCachedMetrics());
                    }
                    else
                    {
                        _logger.LogDebug("✅ [EXEC:{ExecutionId}] Processing completed - cache is empty", executionId);
                    }
                }
                else
                {
                    _logger.LogDebug("⚠️ [EXEC:{ExecutionId}] No metrics processed - cache may be empty", executionId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ [EXEC:{ExecutionId}] Error in smart metrics processing", executionId);
            }
            finally
            {
                _logger.LogTrace("🏁 [EXEC:{ExecutionId}] Processing execution finished", executionId);
                Interlocked.Exchange(ref _isProcessing, 0);
            }
        }

        /// <summary>
        /// Calculates optimal batch size based on current queue length
        /// Logic: 1 métrica = 1, até 100 = múltiplos de 10, acima = múltiplos de 100
        /// </summary>
        /// <param name="currentCount">Current number of cached metrics</param>
        /// <returns>Optimal batch size</returns>
        private static int CalculateOptimalBatchSize(int currentCount)
        {
            return currentCount switch
            {
                1 => 1,                           // Uma métrica = salva imediatamente
                <= 10 => currentCount,            // Até 10 = salva todas de uma vez
                <= 100 => Math.Min(10, currentCount),   // Até 100 = lotes de 10
                <= 1000 => Math.Min(50, currentCount),  // Até 1000 = lotes de 50
                _ => Math.Min(100, currentCount)         // Acima de 1000 = lotes de 100
            };
        }

        /// <summary>
        /// Gets the current number of metrics waiting in cache
        /// </summary>
        public int GetCachedMetricsCount() => _cachedMetricsCount;

        /// <summary>
        /// Processes a batch of metrics from cache using the main controller's methods
        /// Optimized: Uses WriteMetricAsync for single metrics, WriteBulkAsync for batches
        /// Returns the number of metrics processed
        /// </summary>
        private async Task<int> ProcessCachedMetricsBatch(int maxBatchSize, string executionId = "unknown", CancellationToken cancellationToken = default)
        {
            var processedCount = 0;
            var batch = new List<Metric>(maxBatchSize);

            // Collect metrics from cache into batch
            while (batch.Count < maxBatchSize && _metricsCache.TryDequeue(out var metric))
            {
                batch.Add(metric);
                Interlocked.Decrement(ref _cachedMetricsCount);
            }

            if (batch.Count == 0)
            {
                _logger.LogTrace("📦 [EXEC:{ExecutionId}] No metrics to collect for batch processing", executionId);
                return 0;
            }

            _logger.LogDebug("📦 [EXEC:{ExecutionId}] Collected {BatchSize} metrics for batch processing", executionId, batch.Count);

            try
            {
                // Optimization: Use single metric method for better performance when batch size is 1
                if (batch.Count == 1)
                {
                    await this.WriteSingleAsync(batch[0], cancellationToken);
                    _logger.LogDebug("✅ [EXEC:{ExecutionId}] Processed single cached metric '{Measurement}' via main controller", 
                        executionId, batch[0].Measurement);
                }
                else
                {
                    // Use the main controller's WriteBulkAsync method for multiple metrics
                    await this.WriteBulkAsync(batch, cancellationToken);
                    _logger.LogDebug("✅ [EXEC:{ExecutionId}] Processed batch of {BatchSize} cached metrics via main controller", 
                        executionId, batch.Count);
                }
                
                processedCount = batch.Count;
            }
            catch (Exception ex)
            {
                // Drop failed metrics instead of re-queuing them.
                // Re-queuing causes unbounded growth when the backend is unavailable.
                _logger.LogError(ex, "❌ [EXEC:{ExecutionId}] Failed to process cached metrics batch of {BatchSize} metrics — metrics dropped to prevent memory leak", 
                    executionId, batch.Count);
            }

            return processedCount;
        }

        #endregion
    }
}
