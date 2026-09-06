using Sufficit.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Sufficit.Statistics
{
    /// <summary>
    /// Extension methods for StatisticsRuntime providing additional functionality
    /// </summary>
    public static class StatisticsRuntimeExtensions
    {
        /// <summary>
        /// Write a single metric point with a typed value
        /// </summary>
        /// <typeparam name="T">Type of the metric value</typeparam>
        /// <param name="measurement">Measurement name</param>
        /// <param name="value">Metric value</param>
        /// <param name="fieldName">Field name for the value (default: "value")</param>
        /// <param name="tags">Optional tags</param>
        /// <param name="timestamp">Optional timestamp (default: DateTime.UtcNow)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task WriteMetricAsync<T>(
            this StatisticsRuntime controller,
            string measurement,
            T value,
            string fieldName = "value",
            Dictionary<string, string>? tags = null,
            DateTime? timestamp = null,
            CancellationToken cancellationToken = default)
        {
            var metric = new Metric
            {
                Measurement = measurement,
                Tags = tags ?? new Dictionary<string, string>(),
                Fields = new Dictionary<string, object> { [fieldName] = value! },
                Timestamp = timestamp ?? DateTime.UtcNow
            };

            await controller.WriteSingleAsync(metric, cancellationToken);
        }

        #region Batch Operations

        /// <summary>
        /// Write metrics using Batch concept (same measurement/context)
        /// Converts MetricsBatch to individual Metric objects with Fields
        /// </summary>
        /// <param name="controller">The statistics controller</param>
        /// <param name="batch">Batch of metrics with same context</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task WriteBatchAsync(
            this StatisticsRuntime controller,
            MetricsBatch batch,
            CancellationToken cancellationToken = default)
        {
            // Convert batch to a single metric with multiple fields
            var metric = new Metric
            {
                Measurement = batch.Measurement,
                Fields = batch.Fields.ToDictionary(f => f.Key, f => f.Value),
                Tags = batch.Tags ?? new Dictionary<string, string>(),
                Timestamp = batch.Timestamp
            };

            // Use bulk write for efficiency
            await controller.WriteBulkAsync(new[] { metric }, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Write multiple metrics with different measurements in a single operation
        /// Optimized for mixed measurement scenarios
        /// </summary>
        /// <param name="controller">The statistics controller</param>
        /// <param name="measurements">Dictionary where key is measurement name and value is fields</param>
        /// <param name="tags">Common tags for all metrics</param>
        /// <param name="timestamp">Common timestamp for all metrics</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task WriteMultipleMeasurementsAsync(
            this StatisticsRuntime controller,
            Dictionary<string, Dictionary<string, object>> measurements,
            Dictionary<string, string>? tags = null,
            DateTime? timestamp = null,
            CancellationToken cancellationToken = default)
        {
            var metrics = measurements.Select(m => new Metric
            {
                Measurement = m.Key,
                Fields = m.Value,
                Tags = tags ?? new Dictionary<string, string>(),
                Timestamp = timestamp ?? DateTime.UtcNow
            }).ToList();

            await controller.WriteBulkAsync(metrics, cancellationToken: cancellationToken);
        }

        #endregion

        #region Query Helper Methods

        /// <summary>
        /// Get field values from a specific measurement
        /// </summary>
        /// <param name="controller">The statistics controller</param>
        /// <param name="measurement">Measurement name to search</param>
        /// <param name="fieldName">Field name to extract values from</param>
        /// <param name="timeRange">Time range for the search</param>
        /// <param name="tags">Optional tags for filtering</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Collection of field values</returns>
        public static async Task<IEnumerable<object>> GetFieldValuesAsync(
            this StatisticsRuntime controller,
            string measurement,
            string fieldName,
            DateTimeRangeNew? timeRange = null,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var metrics = await controller.SearchAsync(measurement, timeRange, tags, cancellationToken);
            
            return metrics
                .Where(m => m.Fields.ContainsKey(fieldName))
                .Select(m => m.Fields[fieldName])
                .ToList();
        }

        /// <summary>
        /// Sum field values from a specific measurement
        /// </summary>
        /// <param name="controller">The statistics controller</param>
        /// <param name="measurement">Measurement name to search</param>
        /// <param name="fieldName">Field name to sum</param>
        /// <param name="timeRange">Time range for the search</param>
        /// <param name="tags">Optional tags for filtering</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Sum of field values (converted to decimal)</returns>
        public static async Task<decimal> SumFieldAsync(
            this StatisticsRuntime controller,
            string measurement,
            string fieldName,
            DateTimeRangeNew? timeRange = null,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var values = await controller.GetFieldValuesAsync(measurement, fieldName, timeRange, tags, cancellationToken);
            
            return values
                .Where(v => v is IConvertible)
                .Sum(v => Convert.ToDecimal(v));
        }

        /// <summary>
        /// Count metrics in a measurement
        /// </summary>
        /// <param name="controller">The statistics controller</param>
        /// <param name="measurement">Measurement name to search</param>
        /// <param name="timeRange">Time range for the search</param>
        /// <param name="tags">Optional tags for filtering</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Count of metrics</returns>
        public static async Task<int> CountMetricsAsync(
            this StatisticsRuntime controller,
            string measurement,
            DateTimeRangeNew? timeRange = null,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var metrics = await controller.SearchAsync(measurement, timeRange, tags, cancellationToken);
            return metrics.Count();
        }

        /// <summary>
        /// Get average of field values from a specific measurement
        /// </summary>
        /// <param name="controller">The statistics controller</param>
        /// <param name="measurement">Measurement name to search</param>
        /// <param name="fieldName">Field name to average</param>
        /// <param name="timeRange">Time range for the search</param>
        /// <param name="tags">Optional tags for filtering</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Average of field values (converted to decimal)</returns>
        public static async Task<decimal> AverageFieldAsync(
            this StatisticsRuntime controller,
            string measurement,
            string fieldName,
            DateTimeRangeNew? timeRange = null,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var values = await controller.GetFieldValuesAsync(measurement, fieldName, timeRange, tags, cancellationToken);
            var numericValues = values.Where(v => v is IConvertible).Select(v => Convert.ToDecimal(v)).ToList();
            
            return numericValues.Any() ? numericValues.Average() : 0m;
        }

        /// <summary>
        /// Get maximum field value from a specific measurement
        /// </summary>
        /// <param name="controller">The statistics controller</param>
        /// <param name="measurement">Measurement name to search</param>
        /// <param name="fieldName">Field name to find maximum</param>
        /// <param name="timeRange">Time range for the search</param>
        /// <param name="tags">Optional tags for filtering</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Maximum field value (converted to decimal)</returns>
        public static async Task<decimal> MaxFieldAsync(
            this StatisticsRuntime controller,
            string measurement,
            string fieldName,
            DateTimeRangeNew? timeRange = null,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var values = await controller.GetFieldValuesAsync(measurement, fieldName, timeRange, tags, cancellationToken);
            var numericValues = values.Where(v => v is IConvertible).Select(v => Convert.ToDecimal(v));
            
            return numericValues.Any() ? numericValues.Max() : 0m;
        }

        /// <summary>
        /// Get minimum field value from a specific measurement
        /// </summary>
        /// <param name="controller">The statistics controller</param>
        /// <param name="measurement">Measurement name to search</param>
        /// <param name="fieldName">Field name to find minimum</param>
        /// <param name="timeRange">Time range for the search</param>
        /// <param name="tags">Optional tags for filtering</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Minimum field value (converted to decimal)</returns>
        public static async Task<decimal> MinFieldAsync(
            this StatisticsRuntime controller,
            string measurement,
            string fieldName,
            DateTimeRangeNew? timeRange = null,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var values = await controller.GetFieldValuesAsync(measurement, fieldName, timeRange, tags, cancellationToken);
            var numericValues = values.Where(v => v is IConvertible).Select(v => Convert.ToDecimal(v));
            
            return numericValues.Any() ? numericValues.Min() : 0m;
        }

        #endregion

        #region Convenience Methods

        /// <summary>
        /// Write a simple counter metric (increment by 1)
        /// </summary>
        /// <param name="controller">The statistics controller</param>
        /// <param name="measurement">The measurement name</param>
        /// <param name="tags">Optional tags for categorization</param>
        /// <param name="timestamp">Optional timestamp (uses current time if null)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task WriteCounterAsync(
            this StatisticsRuntime controller,
            string measurement,
            Dictionary<string, string>? tags = null,
            DateTime? timestamp = null,
            CancellationToken cancellationToken = default)
        {
            await controller.WriteMetricAsync(measurement, 1, "count", tags, timestamp, cancellationToken);
        }

        /// <summary>
        /// Write a gauge metric (current value)
        /// </summary>
        /// <param name="controller">The statistics controller</param>
        /// <param name="measurement">The measurement name</param>
        /// <param name="value">The gauge value</param>
        /// <param name="tags">Optional tags for categorization</param>
        /// <param name="timestamp">Optional timestamp (uses current time if null)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task WriteGaugeAsync<T>(
            this StatisticsRuntime controller,
            string measurement,
            T value,
            Dictionary<string, string>? tags = null,
            DateTime? timestamp = null,
            CancellationToken cancellationToken = default) where T : struct
        {
            await controller.WriteMetricAsync(measurement, value, "value", tags, timestamp, cancellationToken);
        }

        #endregion

        #region Convenience Search Methods

        /// <summary>
        /// Search metrics with aggregation (sum, avg, count, etc.)
        /// </summary>
        /// <param name="controller">The statistics controller</param>
        /// <param name="measurement">Measurement name to search</param>
        /// <param name="timestamp">Timestamp range for the search</param>
        /// <param name="aggregations">Aggregation functions to apply</param>
        /// <param name="groupBy">Fields to group by</param>
        /// <param name="tags">Optional tags for filtering</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Collection of aggregated metric results</returns>
        public static async Task<IEnumerable<Metric>> SearchWithAggregationAsync(
            this StatisticsRuntime controller,
            string measurement,
            DateTimeRangeNew? timestamp = null,
            List<string>? aggregations = null,
            List<string>? groupBy = null,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var searchParameters = new MetricsSearchParameters
            {
                Measurement = new TextFilter { Text = measurement, ExactMatch = true },
                Timestamp = timestamp,
                Tags = tags ?? new Dictionary<string, string>(),
                Aggregations = aggregations ?? new List<string>(),
                GroupBy = groupBy ?? new List<string>()
            };

            return await controller.SearchAsync(searchParameters, cancellationToken);
        }

        /// <summary>
        /// Search metrics with pagination
        /// </summary>
        /// <param name="controller">The statistics controller</param>
        /// <param name="measurement">Measurement name to search</param>
        /// <param name="timestamp">Timestamp range for the search</param>
        /// <param name="limit">Maximum number of results</param>
        /// <param name="offset">Number of results to skip</param>
        /// <param name="orderBy">Sort order (field name and direction)</param>
        /// <param name="tags">Optional tags for filtering</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Collection of metric results</returns>
        public static async Task<IEnumerable<Metric>> SearchWithPaginationAsync(
            this StatisticsRuntime controller,
            string measurement,
            DateTimeRangeNew? timestamp = null,
            int? limit = null,
            int? offset = null,
            Dictionary<string, string>? orderBy = null,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var searchParameters = new MetricsSearchParameters
            {
                Measurement = new TextFilter { Text = measurement, ExactMatch = true },
                Timestamp = timestamp,
                Tags = tags ?? new Dictionary<string, string>(),
                Limit = limit,
                Offset = offset,
                OrderBy = orderBy ?? new Dictionary<string, string>()
            };

            return await controller.SearchAsync(searchParameters, cancellationToken);
        }

        #endregion
    }
}
