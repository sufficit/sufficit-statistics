using Microsoft.Extensions.Logging;
using Sufficit.Events;
using Sufficit.Statistics;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Sufficit.Statistics
{
    /// <summary>
    /// Extension methods to simplify sending metrics through the event bus
    /// </summary>
    public static class EventBusMetricsExtensions
    {
        

        #region Simple Metric Publishing

        /// <summary>
        /// Publishes a simple metric with a single value
        /// </summary>
        /// <param name="eventBus">The event bus instance</param>
        /// <param name="measurement">Metric measurement name</param>
        /// <param name="value">Metric value</param>
        /// <param name="tags">Optional tags for filtering and grouping</param>
        /// <param name="timestamp">Optional timestamp (defaults to UtcNow)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task PublishMetricAsync(this IEventBus eventBus,
            string measurement,
            object value,
            Dictionary<string, string>? tags = null,
            DateTime? timestamp = null,
            CancellationToken cancellationToken = default)
        {
            var metric = new Metric
            {
                Timestamp = timestamp ?? DateTime.UtcNow,
                Measurement = measurement,
                Fields = new Dictionary<string, object> { ["value"] = value },
                Tags = tags ?? new Dictionary<string, string>()
            };

            await eventBus.PublishFireAndForgetAsync(metric, cancellationToken);
        }

        /// <summary>
        /// Publishes a simple metric with a single value and context ID for client segmentation
        /// </summary>
        /// <param name="eventBus">The event bus instance</param>
        /// <param name="measurement">Metric measurement name</param>
        /// <param name="value">Metric value</param>
        /// <param name="contextId">Context ID for client segmentation (recommended for most metrics)</param>
        /// <param name="tags">Optional additional tags for filtering and grouping</param>
        /// <param name="timestamp">Optional timestamp (defaults to UtcNow)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task PublishMetricAsync(this IEventBus eventBus,
            string measurement,
            object value,
            Guid contextId,
            Dictionary<string, string>? tags = null,
            DateTime? timestamp = null,
            CancellationToken cancellationToken = default)
        {
            var metricTags = tags ?? new Dictionary<string, string>();
            metricTags[MetricTags.ContextId] = contextId.ToString();

            await PublishMetricAsync(eventBus, measurement, value, metricTags, timestamp, cancellationToken);
        }

        /// <summary>
        /// Publishes a metric with multiple fields
        /// </summary>
        /// <param name="eventBus">The event bus instance</param>
        /// <param name="measurement">Metric measurement name</param>
        /// <param name="fields">Dictionary of field names and values</param>
        /// <param name="tags">Optional tags for filtering and grouping</param>
        /// <param name="timestamp">Optional timestamp (defaults to UtcNow)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task PublishMetricAsync(this IEventBus eventBus,
            string measurement,
            Dictionary<string, object> fields,
            Dictionary<string, string>? tags = null,
            DateTime? timestamp = null,
            CancellationToken cancellationToken = default)
        {
            var metric = new Metric
            {
                Timestamp = timestamp ?? DateTime.UtcNow,
                Measurement = measurement,
                Fields = fields,
                Tags = tags ?? new Dictionary<string, string>()
            };

            await eventBus.PublishFireAndForgetAsync(metric, cancellationToken);
        }

        /// <summary>
        /// Publishes a metric with multiple fields and context ID for client segmentation
        /// </summary>
        /// <param name="eventBus">The event bus instance</param>
        /// <param name="measurement">Metric measurement name</param>
        /// <param name="fields">Dictionary of field names and values</param>
        /// <param name="contextId">Context ID for client segmentation (recommended for most metrics)</param>
        /// <param name="tags">Optional additional tags for filtering and grouping</param>
        /// <param name="timestamp">Optional timestamp (defaults to UtcNow)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task PublishMetricAsync(this IEventBus eventBus,
            string measurement,
            Dictionary<string, object> fields,
            Guid contextId,
            Dictionary<string, string>? tags = null,
            DateTime? timestamp = null,
            CancellationToken cancellationToken = default)
        {
            var metricTags = tags ?? new Dictionary<string, string>();
            metricTags[MetricTags.ContextId] = contextId.ToString();

            await PublishMetricAsync(eventBus, measurement, fields, metricTags, timestamp, cancellationToken);
        }

        #endregion

        #region Typed Metric Publishing (using MetricTypes constants)

        /// <summary>
        /// Publishes a count metric (incremental counter)
        /// </summary>
        /// <param name="eventBus">The event bus instance</param>
        /// <param name="measurement">Metric measurement name</param>
        /// <param name="count">Count value (defaults to 1)</param>
        /// <param name="tags">Optional tags for filtering and grouping</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task PublishCountMetricAsync(this IEventBus eventBus,
            string measurement,
            int count = 1,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var metricTags = tags ?? new Dictionary<string, string>();
            metricTags[MetricTags.Type] = MetricTypes.Count;

            var metric = new Metric
            {
                Timestamp = DateTime.UtcNow,
                Measurement = measurement,
                Fields = new Dictionary<string, object> { ["value"] = count },
                Tags = metricTags
            };

            await eventBus.PublishFireAndForgetAsync(metric, cancellationToken);
        }

        /// <summary>
        /// Publishes a count metric (incremental counter) with context ID for client segmentation
        /// </summary>
        /// <param name="eventBus">The event bus instance</param>
        /// <param name="measurement">Metric measurement name</param>
        /// <param name="contextId">Context ID for client segmentation (recommended for most metrics)</param>
        /// <param name="count">Count value (defaults to 1)</param>
        /// <param name="tags">Optional additional tags for filtering and grouping</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task PublishCountMetricAsync(this IEventBus eventBus,
            string measurement,
            Guid contextId,
            int count = 1,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var metricTags = tags ?? new Dictionary<string, string>();
            metricTags[MetricTags.Type] = MetricTypes.Count;
            metricTags[MetricTags.ContextId] = contextId.ToString();

            var metric = new Metric
            {
                Timestamp = DateTime.UtcNow,
                Measurement = measurement,
                Fields = new Dictionary<string, object> { ["value"] = count },
                Tags = metricTags
            };

            await eventBus.PublishFireAndForgetAsync(metric, cancellationToken);
        }

        /// <summary>
        /// Publishes a duration metric (timing information)
        /// </summary>
        /// <param name="eventBus">The event bus instance</param>
        /// <param name="measurement">Metric measurement name</param>
        /// <param name="duration">Duration value</param>
        /// <param name="tags">Optional tags for filtering and grouping</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task PublishDurationMetricAsync(this IEventBus eventBus,
            string measurement,
            TimeSpan duration,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var metricTags = tags ?? new Dictionary<string, string>();
            metricTags[MetricTags.Type] = MetricTypes.Duration;

            await PublishMetricAsync(eventBus, measurement, duration.TotalMilliseconds, metricTags, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Publishes a duration metric (timing information) with context ID for client segmentation
        /// </summary>
        /// <param name="eventBus">The event bus instance</param>
        /// <param name="measurement">Metric measurement name</param>
        /// <param name="duration">Duration value</param>
        /// <param name="contextId">Context ID for client segmentation (recommended for most metrics)</param>
        /// <param name="tags">Optional additional tags for filtering and grouping</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task PublishDurationMetricAsync(this IEventBus eventBus,
            string measurement,
            TimeSpan duration,
            Guid contextId,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var metricTags = tags ?? new Dictionary<string, string>();
            metricTags[MetricTags.Type] = MetricTypes.Duration;

            await PublishMetricAsync(eventBus, measurement, duration.TotalMilliseconds, contextId, metricTags, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Publishes a percentage metric
        /// </summary>
        /// <param name="eventBus">The event bus instance</param>
        /// <param name="measurement">Metric measurement name</param>
        /// <param name="percentage">Percentage value (0-100)</param>
        /// <param name="tags">Optional tags for filtering and grouping</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task PublishPercentageMetricAsync(this IEventBus eventBus,
            string measurement,
            double percentage,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var metricTags = tags ?? new Dictionary<string, string>();
            metricTags[MetricTags.Type] = MetricTypes.Percentage;

            await PublishMetricAsync(eventBus, measurement, percentage, metricTags, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Publishes a percentage metric with context ID for client segmentation
        /// </summary>
        /// <param name="eventBus">The event bus instance</param>
        /// <param name="measurement">Metric measurement name</param>
        /// <param name="percentage">Percentage value (0-100)</param>
        /// <param name="contextId">Context ID for client segmentation (recommended for most metrics)</param>
        /// <param name="tags">Optional additional tags for filtering and grouping</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task PublishPercentageMetricAsync(this IEventBus eventBus,
            string measurement,
            double percentage,
            Guid contextId,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var metricTags = tags ?? new Dictionary<string, string>();
            metricTags[MetricTags.Type] = MetricTypes.Percentage;

            await PublishMetricAsync(eventBus, measurement, percentage, contextId, metricTags, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Publishes a histogram metric (distribution analysis)
        /// </summary>
        /// <param name="eventBus">The event bus instance</param>
        /// <param name="measurement">Metric measurement name</param>
        /// <param name="value">Value to be bucketed in histogram</param>
        /// <param name="tags">Optional tags for filtering and grouping</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task PublishHistogramMetricAsync(this IEventBus eventBus,
            string measurement,
            double value,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var metricTags = tags ?? new Dictionary<string, string>();
            metricTags[MetricTags.Type] = MetricTypes.Histogram;

            await PublishMetricAsync(eventBus, measurement, value, metricTags, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Publishes a histogram metric with context ID for client segmentation
        /// </summary>
        /// <param name="eventBus">The event bus instance</param>
        /// <param name="measurement">Metric measurement name</param>
        /// <param name="value">Value to be bucketed in histogram</param>
        /// <param name="contextId">Context ID for client segmentation (recommended for most metrics)</param>
        /// <param name="tags">Optional additional tags for filtering and grouping</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task PublishHistogramMetricAsync(this IEventBus eventBus,
            string measurement,
            double value,
            Guid contextId,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var metricTags = tags ?? new Dictionary<string, string>();
            metricTags[MetricTags.Type] = MetricTypes.Histogram;

            await PublishMetricAsync(eventBus, measurement, value, contextId, metricTags, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Publishes a gauge metric (snapshot value at a point in time)
        /// </summary>
        /// <param name="eventBus">The event bus instance</param>
        /// <param name="measurement">Metric measurement name</param>
        /// <param name="value">Current gauge value</param>
        /// <param name="tags">Optional tags for filtering and grouping</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task PublishGaugeMetricAsync(this IEventBus eventBus,
            string measurement,
            double value,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var metricTags = tags ?? new Dictionary<string, string>();
            metricTags[MetricTags.Type] = MetricTypes.Gauge;

            await PublishMetricAsync(eventBus, measurement, value, metricTags, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Publishes a gauge metric with context ID for client segmentation
        /// </summary>
        /// <param name="eventBus">The event bus instance</param>
        /// <param name="measurement">Metric measurement name</param>
        /// <param name="value">Current gauge value</param>
        /// <param name="contextId">Context ID for client segmentation (recommended for most metrics)</param>
        /// <param name="tags">Optional additional tags for filtering and grouping</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task PublishGaugeMetricAsync(this IEventBus eventBus,
            string measurement,
            double value,
            Guid contextId,
            Dictionary<string, string>? tags = null,
            CancellationToken cancellationToken = default)
        {
            var metricTags = tags ?? new Dictionary<string, string>();
            metricTags[MetricTags.Type] = MetricTypes.Gauge;

            await PublishMetricAsync(eventBus, measurement, value, contextId, metricTags, cancellationToken: cancellationToken);
        }

        #endregion
    }
}
