using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Sufficit.Statistics
{
    /// <summary>
    /// VictoriaMetrics implementation of <see cref="IMetricsProvider"/>.
    /// Writes through the Influx-compatible endpoint and reads through the Prometheus-compatible API.
    /// </summary>
    public class VictoriaMetricsProvider : IMetricsProvider
    {
        #region Fields

        private readonly ILogger<VictoriaMetricsProvider> _logger;
        private readonly HttpClient _httpClient;
        private readonly VictoriaMetricsConfiguration _config;
        private readonly string _writeEndpoint;
        private readonly string _queryEndpoint;
        private readonly string _exportEndpoint;

        private DateTime _circuitBreakerOpenUntil = DateTime.MinValue;
        private int _consecutiveFailures = 0;
        private int _discardedMetricsCount = 0;
        private readonly object _circuitBreakerLock = new object();
        private const int CIRCUIT_BREAKER_FAILURE_THRESHOLD = 3;
        private static readonly TimeSpan CIRCUIT_BREAKER_TIMEOUT = TimeSpan.FromMinutes(5);

        #endregion

        #region Constructor

        /// <summary>
        /// Initializes the VictoriaMetrics provider.
        /// </summary>
        public VictoriaMetricsProvider(
            ILogger<VictoriaMetricsProvider> logger,
            HttpClient httpClient,
            VictoriaMetricsConfiguration config)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _config = config ?? throw new ArgumentNullException(nameof(config));

            _httpClient.Timeout = TimeSpan.FromMilliseconds(_config.TimeoutMs);

            var authorizationValue = BuildAuthorizationValue(_config);
            if (!string.IsNullOrWhiteSpace(authorizationValue))
            {
                _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
                    _config.AuthorizationScheme,
                    authorizationValue);
            }

            var baseUrl = _config.Url.TrimEnd('/');
            var precision = NormalizePrecision(_config.Precision);
            var apiPrefix = NormalizePrometheusApiPrefix(_config.PrometheusApiPrefix);

            _writeEndpoint = $"{baseUrl}/write?db={Uri.EscapeDataString(_config.Database)}&precision={precision}";
            _queryEndpoint = $"{baseUrl}{apiPrefix}/query";
            _exportEndpoint = $"{baseUrl}{apiPrefix}/export";

            _logger.LogInformation(
                "🔌 VictoriaMetricsProvider initialized - Database: {Database}, WriteEndpoint: {WriteEndpoint}, QueryEndpoint: {QueryEndpoint}",
                _config.Database,
                _writeEndpoint,
                _queryEndpoint);
        }

        #endregion

        #region IMetricsProvider Implementation

        /// <inheritdoc />
        public async Task WriteAsync<T>(
            string measurement,
            T value,
            Dictionary<string, string>? tags = null,
            DateTime? timestamp = null,
            CancellationToken cancellationToken = default) where T : struct
        {
            if (string.IsNullOrWhiteSpace(measurement))
                throw new ArgumentException("Measurement name cannot be null or empty", nameof(measurement));

            var metric = new Metric()
            {
                Measurement = measurement,
                Fields = new Dictionary<string, object>() { { "value", value } },
                Tags = tags ?? new Dictionary<string, string>(),
                Timestamp = timestamp ?? DateTime.UtcNow,
            };

            await WriteMetricAsync(metric, cancellationToken);

            _logger.LogDebug("📝 VictoriaMetricsProvider: Single metric '{Measurement}' written successfully", measurement);
        }

        /// <inheritdoc />
        public async Task WriteAsync(Metric metric, CancellationToken cancellationToken = default)
        {
            if (metric == null)
                throw new ArgumentNullException(nameof(metric));

            await WriteMetricAsync(metric, cancellationToken);

            _logger.LogDebug("📝 VictoriaMetricsProvider: Metric '{Measurement}' written successfully", metric.Measurement);
        }

        /// <inheritdoc />
        public Task WriteBulkAsync(IEnumerable<Metric> metrics, CancellationToken cancellationToken = default)
            => WriteBulkAsync(metrics, _config.BatchSize, cancellationToken);

        /// <inheritdoc />
        public async Task WriteBulkAsync(
            IEnumerable<Metric> metrics,
            int? batchSize,
            CancellationToken cancellationToken = default)
        {
            var effectiveBatchSize = batchSize ?? _config.BatchSize;
            if (effectiveBatchSize <= 0)
            {
                _logger.LogWarning(
                    "⚠️ VictoriaMetricsProvider: Invalid batch size {BatchSize}, using configured value {ConfiguredBatchSize}",
                    effectiveBatchSize,
                    _config.BatchSize);
                effectiveBatchSize = _config.BatchSize;
            }

            var processedCount = 0;
            var processedBatches = 0;
            var batch = new List<Metric>(effectiveBatchSize);

            foreach (var metric in metrics)
            {
                cancellationToken.ThrowIfCancellationRequested();
                batch.Add(metric);

                if (batch.Count < effectiveBatchSize)
                    continue;

                await WriteBatchAsync(batch, cancellationToken);
                processedBatches++;
                processedCount += batch.Count;
                _logger.LogDebug(
                    "📦 VictoriaMetricsProvider: Batch {CurrentBatch} processed ({ProcessedCount} metrics total)",
                    processedBatches,
                    processedCount);
                batch.Clear();
            }

            if (batch.Count > 0)
            {
                await WriteBatchAsync(batch, cancellationToken);
                processedBatches++;
                processedCount += batch.Count;
                _logger.LogDebug(
                    "📦 VictoriaMetricsProvider: Batch {CurrentBatch} processed ({ProcessedCount} metrics total)",
                    processedBatches,
                    processedCount);
            }

            if (processedCount == 0)
            {
                return;
            }

            _logger.LogDebug(
                "✅ VictoriaMetricsProvider: Successfully bulk inserted {MetricCount} metrics in {BatchCount} batches",
                processedCount,
                processedBatches);
        }

        /// <inheritdoc />
        public async Task<IEnumerable<Metric>> QueryAsync(string query, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(query))
                throw new ArgumentException("Query cannot be null or empty", nameof(query));

            var requestUri = $"{_queryEndpoint}?query={Uri.EscapeDataString(query)}";
            var response = await _httpClient.GetAsync(requestUri, cancellationToken);
            var responseBody = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogError(
                    "❌ VictoriaMetricsProvider: Query failed with status {StatusCode} - {Response}",
                    response.StatusCode,
                    responseBody);
                response.EnsureSuccessStatusCode();
            }

            return ParsePrometheusQueryResponse(responseBody);
        }

        /// <inheritdoc />
        public async Task<IEnumerable<Metric>> SearchAsync(MetricsSearchParameters parameters, CancellationToken cancellationToken = default)
        {
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            if ((parameters.Aggregations?.Any() == true) ||
                (parameters.GroupBy?.Any() == true) ||
                !string.IsNullOrWhiteSpace(parameters.TimeBucket))
            {
                _logger.LogWarning(
                    "⚠️ VictoriaMetricsProvider: Structured search currently ignores Aggregations, GroupBy and TimeBucket and returns raw samples only.");
            }

            var requestUri = BuildExportRequestUri(parameters);
            var response = await _httpClient.GetAsync(requestUri, cancellationToken);
            var responseBody = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogError(
                    "❌ VictoriaMetricsProvider: Search failed with status {StatusCode} - {Response}",
                    response.StatusCode,
                    responseBody);
                response.EnsureSuccessStatusCode();
            }

            var metrics = ParseExportResponse(responseBody, parameters);
            return ApplySearchPostProcessing(metrics, parameters).ToList();
        }

        /// <inheritdoc />
        public async Task<bool> IsHealthyAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                var response = await _httpClient.GetAsync($"{_config.Url.TrimEnd('/')}/ping", cancellationToken);
                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogWarning(
                        "⚠️ VictoriaMetricsProvider: Health check failed with status {StatusCode}",
                        response.StatusCode);
                    return false;
                }

                await TestAuthentication(cancellationToken);
                _logger.LogDebug("✅ VictoriaMetricsProvider: Health check passed");
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ VictoriaMetricsProvider: Health check error");
                return false;
            }
        }

        #endregion

        #region Internal Methods

        private async Task TestAuthentication(CancellationToken cancellationToken)
        {
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000000;
            var payload = $"_internal_test,type=health_check value=1 {timestamp}";

            using var content = new StringContent(payload, Encoding.UTF8, "application/x-line-protocol");
            var response = await _httpClient.PostAsync(_writeEndpoint, content, cancellationToken);

            if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                throw new UnauthorizedAccessException("VictoriaMetrics authentication failed during the health-check write test.");

            if (!response.IsSuccessStatusCode)
            {
                var responseBody = await response.Content.ReadAsStringAsync();
                throw new HttpRequestException($"VictoriaMetrics test write failed: {response.StatusCode} - {responseBody}");
            }
        }

        private async Task WriteMetricAsync(Metric metric, CancellationToken cancellationToken)
        {
            var lineProtocol = ConvertToLineProtocol(metric);
            await WriteLineProtocolAsync(lineProtocol, cancellationToken);
        }

        private async Task WriteBatchAsync(List<Metric> metrics, CancellationToken cancellationToken)
        {
            var payload = string.Join("\n", metrics.Select(ConvertToLineProtocol));
            await WriteLineProtocolAsync(payload, cancellationToken);
        }

        private async Task WriteLineProtocolAsync(string lineProtocol, CancellationToken cancellationToken)
        {
            lock (_circuitBreakerLock)
            {
                if (DateTime.UtcNow < _circuitBreakerOpenUntil)
                {
                    _discardedMetricsCount++;
                    _logger.LogDebug(
                        "⚡ VictoriaMetricsProvider: Circuit breaker OPEN - discarding metric write #{Count}",
                        _discardedMetricsCount);
                    return;
                }
            }

            const int maxRetries = 3;
            const int baseDelayMs = 1000;

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    using var content = new StringContent(lineProtocol, Encoding.UTF8, "application/x-line-protocol");
                    var response = await _httpClient.PostAsync(_writeEndpoint, content, cancellationToken);
                    var responseBody = response.IsSuccessStatusCode ? string.Empty : await response.Content.ReadAsStringAsync();

                    if (!response.IsSuccessStatusCode)
                    {
                        _logger.LogError(
                            "❌ VictoriaMetricsProvider: Write failed with status {StatusCode} - {Response}",
                            response.StatusCode,
                            responseBody);
                    }

                    response.EnsureSuccessStatusCode();

                    lock (_circuitBreakerLock)
                    {
                        if (_consecutiveFailures > 0 || _circuitBreakerOpenUntil > DateTime.MinValue)
                        {
                            if (_discardedMetricsCount > 0)
                            {
                                _logger.LogWarning(
                                    "✅ VictoriaMetricsProvider: Connection restored - {DiscardedCount} metrics were discarded while the circuit breaker was open",
                                    _discardedMetricsCount);
                            }

                            _consecutiveFailures = 0;
                            _circuitBreakerOpenUntil = DateTime.MinValue;
                            _discardedMetricsCount = 0;
                        }
                    }

                    return;
                }
                catch (Exception ex) when (attempt < maxRetries && IsRetryableException(ex))
                {
                    var delay = TimeSpan.FromMilliseconds(baseDelayMs * Math.Pow(2, attempt - 1));

                    if (ex is OperationCanceledException)
                    {
                        _logger.LogDebug(
                            "🔄 VictoriaMetricsProvider: Write cancelled on attempt {Attempt}/{MaxRetries}",
                            attempt,
                            maxRetries);
                    }
                    else
                    {
                        _logger.LogWarning(
                            "⚠️ VictoriaMetricsProvider: Write failed on attempt {Attempt}/{MaxRetries}, retrying in {Delay}ms - {Error}",
                            attempt,
                            maxRetries,
                            delay.TotalMilliseconds,
                            ex.Message);
                    }

                    await Task.Delay(delay, cancellationToken);
                }
                catch (Exception ex)
                {
                    if (ex is OperationCanceledException)
                    {
                        _logger.LogDebug(
                            "❌ VictoriaMetricsProvider: Write cancelled after {Attempt} attempts",
                            attempt);
                    }
                    else
                    {
                        _logger.LogError(
                            ex,
                            "❌ VictoriaMetricsProvider: Write failed on attempt {Attempt}/{MaxRetries} - {LineProtocol}",
                            attempt,
                            maxRetries,
                            lineProtocol.Substring(0, Math.Min(lineProtocol.Length, 200)));

                        lock (_circuitBreakerLock)
                        {
                            _consecutiveFailures++;
                            if (_consecutiveFailures >= CIRCUIT_BREAKER_FAILURE_THRESHOLD)
                            {
                                _circuitBreakerOpenUntil = DateTime.UtcNow.Add(CIRCUIT_BREAKER_TIMEOUT);
                                _logger.LogWarning(
                                    "⚡ VictoriaMetricsProvider: Circuit breaker OPENED after {Failures} consecutive failures",
                                    _consecutiveFailures);
                            }
                        }
                    }

                    throw;
                }
            }
        }

        private string BuildExportRequestUri(MetricsSearchParameters parameters)
        {
            var selectors = BuildExportMatchers(parameters).ToList();
            if (!selectors.Any())
                selectors.Add(BuildSelector(
                    ".+",
                    true,
                    parameters.Tags ?? new Dictionary<string, string>()));

            var start = parameters.Timestamp?.Start?.ToUniversalTime() ?? DateTime.UtcNow.AddHours(-24);
            var end = parameters.Timestamp?.End?.ToUniversalTime() ?? DateTime.UtcNow;

            if (end < start)
            {
                var swap = start;
                start = end;
                end = swap;
            }

            var builder = new StringBuilder(_exportEndpoint);
            builder.Append('?');

            for (int index = 0; index < selectors.Count; index++)
            {
                if (index > 0)
                    builder.Append('&');

                builder.Append("match[]=");
                builder.Append(Uri.EscapeDataString(selectors[index]));
            }

            builder.Append("&start=");
            builder.Append(FormatUnixSeconds(start));
            builder.Append("&end=");
            builder.Append(FormatUnixSeconds(end));

            return builder.ToString();
        }

        private IEnumerable<string> BuildExportMatchers(MetricsSearchParameters parameters)
        {
            var requestedFields = parameters.Fields
                .Where(static field => !string.IsNullOrWhiteSpace(field))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (parameters.Measurement?.IsValid == true)
            {
                var measurementText = parameters.Measurement.Text ?? string.Empty;

                if (parameters.Measurement.ExactMatch)
                {
                    if (requestedFields.Any())
                    {
                        foreach (var field in requestedFields)
                        {
                            yield return BuildSelector($"{measurementText}_{field}", false, parameters.Tags);
                        }

                        yield break;
                    }

                    yield return BuildSelector($"^{Regex.Escape(measurementText)}_.+$", true, parameters.Tags);
                    yield break;
                }

                var escapedMeasurement = Regex.Escape(measurementText);
                if (requestedFields.Any())
                {
                    var fieldPattern = string.Join("|", requestedFields.Select(Regex.Escape));
                    yield return BuildSelector($".*{escapedMeasurement}.*_({fieldPattern})$", true, parameters.Tags);
                    yield break;
                }

                yield return BuildSelector($".*{escapedMeasurement}.*_.+$", true, parameters.Tags);
                yield break;
            }

            if (requestedFields.Any())
            {
                var fieldPattern = string.Join("|", requestedFields.Select(Regex.Escape));
                yield return BuildSelector($".+_({fieldPattern})$", true, parameters.Tags);
                yield break;
            }

            yield return BuildSelector(".+", true, parameters.Tags);
        }

        private string BuildSelector(string metricMatcher, bool useRegex, Dictionary<string, string>? tags)
        {
            var clauses = new List<string>
            {
                useRegex
                    ? $"__name__=~\"{EscapeLabelValue(metricMatcher)}\""
                    : $"__name__=\"{EscapeLabelValue(metricMatcher)}\"",
                $"db=\"{EscapeLabelValue(_config.Database)}\""
            };

            if (tags != null)
            {
                foreach (var tag in tags)
                {
                    if (string.IsNullOrWhiteSpace(tag.Key) || string.IsNullOrWhiteSpace(tag.Value) || string.Equals(tag.Key, "__name__", StringComparison.OrdinalIgnoreCase))
                        continue;

                    clauses.Add($"{tag.Key}=\"{EscapeLabelValue(tag.Value)}\"");
                }
            }

            return $"{{{string.Join(",", clauses)}}}";
        }

        private IEnumerable<Metric> ParseExportResponse(string responseBody, MetricsSearchParameters parameters)
        {
            var metrics = new List<Metric>();
            if (string.IsNullOrWhiteSpace(responseBody))
                return metrics;

            using var reader = new StringReader(responseBody);
            string? line;
            while ((line = reader.ReadLine()) != null)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                using var document = JsonDocument.Parse(line);
                var root = document.RootElement;

                if (!root.TryGetProperty("metric", out var metricElement) ||
                    !root.TryGetProperty("timestamps", out var timestampsElement) ||
                    !root.TryGetProperty("values", out var valuesElement) ||
                    timestampsElement.ValueKind != JsonValueKind.Array ||
                    valuesElement.ValueKind != JsonValueKind.Array)
                {
                    continue;
                }

                var labels = metricElement.EnumerateObject().ToDictionary(
                    property => property.Name,
                    property => property.Value.GetString() ?? string.Empty,
                    StringComparer.OrdinalIgnoreCase);

                var seriesName = labels.TryGetValue("__name__", out var resolvedName) ? resolvedName : "result_value";

                var timestampItems = timestampsElement.EnumerateArray().ToArray();
                var valueItems = valuesElement.EnumerateArray().ToArray();
                var count = Math.Min(timestampItems.Length, valueItems.Length);

                for (int index = 0; index < count; index++)
                {
                    if (!TryReadUnixMilliseconds(timestampItems[index], out var timestampMs))
                        continue;

                    var value = ConvertMetricValue(valueItems[index]);
                    metrics.Add(CreateMetricFromSeries(seriesName, labels, timestampMs, value, parameters));
                }
            }

            return metrics;
        }

        private IEnumerable<Metric> ParsePrometheusQueryResponse(string responseBody)
        {
            var metrics = new List<Metric>();
            if (string.IsNullOrWhiteSpace(responseBody))
                return metrics;

            using var document = JsonDocument.Parse(responseBody);
            var root = document.RootElement;

            if (!root.TryGetProperty("status", out var statusElement) ||
                !string.Equals(statusElement.GetString(), "success", StringComparison.OrdinalIgnoreCase) ||
                !root.TryGetProperty("data", out var dataElement))
            {
                return metrics;
            }

            if (!dataElement.TryGetProperty("resultType", out var resultTypeElement))
                return metrics;

            var resultType = resultTypeElement.GetString() ?? string.Empty;

            switch (resultType)
            {
                case "vector":
                    if (dataElement.TryGetProperty("result", out var vectorResult) && vectorResult.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var item in vectorResult.EnumerateArray())
                        {
                            if (!item.TryGetProperty("metric", out var metricElement) ||
                                !item.TryGetProperty("value", out var valueElement) ||
                                valueElement.ValueKind != JsonValueKind.Array ||
                                valueElement.GetArrayLength() < 2)
                            {
                                continue;
                            }

                            var labels = metricElement.EnumerateObject().ToDictionary(
                                property => property.Name,
                                property => property.Value.GetString() ?? string.Empty,
                                StringComparer.OrdinalIgnoreCase);

                            var timestamp = ReadPrometheusSeconds(valueElement[0]);
                            var value = ConvertMetricValue(valueElement[1]);
                            var seriesName = labels.TryGetValue("__name__", out var resolvedName) ? resolvedName : "result_value";
                            metrics.Add(CreateMetricFromSeries(seriesName, labels, timestamp, value, null));
                        }
                    }
                    break;

                case "matrix":
                    if (dataElement.TryGetProperty("result", out var matrixResult) && matrixResult.ValueKind == JsonValueKind.Array)
                    {
                        foreach (var item in matrixResult.EnumerateArray())
                        {
                            if (!item.TryGetProperty("metric", out var metricElement) ||
                                !item.TryGetProperty("values", out var valuesElement) ||
                                valuesElement.ValueKind != JsonValueKind.Array)
                            {
                                continue;
                            }

                            var labels = metricElement.EnumerateObject().ToDictionary(
                                property => property.Name,
                                property => property.Value.GetString() ?? string.Empty,
                                StringComparer.OrdinalIgnoreCase);
                            var seriesName = labels.TryGetValue("__name__", out var resolvedName) ? resolvedName : "result_value";

                            foreach (var sample in valuesElement.EnumerateArray())
                            {
                                if (sample.ValueKind != JsonValueKind.Array || sample.GetArrayLength() < 2)
                                    continue;

                                var timestamp = ReadPrometheusSeconds(sample[0]);
                                var value = ConvertMetricValue(sample[1]);
                                metrics.Add(CreateMetricFromSeries(seriesName, labels, timestamp, value, null));
                            }
                        }
                    }
                    break;

                case "scalar":
                case "string":
                    if (dataElement.TryGetProperty("result", out var scalarResult) && scalarResult.ValueKind == JsonValueKind.Array && scalarResult.GetArrayLength() >= 2)
                    {
                        var scalarValue = ConvertMetricValue(scalarResult[1]);
                        var metric = new Metric()
                        {
                            Measurement = "result",
                            Timestamp = ConvertFromUnixMilliseconds(ReadPrometheusSeconds(scalarResult[0])),
                            Tags = new Dictionary<string, string>(),
                            Fields = new Dictionary<string, object>()
                        };

                        if (scalarValue != null)
                            metric.Fields["value"] = scalarValue;

                        metrics.Add(metric);
                    }
                    break;
            }

            return metrics;
        }

        private Metric CreateMetricFromSeries(
            string seriesName,
            Dictionary<string, string> labels,
            long timestampMs,
            object? value,
            MetricsSearchParameters? parameters)
        {
            SplitSeriesName(seriesName, parameters, out var measurement, out var fieldName);

            var metric = new Metric()
            {
                Measurement = measurement,
                Timestamp = ConvertFromUnixMilliseconds(timestampMs),
                Tags = new Dictionary<string, string>(),
                Fields = new Dictionary<string, object>(),
            };

            if (value != null)
                metric.Fields[fieldName] = value;

            foreach (var label in labels)
            {
                if (string.Equals(label.Key, "__name__", StringComparison.OrdinalIgnoreCase))
                    continue;

                if (string.Equals(label.Key, "db", StringComparison.OrdinalIgnoreCase) && string.Equals(label.Value, _config.Database, StringComparison.OrdinalIgnoreCase))
                    continue;

                metric.Tags[label.Key] = label.Value;
            }

            return metric;
        }

        private IEnumerable<Metric> ApplySearchPostProcessing(IEnumerable<Metric> metrics, MetricsSearchParameters parameters)
        {
            IEnumerable<Metric> result = metrics;

            if (parameters.Fields?.Any() == true)
            {
                result = result.Where(metric => metric.Fields.Keys.Any(key => parameters.Fields.Contains(key, StringComparer.OrdinalIgnoreCase)));
            }

            if (parameters.OrderBy?.Any() == true)
            {
                var firstOrder = parameters.OrderBy.First();
                var descending = string.Equals(firstOrder.Value, "desc", StringComparison.OrdinalIgnoreCase);

                if (string.Equals(firstOrder.Key, "timestamp", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(firstOrder.Key, "_time", StringComparison.OrdinalIgnoreCase))
                {
                    result = descending
                        ? result.OrderByDescending(metric => metric.Timestamp)
                        : result.OrderBy(metric => metric.Timestamp);
                }
                else
                {
                    result = descending
                        ? result.OrderByDescending(metric => GetComparableFieldValue(metric, firstOrder.Key))
                        : result.OrderBy(metric => GetComparableFieldValue(metric, firstOrder.Key));
                }
            }
            else
            {
                result = result.OrderByDescending(metric => metric.Timestamp);
            }

            if (parameters.Offset.HasValue && parameters.Offset.Value > 0)
                result = result.Skip(parameters.Offset.Value);

            if (parameters.Limit.HasValue && parameters.Limit.Value > 0)
                result = result.Take(parameters.Limit.Value);

            return result;
        }

        private static IComparable? GetComparableFieldValue(Metric metric, string fieldName)
        {
            if (!metric.Fields.TryGetValue(fieldName, out var value) || value == null)
                return null;

            return value as IComparable;
        }

        private static string BuildAuthorizationValue(VictoriaMetricsConfiguration config)
        {
            if (!string.IsNullOrWhiteSpace(config.Token))
                return config.Token;

            if (string.Equals(config.AuthorizationScheme, "Basic", StringComparison.OrdinalIgnoreCase) &&
                !string.IsNullOrWhiteSpace(config.Username) &&
                !string.IsNullOrWhiteSpace(config.Password))
            {
                var rawValue = $"{config.Username}:{config.Password}";
                return Convert.ToBase64String(Encoding.UTF8.GetBytes(rawValue));
            }

            return string.Empty;
        }

        private static string NormalizePrometheusApiPrefix(string prefix)
        {
            if (string.IsNullOrWhiteSpace(prefix))
                return "/prometheus/api/v1";

            var trimmed = prefix.Trim();
            if (!trimmed.StartsWith("/", StringComparison.Ordinal))
                trimmed = "/" + trimmed;

            return trimmed.TrimEnd('/');
        }

        private static string NormalizePrecision(string precision)
        {
            return precision switch
            {
                "second" or "seconds" or "s" => "s",
                "millisecond" or "milliseconds" or "ms" => "ms",
                "microsecond" or "microseconds" or "us" => "us",
                "nanosecond" or "nanoseconds" or "ns" => "ns",
                _ => "ms"
            };
        }

        private static bool IsRetryableException(Exception ex)
        {
            return ex is HttpRequestException ||
                   ex is OperationCanceledException ||
                   ex is SocketException ||
                   (ex.InnerException != null && IsRetryableException(ex.InnerException));
        }

        private string ConvertToLineProtocol(Metric metric)
        {
            var builder = new StringBuilder();
            builder.Append(EscapeLineProtocol(metric.Measurement ?? "unknown"));

            if (metric.Tags?.Any() == true)
            {
                foreach (var tag in metric.Tags.OrderBy(static tag => tag.Key))
                {
                    builder.Append($",{EscapeLineProtocol(tag.Key)}={EscapeLineProtocol(tag.Value)}");
                }
            }

            builder.Append(' ');

            if (metric.Fields?.Any() == true)
            {
                builder.Append(string.Join(",", metric.Fields.Select(static field => $"{field.Key}={FormatFieldValue(field.Value)}")));
            }
            else
            {
                builder.Append("value=1");
            }

            if (metric.Timestamp != default(DateTime))
            {
                builder.Append(' ');
                builder.Append(ConvertTimestamp(metric.Timestamp));
            }

            return builder.ToString();
        }

        private static string EscapeLineProtocol(string value)
        {
            if (string.IsNullOrEmpty(value))
                return value;

            return value
                .Replace("\\", "\\\\")
                .Replace(",", "\\,")
                .Replace("=", "\\=")
                .Replace(" ", "\\ ");
        }

        private static string EscapeLabelValue(string value)
        {
            return value
                .Replace("\\", "\\\\")
                .Replace("\"", "\\\"");
        }

        private static string FormatFieldValue(object? value)
        {
            if (value == null)
                return "null";

            return value switch
            {
                string text => $"\"{text.Replace("\"", "\\\"")}\"",
                bool boolValue => boolValue.ToString().ToLowerInvariant(),
                float floatValue => floatValue.ToString("F6", CultureInfo.InvariantCulture),
                double doubleValue => doubleValue.ToString("F6", CultureInfo.InvariantCulture),
                decimal decimalValue => decimalValue.ToString("F6", CultureInfo.InvariantCulture),
                _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? "null"
            };
        }

        private long ConvertTimestamp(DateTime timestamp)
        {
            var utcTimestamp = timestamp.Kind == DateTimeKind.Utc ? timestamp : timestamp.ToUniversalTime();
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            return _config.Precision switch
            {
                "s" or "second" or "seconds" => (long)(utcTimestamp - epoch).TotalSeconds,
                "ms" or "millisecond" or "milliseconds" => (long)(utcTimestamp - epoch).TotalMilliseconds,
                "us" or "microsecond" or "microseconds" => (utcTimestamp - epoch).Ticks / 10,
                "ns" or "nanosecond" or "nanoseconds" => (utcTimestamp - epoch).Ticks * 100,
                _ => (long)(utcTimestamp - epoch).TotalMilliseconds,
            };
        }

        private static string FormatUnixSeconds(DateTime value)
            => new DateTimeOffset(value).ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);

        private static void SplitSeriesName(string seriesName, MetricsSearchParameters? parameters, out string measurement, out string fieldName)
        {
            if (parameters?.Fields?.Any() == true)
            {
                var matchedField = parameters.Fields
                    .Where(static field => !string.IsNullOrWhiteSpace(field))
                    .OrderByDescending(static field => field.Length)
                    .FirstOrDefault(field => seriesName.EndsWith("_" + field, StringComparison.OrdinalIgnoreCase));

                if (!string.IsNullOrWhiteSpace(matchedField))
                {
                    measurement = seriesName.Substring(0, seriesName.Length - matchedField.Length - 1);
                    fieldName = matchedField;
                    return;
                }
            }

            if (parameters?.Measurement?.IsValid == true && parameters.Measurement.ExactMatch)
            {
                var measurementText = parameters.Measurement.Text ?? string.Empty;
                var prefix = measurementText + "_";
                if (seriesName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                {
                    measurement = measurementText;
                    fieldName = seriesName.Substring(prefix.Length);
                    if (string.IsNullOrWhiteSpace(fieldName))
                        fieldName = "value";
                    return;
                }
            }

            var separatorIndex = seriesName.LastIndexOf('_');
            if (separatorIndex > 0 && separatorIndex < seriesName.Length - 1)
            {
                measurement = seriesName.Substring(0, separatorIndex);
                fieldName = seriesName.Substring(separatorIndex + 1);
                return;
            }

            measurement = seriesName;
            fieldName = "value";
        }

        private static bool TryReadUnixMilliseconds(JsonElement element, out long timestampMs)
        {
            if (element.ValueKind == JsonValueKind.Number && element.TryGetInt64(out timestampMs))
                return true;

            if (element.ValueKind == JsonValueKind.String && long.TryParse(element.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out timestampMs))
                return true;

            timestampMs = 0;
            return false;
        }

        private static long ReadPrometheusSeconds(JsonElement element)
        {
            if (element.ValueKind == JsonValueKind.Number && element.TryGetDouble(out var numericSeconds))
                return (long)(numericSeconds * 1000);

            if (element.ValueKind == JsonValueKind.String && double.TryParse(element.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out numericSeconds))
                return (long)(numericSeconds * 1000);

            return 0;
        }

        private static DateTime ConvertFromUnixMilliseconds(long timestampMs)
        {
            try
            {
                return DateTimeOffset.FromUnixTimeMilliseconds(timestampMs).UtcDateTime;
            }
            catch (ArgumentOutOfRangeException)
            {
                return DateTime.UtcNow;
            }
        }

        private static object? ConvertMetricValue(JsonElement element)
        {
            return element.ValueKind switch
            {
                JsonValueKind.Number when element.TryGetInt64(out var longValue) => longValue,
                JsonValueKind.Number when element.TryGetDouble(out var doubleValue) => doubleValue,
                JsonValueKind.String => ParseStringMetricValue(element.GetString()),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null,
                _ => element.ToString(),
            };
        }

        private static object? ParseStringMetricValue(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return value;

            if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var longValue))
                return longValue;

            if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var doubleValue))
                return doubleValue;

            if (bool.TryParse(value, out var boolValue))
                return boolValue;

            return value;
        }

        #endregion
    }
}