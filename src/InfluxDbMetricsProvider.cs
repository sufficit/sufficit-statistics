using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Sufficit.Statistics
{
    /// <summary>
    /// InfluxDB v3 implementation of IMetricsProvider
    /// Provides high-performance time-series metrics storage using InfluxDB Line Protocol
    /// Supports batch operations and optimized for high-throughput metric ingestion
    /// Compatible with InfluxDB v3 (Flight SQL API and enhanced Line Protocol)
    /// </summary>
    public class InfluxDbMetricsProvider : IMetricsProvider
    {
        #region Fields

        private readonly ILogger<InfluxDbMetricsProvider> _logger;
        private readonly HttpClient _httpClient;
        private readonly InfluxDbConfiguration _config;
        private string _writeEndpoint;
        private string _queryEndpoint;
        private readonly string[] _possibleWriteEndpoints;
        private readonly string[] _possibleQueryEndpoints;

        // Circuit Breaker fields
        private DateTime _circuitBreakerOpenUntil = DateTime.MinValue;
        private int _consecutiveFailures = 0;
        private int _discardedMetricsCount = 0;
        private readonly object _circuitBreakerLock = new object();
        private const int CIRCUIT_BREAKER_FAILURE_THRESHOLD = 3;
        private static readonly TimeSpan CIRCUIT_BREAKER_TIMEOUT = TimeSpan.FromMinutes(5);

        #endregion
        #region Constructor

        /// <summary>
        /// Initialize InfluxDB metrics provider
        /// </summary>
        /// <param name="logger">Logger instance</param>
        /// <param name="httpClient">HTTP client for InfluxDB communication</param>
        /// <param name="config">InfluxDB configuration</param>
        public InfluxDbMetricsProvider(
            ILogger<InfluxDbMetricsProvider> logger,
            HttpClient httpClient,
            InfluxDbConfiguration config)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _config = config ?? throw new ArgumentNullException(nameof(config));

            // Configure HTTP client
            _httpClient.Timeout = TimeSpan.FromMilliseconds(_config.TimeoutMs);
            // Use configurable authorization scheme (Bearer for v3, Token for v2)
            _httpClient.DefaultRequestHeaders.Add("Authorization", $"{_config.AuthorizationScheme} {_config.Token}");
            _httpClient.DefaultRequestHeaders.Add("Accept", "application/json");

            // Build endpoints for InfluxDB v3
            var baseUrl = _config.Url.TrimEnd('/');
            
            // Normalize precision for URL (InfluxDB expects abbreviated forms)
            var urlPrecision = _config.Precision switch
            {
                "second" or "seconds" => "s",
                "millisecond" or "milliseconds" or "ms" => "ms",
                "microsecond" or "microseconds" or "us" => "us",
                "nanosecond" or "nanoseconds" or "ns" => "ns",
                _ => "ms" // Default to milliseconds
            };
            
            // InfluxDB v3 endpoints only (Sufficit infrastructure is v3-only)
            _possibleWriteEndpoints = new[]
            {
                $"{baseUrl}/write?db={_config.Database}&precision={urlPrecision}",  // v3 write endpoint
            };
            
            // InfluxDB v3 query endpoints only
            _possibleQueryEndpoints = new[]
            {
                $"{baseUrl}/query?db={_config.Database}",           // v3 Flux/InfluxQL query endpoint
                $"{baseUrl}/api/v3/query_sql",                      // v3 SQL query endpoint (alternative)
            };
            
            // Start with the first endpoint
            _writeEndpoint = _possibleWriteEndpoints[0];
            _queryEndpoint = _possibleQueryEndpoints[0];

            _logger.LogInformation("🔌 InfluxDbMetricsProvider v3 initialized - Database: {Database}, Endpoint: {WriteEndpoint}, Auth: {AuthScheme}", 
                _config.Database, _writeEndpoint, _config.AuthorizationScheme);
                
            _logger.LogDebug("🔧 Alternative endpoints available - Write: [{WriteEndpoints}], Query: [{QueryEndpoints}]",
                string.Join(", ", _possibleWriteEndpoints), string.Join(", ", _possibleQueryEndpoints));
        }

        #endregion

        #region IMetricsProvider Implementation

        /// <summary>
        /// Write a single metric value with optional tags
        /// </summary>
        public async Task WriteAsync<T>(
            string measurement, 
            T value, 
            Dictionary<string, string>? tags = null, 
            DateTime? timestamp = null, 
            CancellationToken cancellationToken = default) where T : struct
        {
            if (string.IsNullOrWhiteSpace(measurement))
                throw new ArgumentException("Measurement name cannot be null or empty", nameof(measurement));

            var metric = new Metric
            {
                Measurement = measurement,
                Fields = new Dictionary<string, object> { { "value", value } },
                Tags = tags ?? new Dictionary<string, string>(),
                Timestamp = timestamp ?? DateTime.UtcNow
            };

            await WriteMetricAsync(metric, cancellationToken);
            
            _logger.LogDebug("📝 InfluxDbMetricsProvider: Single metric '{Measurement}' written successfully", measurement);
        }

        /// <summary>
        /// Write a single complete metric
        /// </summary>
        public async Task WriteAsync(Metric metric, CancellationToken cancellationToken = default)
        {
            if (metric == null)
                throw new ArgumentNullException(nameof(metric));
            
            await WriteMetricAsync(metric, cancellationToken);
            
            _logger.LogDebug("📝 InfluxDbMetricsProvider: Metric '{Measurement}' written successfully", metric.Measurement);
        }

        /// <summary>
        /// Write multiple individual metrics in bulk operation
        /// </summary>
        public Task WriteBulkAsync(
            IEnumerable<Metric> metrics, 
            CancellationToken cancellationToken = default)
            => WriteBulkAsync(metrics, _config.BatchSize, cancellationToken);        

        /// <summary>
        /// Write multiple individual metrics in bulk operation with custom batch size
        /// </summary>
        /// <param name="metrics">Collection of metrics to write</param>
        /// <param name="batchSize">Custom batch size (optional). If null, uses configured BatchSize from settings</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public async Task WriteBulkAsync(
            IEnumerable<Metric> metrics, 
            int? batchSize = null,
            CancellationToken cancellationToken = default)
        {
            // Use custom batchSize or fall back to configured BatchSize
            var effectiveBatchSize = batchSize ?? _config.BatchSize;
            
            // Validate batch size
            if (effectiveBatchSize <= 0)
            {
                _logger.LogWarning("⚠️ Invalid batch size {BatchSize}, using default {DefaultBatchSize}", 
                    effectiveBatchSize, _config.BatchSize);
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
                _logger.LogDebug("📦 InfluxDbMetricsProvider: Batch {CurrentBatch} processed ({ProcessedCount} metrics total)", 
                    processedBatches, processedCount);
                batch.Clear();
            }

            if (batch.Count > 0)
            {
                await WriteBatchAsync(batch, cancellationToken);
                processedBatches++;
                processedCount += batch.Count;
                _logger.LogDebug("📦 InfluxDbMetricsProvider: Batch {CurrentBatch} processed ({ProcessedCount} metrics total)", 
                    processedBatches, processedCount);
            }

            if (processedCount == 0)
            {
                return;
            }

            _logger.LogDebug("✅ InfluxDbMetricsProvider: Successfully bulk inserted {MetricCount} metrics in {BatchCount} batches (batch size: {BatchSize})", 
                processedCount, processedBatches, effectiveBatchSize);
        }

        /// <summary>
        /// Query metrics using InfluxDB Flux query language
        /// Supports InfluxDB v3 SQL and InfluxQL endpoints
        /// </summary>
        public async Task<IEnumerable<Metric>> QueryAsync(
            string query, 
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(query))
                throw new ArgumentException("Query cannot be null or empty", nameof(query));

            // Determine content type based on query type and endpoint
            string contentType;
            StringContent content;
            
            // InfluxDB v3: Use Flux queries with appropriate content type
            if (!query.Contains("SELECT") && !query.Contains("FROM"))
            {
                // This is a Flux query
                contentType = "application/vnd.flux";
                content = new StringContent(query, Encoding.UTF8, contentType);
            }
            else if (_queryEndpoint.Contains("/sql"))
            {
                // For SQL endpoint, wrap query in JSON
                contentType = "application/json";
                var queryJson = JsonSerializer.Serialize(new { query = query });
                content = new StringContent(queryJson, Encoding.UTF8, contentType);
            }
            else if (_queryEndpoint.Contains("/influxql") || _queryEndpoint.Contains("/query?db="))
            {
                // For InfluxQL endpoint or legacy query endpoint
                contentType = "application/x-www-form-urlencoded";
                content = new StringContent($"q={Uri.EscapeDataString(query)}", Encoding.UTF8, contentType);
            }
            else
            {
                // For Flux queries (v2 compatibility)
                contentType = "application/vnd.flux";
                content = new StringContent(query, Encoding.UTF8, contentType);
            }
            
            try
            {
                var response = await _httpClient.PostAsync(_queryEndpoint, content, cancellationToken);
                response.EnsureSuccessStatusCode();

                var responseContent = await response.Content.ReadAsStringAsync();
                
                // Parse response based on content type detection (not just endpoint)
                IEnumerable<Metric> results;
                
                // Detect response format
                var trimmedResponse = responseContent.TrimStart();
                bool isCsvResponse = trimmedResponse.StartsWith("_measurement") || 
                                   (trimmedResponse.Contains("_measurement") && 
                                    trimmedResponse.Contains("_time") && 
                                    !trimmedResponse.StartsWith("{"));
                
                if (isCsvResponse)
                {
                    // CSV response (InfluxDB v3 default format)
                    _logger.LogDebug("🔍 InfluxDbMetricsProvider: Detected CSV response format, using Flux parser");
                    results = ParseFluxResponse(responseContent);
                }
                else if (_queryEndpoint.Contains("/query?db=") || _queryEndpoint.Contains("/influxql"))
                {
                    // JSON response for InfluxQL endpoints
                    _logger.LogDebug("🔍 InfluxDbMetricsProvider: Using InfluxQL JSON parser for endpoint {Endpoint}", _queryEndpoint);
                    results = ParseInfluxQlResponse(responseContent);
                }
                else
                {
                    // Default to Flux parser for other endpoints
                    _logger.LogDebug("🔍 InfluxDbMetricsProvider: Using Flux parser for endpoint {Endpoint}", _queryEndpoint);
                    results = ParseFluxResponse(responseContent);
                }

                _logger.LogDebug("🔍 InfluxDbMetricsProvider: Query executed successfully on {Endpoint}, returned {ResultCount} metrics", 
                    _queryEndpoint, results.Count());
                return results;
            }
            catch (HttpRequestException ex) when (ex.Message.Contains("404") || ex.Message.Contains("400"))
            {
                // Try endpoint discovery for query endpoints
                _logger.LogWarning("🔄 InfluxDbMetricsProvider: Query endpoint '{Endpoint}' failed with {StatusCode}, attempting endpoint discovery", 
                    _queryEndpoint, ex.Message.Contains("404") ? "404" : "400");
                
                if (await TryDiscoverCorrectQueryEndpoint(content, cancellationToken))
                {
                    _logger.LogInformation("🔄 InfluxDB query endpoint discovered - switching to: {NewEndpoint}", _queryEndpoint);
                    
                    // Retry with new endpoint
                    var retryResponse = await _httpClient.PostAsync(_queryEndpoint, content, cancellationToken);
                    retryResponse.EnsureSuccessStatusCode();

                    var retryResponseContent = await retryResponse.Content.ReadAsStringAsync();
                    
                    // Parse response based on content type detection (not just endpoint)
                    IEnumerable<Metric> results;
                    
                    // Detect response format
                    var trimmedRetryResponse = retryResponseContent.TrimStart();
                    bool isCsvRetryResponse = trimmedRetryResponse.StartsWith("_measurement") || 
                                             (trimmedRetryResponse.Contains("_measurement") && 
                                              trimmedRetryResponse.Contains("_time") && 
                                              !trimmedRetryResponse.StartsWith("{"));
                    
                    if (isCsvRetryResponse)
                    {
                        // CSV response (InfluxDB v3 default format)
                        _logger.LogDebug("🔍 InfluxDbMetricsProvider: Detected CSV response format after endpoint discovery, using Flux parser");
                        results = ParseFluxResponse(retryResponseContent);
                    }
                    else if (_queryEndpoint.Contains("/query?db=") || _queryEndpoint.Contains("/influxql"))
                    {
                        // JSON response for InfluxQL endpoints
                        _logger.LogDebug("🔍 InfluxDbMetricsProvider: Raw InfluxQL response after endpoint discovery (first 500 chars): {Response}", 
                            retryResponseContent.Length > 500 ? retryResponseContent.Substring(0, 500) + "..." : retryResponseContent);
                        results = ParseInfluxQlResponse(retryResponseContent);
                    }
                    else
                    {
                        // Default to Flux parser for other endpoints
                        results = ParseFluxResponse(retryResponseContent);
                    }

                    _logger.LogDebug("🔍 InfluxDbMetricsProvider: Query executed successfully after endpoint discovery on {Endpoint}, returned {ResultCount} metrics", 
                        _queryEndpoint, results.Count());
                    return results;
                }
                else
                {
                    _logger.LogWarning("⚠️ InfluxDB query endpoint discovery failed - no working endpoint found");
                }
                
                // If discovery failed, throw original exception
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ InfluxDbMetricsProvider: Query failed on {Endpoint} - {Query}", _queryEndpoint, query);
                throw;
            }
        }

        /// <summary>
        /// Search metrics using structured parameters
        /// Converts MetricsSearchParameters to appropriate query format based on endpoint
        /// </summary>
        public async Task<IEnumerable<Metric>> SearchAsync(
            MetricsSearchParameters parameters, 
            CancellationToken cancellationToken = default)
        {
            string query;
            
            // Build query based on endpoint type (InfluxDB v3)
            // Prioritize Flux queries for better performance
            if (_queryEndpoint.Contains("/sql"))
            {
                query = BuildSqlQuery(parameters);
            }
            else if (_queryEndpoint.Contains("/influxql") || _queryEndpoint.Contains("/query?db="))
            {
                query = BuildFluxQuery(parameters); // Use Flux even for legacy endpoints
            }
            else
            {
                query = BuildFluxQuery(parameters);
            }
            
            try
            {
                return await QueryAsync(query, cancellationToken);
            }
            catch (HttpRequestException ex) when (ex.Message.Contains("404"))
            {
                // Bucket doesn't exist - try to find available buckets and log helpful info
                _logger.LogWarning("🔄 InfluxDbMetricsProvider: Bucket '{Database}' not found, attempting to discover available buckets", _config.Database);
                
                try
                {
                    string bucketsQuery;
                    if (_queryEndpoint.Contains("/sql"))
                    {
                        // InfluxDB v3 SQL: Try to query system tables or information schema
                        // Different approaches for InfluxDB v3
                        bucketsQuery = "SELECT table_name FROM information_schema.tables LIMIT 10";
                    }
                    else if (_queryEndpoint.Contains("/influxql") || _queryEndpoint.Contains("/query?db="))
                    {
                        // InfluxQL: Show measurements (what we usually want instead of databases)
                        bucketsQuery = "SHOW MEASUREMENTS";
                    }
                    else
                    {
                        // Flux query for v2 compatibility
                        bucketsQuery = "buckets() |> filter(fn: (r) => r.name != \"\") |> yield()";
                    }
                    
                    var buckets = await QueryAsync(bucketsQuery, cancellationToken);
                    
                    var bucketNames = buckets.Select(b => b.Measurement).Distinct().ToList();
                    _logger.LogError("❌ InfluxDbMetricsProvider: Available buckets in InfluxDB: [{Buckets}]. Configure 'Database' setting to match an existing bucket.", 
                        string.Join(", ", bucketNames));
                }
                catch (Exception bucketEx)
                {
                    _logger.LogError(bucketEx, "❌ InfluxDbMetricsProvider: Failed to list available buckets. Check InfluxDB connection and permissions.");
                }
                
                // Return empty result instead of throwing
                _logger.LogWarning("🔄 InfluxDbMetricsProvider: Returning empty result due to missing bucket");
                return Array.Empty<Metric>();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ InfluxDbMetricsProvider: Query failed with unexpected error");
                throw;
            }
        }

        /// <summary>
        /// Check if InfluxDB is healthy and accessible
        /// </summary>
        public async Task<bool> IsHealthyAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                var healthEndpoint = $"{_config.Url.TrimEnd('/')}/ping";
                var response = await _httpClient.GetAsync(healthEndpoint, cancellationToken);
                
                if (response.IsSuccessStatusCode)
                {
                    _logger.LogDebug("✅ InfluxDbMetricsProvider: Health check passed");
                    
                    // Test authentication by attempting a simple write test
                    await TestAuthentication(cancellationToken);
                    
                    return true;
                }
                
                _logger.LogWarning("⚠️ InfluxDbMetricsProvider: Health check failed with status {StatusCode}", response.StatusCode);
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ InfluxDbMetricsProvider: Health check error");
                return false;
            }
        }

        /// <summary>
        /// Test authentication by sending a minimal test write
        /// </summary>
        private async Task TestAuthentication(CancellationToken cancellationToken)
        {
            // Create proper Line Protocol format: measurement,tag=value field=value timestamp
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000000; // Convert ms to ns
            var testPayload = $"_internal_test,type=health_check value=1 {timestamp}";
            var content = new StringContent(testPayload, Encoding.UTF8, "application/x-line-protocol");

            var response = await _httpClient.PostAsync(_writeEndpoint, content, cancellationToken);

            if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
            {
                var authHeader = _httpClient.DefaultRequestHeaders.Authorization?.ToString() ?? "MISSING";
                throw new UnauthorizedAccessException($"InfluxDB authentication failed. Auth header: {authHeader.Substring(0, Math.Min(20, authHeader.Length))}..., Endpoint: {_writeEndpoint}");
            }

            if (!response.IsSuccessStatusCode)
            {
                var responseBody = await response.Content.ReadAsStringAsync();
                throw new HttpRequestException($"InfluxDB test write failed: {response.StatusCode} - {responseBody}");
            }
        }

        #endregion

        #region Internal Methods

        /// <summary>
        /// Write a single metric to InfluxDB using Line Protocol
        /// </summary>
        private async Task WriteMetricAsync(Metric metric, CancellationToken cancellationToken)
        {
            var lineProtocol = ConvertToLineProtocol(metric);
            await WriteLineProtocolAsync(lineProtocol, cancellationToken);
        }

        /// <summary>
        /// Write a batch of metrics to InfluxDB using Line Protocol
        /// </summary>
        private async Task WriteBatchAsync(List<Metric> metrics, CancellationToken cancellationToken)
        {
            var lineProtocolLines = metrics.Select(ConvertToLineProtocol);
            var lineProtocol = string.Join("\n", lineProtocolLines);
            
            // Log payload size information
            var payloadSizeKB = Encoding.UTF8.GetByteCount(lineProtocol) / 1024.0;
            _logger.LogDebug("📦 InfluxDB batch: {MetricCount} metrics, {PayloadSize:F1} KB payload", 
                metrics.Count, payloadSizeKB);
            
            await WriteLineProtocolAsync(lineProtocol, cancellationToken);
        }

        /// <summary>
        /// Write line protocol data to InfluxDB with retry logic and circuit breaker
        /// </summary>
        private async Task WriteLineProtocolAsync(string lineProtocol, CancellationToken cancellationToken)
        {
            // Check circuit breaker state
            lock (_circuitBreakerLock)
            {
                if (DateTime.UtcNow < _circuitBreakerOpenUntil)
                {
                    _discardedMetricsCount++;
                    _logger.LogDebug("⚡ Circuit breaker OPEN - Discarding metric write #{Count} (reopens in {Seconds}s)", 
                        _discardedMetricsCount, (_circuitBreakerOpenUntil - DateTime.UtcNow).TotalSeconds);
                    return; // Discard write silently
                }
                
                // If circuit was open and timeout expired, try health check
                if (_circuitBreakerOpenUntil > DateTime.MinValue && DateTime.UtcNow >= _circuitBreakerOpenUntil)
                {
                    _logger.LogInformation("🔄 Circuit breaker timeout expired - Attempting health check");
                }
            }

            // Use proper Content-Type for InfluxDB v3 Line Protocol
            // CRITICAL FIX: Using statement ensures StringContent is properly disposed at method end
            // This prevents connection leak and resource exhaustion (1000+ TCP connections issue)
            using var content = new StringContent(lineProtocol, Encoding.UTF8, "application/x-line-protocol");
            const int maxRetries = 3;
            const int baseDelayMs = 1000;
            
            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    var response = await _httpClient.PostAsync(_writeEndpoint, content, cancellationToken);
                    
                    if (!response.IsSuccessStatusCode)
                    {
                        var responseBody = await response.Content.ReadAsStringAsync();
                        
                        // Try endpoint discovery for 401 errors (try discovery on every 401, not just first attempt)
                        if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                        {
                            if (await TryDiscoverCorrectEndpoint(content, cancellationToken))
                            {
                                _logger.LogInformation("🔄 InfluxDB endpoint discovered - switching to: {NewEndpoint}", _writeEndpoint);
                                continue; // Retry with new endpoint
                            }
                            else
                            {
                                _logger.LogWarning("⚠️ InfluxDB endpoint discovery failed - no working endpoint found");
                            }
                        }
                        
                        // Enhanced logging for authentication issues
                        if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                        {
                            var authHeader = _httpClient.DefaultRequestHeaders.Authorization?.ToString() ?? "MISSING";
                            _logger.LogError("❌ InfluxDB authentication failed - Status: {StatusCode}, Auth Header: {AuthHeader}, Endpoint: {Endpoint}, Response: {Response}", 
                                response.StatusCode, authHeader.Substring(0, Math.Min(20, authHeader.Length)) + "...", _writeEndpoint, responseBody);
                        }
                        else if (response.StatusCode == System.Net.HttpStatusCode.BadRequest)
                        {
                            _logger.LogError("❌ InfluxDB Bad Request (400) - Status: {StatusCode}, Response: {Response}, Endpoint: {Endpoint}", 
                                response.StatusCode, responseBody, _writeEndpoint);
                        }
                        else
                        {
                            _logger.LogError("❌ InfluxDB write failed - Status: {StatusCode}, Response: {Response}, Endpoint: {Endpoint}", 
                                response.StatusCode, responseBody, _writeEndpoint);
                        }
                    }
                    
                    response.EnsureSuccessStatusCode();
                    
                    // Success - close circuit breaker and reset failure counter
                    lock (_circuitBreakerLock)
                    {
                        if (_consecutiveFailures > 0 || _circuitBreakerOpenUntil > DateTime.MinValue)
                        {
                            if (_discardedMetricsCount > 0)
                            {
                                _logger.LogWarning("✅ InfluxDB connection restored - Circuit breaker CLOSED - {DiscardedCount} metrics were discarded during downtime", 
                                    _discardedMetricsCount);
                            }
                            else
                            {
                                _logger.LogInformation("✅ InfluxDB connection restored - Circuit breaker CLOSED");
                            }
                            
                            _consecutiveFailures = 0;
                            _circuitBreakerOpenUntil = DateTime.MinValue;
                            _discardedMetricsCount = 0;
                        }
                    }
                    
                    if (attempt > 1)
                    {
                        _logger.LogDebug("✅ InfluxDB write succeeded on attempt {Attempt}", attempt);
                    }
                    return;
                }
                catch (Exception ex) when (attempt < maxRetries && IsRetryableException(ex))
                {
                    var delay = TimeSpan.FromMilliseconds(baseDelayMs * Math.Pow(2, attempt - 1));
                    
                    // Don't log warnings for OperationCanceledException as it's normal when operations are cancelled
                    if (ex is OperationCanceledException)
                    {
                        _logger.LogDebug("🔄 InfluxDB write cancelled on attempt {Attempt}/{MaxRetries}, operation was cancelled by user or timeout", 
                            attempt, maxRetries);
                    }
                    else
                    {
                        _logger.LogWarning("⚠️ InfluxDB write failed on attempt {Attempt}/{MaxRetries}, retrying in {Delay}ms - {Error}", 
                            attempt, maxRetries, delay.TotalMilliseconds, ex.Message);
                    }
                    
                    await Task.Delay(delay, cancellationToken);
                }
                catch (Exception ex)
                {
                    // Don't log errors for OperationCanceledException as it's normal when operations are cancelled
                    if (ex is OperationCanceledException)
                    {
                        _logger.LogDebug("❌ InfluxDbMetricsProvider: Write cancelled after {Attempt} attempts - operation was cancelled by user or timeout", 
                            attempt);
                    }
                    else
                    {
                        _logger.LogError(ex, "❌ InfluxDbMetricsProvider: Write failed on attempt {Attempt}/{MaxRetries} - {LineProtocol}", 
                            attempt, maxRetries, lineProtocol.Substring(0, Math.Min(lineProtocol.Length, 200)));
                        
                        // Open circuit breaker after consecutive failures
                        lock (_circuitBreakerLock)
                        {
                            _consecutiveFailures++;
                            
                            if (_consecutiveFailures >= CIRCUIT_BREAKER_FAILURE_THRESHOLD)
                            {
                                _circuitBreakerOpenUntil = DateTime.UtcNow.Add(CIRCUIT_BREAKER_TIMEOUT);
                                _logger.LogWarning("⚡ Circuit breaker OPENED after {Failures} consecutive failures - Metrics will be discarded for {Minutes} minutes", 
                                    _consecutiveFailures, CIRCUIT_BREAKER_TIMEOUT.TotalMinutes);
                            }
                        }
                    }
                    throw;
                }
            }
        }

        /// <summary>
        /// Determine if an exception is retryable (network/timeout issues)
        /// </summary>
        private static bool IsRetryableException(Exception ex)
        {
            return ex is HttpRequestException ||
                   ex is OperationCanceledException ||
                   ex is SocketException ||
                   (ex.InnerException != null && IsRetryableException(ex.InnerException));
        }

        /// <summary>
        /// Try to discover the correct InfluxDB endpoint by testing alternatives
        /// </summary>
        private async Task<bool> TryDiscoverCorrectEndpoint(StringContent testContent, CancellationToken cancellationToken)
        {
            _logger.LogDebug("🔍 InfluxDB endpoint discovery - testing {Count} alternatives", _possibleWriteEndpoints.Length);
            
            // First, try to detect InfluxDB version for better endpoint prioritization
            await TryDetectInfluxDbVersion(cancellationToken);
            
            // Test token validity before trying endpoints
            await TestTokenValidity(cancellationToken);
            
            // Log authentication details for debugging
            var authHeader = _httpClient.DefaultRequestHeaders.Authorization;
            if (authHeader != null)
            {
                var tokenPreview = authHeader.Parameter?.Length > 10 
                    ? authHeader.Parameter.Substring(0, 10) + "..." 
                    : authHeader.Parameter;
                _logger.LogDebug("🔐 Using auth: {Scheme} {Token}", authHeader.Scheme, tokenPreview);
            }
            else
            {
                _logger.LogWarning("⚠️ No authentication header found in HttpClient!");
            }
            
            // Try all endpoints to find one that works
            for (int i = 0; i < _possibleWriteEndpoints.Length; i++)
            {
                var testEndpoint = _possibleWriteEndpoints[i];
                
                // Skip current failing endpoint
                if (testEndpoint == _writeEndpoint)
                {
                    _logger.LogDebug("⏭️ Skipping current failing endpoint: {Endpoint}", testEndpoint);
                    continue;
                }
                
                try
                {
                    _logger.LogDebug("🧪 Testing endpoint: {Endpoint}", testEndpoint);
                    
                    // Create a copy of the content with proper InfluxDB v3 Content-Type
                    var originalContent = await testContent.ReadAsStringAsync();
                    var testContentCopy = new StringContent(originalContent, Encoding.UTF8, "application/x-line-protocol");
                    
                    var response = await _httpClient.PostAsync(testEndpoint, testContentCopy, cancellationToken);
                    
                    if (response.IsSuccessStatusCode)
                    {
                        // Found working endpoint!
                        _writeEndpoint = testEndpoint;
                        _logger.LogInformation("✅ InfluxDB endpoint discovery successful - found working endpoint: {Endpoint}", testEndpoint);
                        return true;
                    }
                    
                    _logger.LogDebug("❌ Endpoint {Endpoint} failed with status: {StatusCode}", testEndpoint, response.StatusCode);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("❌ Endpoint {Endpoint} failed with exception: {Exception}", testEndpoint, ex.Message);
                }
            }
            
            _logger.LogWarning("❌ All endpoint alternatives failed during discovery");
            return false;
        }

        /// <summary>
        /// Try to discover the correct InfluxDB query endpoint by testing alternatives
        /// </summary>
        private async Task<bool> TryDiscoverCorrectQueryEndpoint(StringContent testContent, CancellationToken cancellationToken)
        {
            _logger.LogDebug("🔍 InfluxDB query endpoint discovery - testing {Count} alternatives", _possibleQueryEndpoints.Length);
            
            // First, try to detect InfluxDB version for better endpoint prioritization
            await TryDetectInfluxDbVersion(cancellationToken);
            
            // Test token validity before trying endpoints
            await TestTokenValidity(cancellationToken);
            
            // Log authentication details for debugging
            var authHeader = _httpClient.DefaultRequestHeaders.Authorization;
            if (authHeader != null)
            {
                var tokenPreview = authHeader.Parameter?.Length > 10 
                    ? authHeader.Parameter.Substring(0, 10) + "..." 
                    : authHeader.Parameter;
                _logger.LogDebug("🔐 Using auth: {Scheme} {Token}", authHeader.Scheme, tokenPreview);
            }
            else
            {
                _logger.LogWarning("⚠️ No authentication header found in HttpClient!");
            }
            
            // Try all query endpoints to find one that works
            for (int i = 0; i < _possibleQueryEndpoints.Length; i++)
            {
                var testEndpoint = _possibleQueryEndpoints[i];
                
                // Skip current failing endpoint
                if (testEndpoint == _queryEndpoint)
                {
                    _logger.LogDebug("⏭️ Skipping current failing query endpoint: {Endpoint}", testEndpoint);
                    continue;
                }
                
                try
                {
                    _logger.LogDebug("🧪 Testing query endpoint: {Endpoint}", testEndpoint);
                    
                    // Create a copy of the content with appropriate Content-Type based on endpoint
                    var originalContent = await testContent.ReadAsStringAsync();
                    StringContent testContentCopy;
                    
                    if (testEndpoint.Contains("/sql"))
                    {
                        // For SQL endpoint, wrap query in JSON
                        var queryJson = JsonSerializer.Serialize(new { query = originalContent });
                        testContentCopy = new StringContent(queryJson, Encoding.UTF8, "application/json");
                    }
                    else if (testEndpoint.Contains("/influxql") || testEndpoint.Contains("/query?db="))
                    {
                        // For InfluxQL endpoint
                        testContentCopy = new StringContent($"q={Uri.EscapeDataString(originalContent)}", Encoding.UTF8, "application/x-www-form-urlencoded");
                    }
                    else
                    {
                        // For Flux queries
                        testContentCopy = new StringContent(originalContent, Encoding.UTF8, "application/vnd.flux");
                    }
                    
                    var response = await _httpClient.PostAsync(testEndpoint, testContentCopy, cancellationToken);
                    
                    if (response.IsSuccessStatusCode)
                    {
                        // Found working endpoint!
                        _queryEndpoint = testEndpoint;
                        _logger.LogInformation("✅ InfluxDB query endpoint discovery successful - found working endpoint: {Endpoint}", testEndpoint);
                        return true;
                    }
                    
                    _logger.LogDebug("❌ Query endpoint {Endpoint} failed with status: {StatusCode}", testEndpoint, response.StatusCode);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("❌ Query endpoint {Endpoint} failed with exception: {Exception}", testEndpoint, ex.Message);
                }
            }
            
            _logger.LogWarning("❌ All query endpoint alternatives failed during discovery");
            return false;
        }

        /// <summary>
        /// Test token validity against InfluxDB instance
        /// </summary>
        private async Task TestTokenValidity(CancellationToken cancellationToken)
        {
            try
            {
                var baseUrl = _httpClient.BaseAddress?.ToString().TrimEnd('/') ?? "http://influx.sufficit.com.br:8086";
                
                // Test endpoints that should work with valid tokens
                var testEndpoints = new[]
                {
                    "/health",           // InfluxDB v3 health endpoint
                    "/api/v2/health",    // InfluxDB v2 health endpoint  
                    "/ping"              // Universal ping endpoint
                };
                
                _logger.LogDebug("🧪 Testing token validity against health endpoints...");
                
                foreach (var endpoint in testEndpoints)
                {
                    try
                    {
                        var testUrl = $"{baseUrl}{endpoint}";
                        _logger.LogDebug("🔍 Testing token against: {Endpoint}", testUrl);
                        
                        var response = await _httpClient.GetAsync(testUrl, cancellationToken);
                        
                        _logger.LogDebug("📊 Token test result for {Endpoint}: {StatusCode}", 
                            endpoint, response.StatusCode);
                            
                        if (response.IsSuccessStatusCode)
                        {
                            _logger.LogInformation("✅ Token appears valid - {Endpoint} returned {StatusCode}", 
                                endpoint, response.StatusCode);
                            return;
                        }
                        else if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                        {
                            _logger.LogWarning("🔒 Token authentication failed on {Endpoint} - 401 Unauthorized", endpoint);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug("❌ Token test failed for {Endpoint}: {Exception}", endpoint, ex.Message);
                    }
                }
                
                _logger.LogError("❌ Token appears to be invalid or has insufficient permissions for ALL test endpoints");
            }
            catch (Exception ex)
            {
                _logger.LogDebug("❌ Token validity test failed: {Exception}", ex.Message);
            }
        }

        /// <summary>
        /// Try to detect InfluxDB version using ping endpoint
        /// </summary>
        private async Task TryDetectInfluxDbVersion(CancellationToken cancellationToken)
        {
            try
            {
                var baseUrl = _httpClient.BaseAddress?.ToString().TrimEnd('/') ?? "http://influx.sufficit.com.br:8086";
                var pingUrl = $"{baseUrl}/ping";
                
                _logger.LogDebug("🏓 Pinging InfluxDB to detect version: {PingUrl}", pingUrl);
                
                var response = await _httpClient.GetAsync(pingUrl, cancellationToken);
                
                _logger.LogDebug("📡 InfluxDB ping response status: {StatusCode}", response.StatusCode);
                
                // Try to get version from headers
                string version = "Unknown";
                if (response.Headers.Contains("X-Influxdb-Version"))
                {
                    version = response.Headers.GetValues("X-Influxdb-Version").FirstOrDefault() ?? "Unknown";
                }
                else if (response.Headers.Contains("X-Influxdb-Build"))
                {
                    version = response.Headers.GetValues("X-Influxdb-Build").FirstOrDefault() ?? "Unknown";
                }
                
                _logger.LogInformation("📡 InfluxDB version detected: {Version} (Port: {Port})", version, _httpClient.BaseAddress?.Port ?? 8086);
                
                // Log all headers for debugging
                _logger.LogDebug("📋 InfluxDB ping response headers:");
                foreach (var header in response.Headers)
                {
                    _logger.LogDebug("   {HeaderName}: {HeaderValue}", header.Key, string.Join(", ", header.Value));
                }
                
                if (version.StartsWith("1."))
                {
                    _logger.LogInformation("🔍 Detected InfluxDB v1 - prioritizing legacy endpoints");
                }
                else if (version.StartsWith("2."))
                {
                    _logger.LogInformation("🔍 Detected InfluxDB v2 - prioritizing v2 API endpoints");
                }
                else if (version.StartsWith("3."))
                {
                    _logger.LogInformation("🔍 Detected InfluxDB v3 - prioritizing v3 API endpoints");
                }
                else
                {
                    _logger.LogWarning("⚠️ Unknown InfluxDB version: {Version} - will try all endpoint patterns", version);
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug("❌ Could not detect InfluxDB version via ping: {Exception}", ex.Message);
            }
        }

        /// <summary>
        /// Convert Metric object to InfluxDB Line Protocol format
        /// Format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
        /// </summary>
        private string ConvertToLineProtocol(Metric metric)
        {
            var sb = new StringBuilder();
            
            // Measurement name (escaped if necessary)
            sb.Append(EscapeLineProtocol(metric.Measurement ?? "unknown"));

            // Tags (sorted for consistency)
            if (metric.Tags?.Any() == true)
            {
                foreach (var tag in metric.Tags.OrderBy(t => t.Key))
                {
                    sb.Append($",{EscapeLineProtocol(tag.Key)}={EscapeLineProtocol(tag.Value)}");
                }
            }

            sb.Append(' ');

            // Fields
            if (metric.Fields?.Any() == true)
            {
                var fieldPairs = metric.Fields.Select(field => 
                    $"{EscapeLineProtocol(field.Key)}={FormatFieldValue(field.Value)}");
                sb.Append(string.Join(",", fieldPairs));
            }
            else
            {
                // Default field if no fields specified
                sb.Append("value=1");
            }

            // Timestamp (convert to nanoseconds or specified precision)
            if (metric.Timestamp != default(DateTime))
            {
                var timestamp = ConvertTimestamp(metric.Timestamp);
                sb.Append($" {timestamp}");
            }

            return sb.ToString();
        }

        /// <summary>
        /// Escape special characters in InfluxDB Line Protocol
        /// </summary>
        private string EscapeLineProtocol(string value)
        {
            if (string.IsNullOrEmpty(value)) return value;
            
            return value
                .Replace("\\", "\\\\")  // Escape backslashes first
                .Replace(",", "\\,")    // Escape commas
                .Replace("=", "\\=")    // Escape equals
                .Replace(" ", "\\ ");   // Escape spaces
        }

        /// <summary>
        /// Format field value according to InfluxDB type requirements
        /// </summary>
        private string FormatFieldValue(object? value)
        {
            if (value == null) return "null";

            return value switch
            {
                string s => $"\"{s.Replace("\"", "\\\"")}\"",  // String values need quotes
                bool b => b.ToString().ToLowerInvariant(),      // Boolean as lowercase
                float f => f.ToString("F6", System.Globalization.CultureInfo.InvariantCulture),
                double d => d.ToString("F6", System.Globalization.CultureInfo.InvariantCulture),
                decimal dec => dec.ToString("F6", System.Globalization.CultureInfo.InvariantCulture),
                _ => value.ToString() ?? "null"
            };
        }

        /// <summary>
        /// Convert DateTime to InfluxDB timestamp format based on precision
        /// </summary>
        private long ConvertTimestamp(DateTime timestamp)
        {
            var utcTimestamp = timestamp.Kind == DateTimeKind.Utc ? timestamp : timestamp.ToUniversalTime();
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            
            return _config.Precision switch
            {
                "s" or "second" or "seconds" => (long)(utcTimestamp - epoch).TotalSeconds,
                "ms" or "millisecond" or "milliseconds" => (long)(utcTimestamp - epoch).TotalMilliseconds,
                "us" or "microsecond" or "microseconds" => (long)((utcTimestamp - epoch).Ticks / 10), // Ticks to microseconds (1 tick = 100 nanoseconds)
                "ns" or "nanosecond" or "nanoseconds" => (long)(utcTimestamp - epoch).Ticks * 100, // Ticks to nanoseconds
                _ => (long)(utcTimestamp - epoch).TotalMilliseconds // Default to milliseconds
            };
        }

        /// <summary>
        /// Build InfluxDB v3 SQL query from structured search parameters
        /// </summary>
        private string BuildSqlQuery(MetricsSearchParameters parameters)
        {
            var query = new StringBuilder();
            
            // Basic SELECT with common fields
            query.Append("SELECT time, ");
            
            // Add specific fields if requested
            if (parameters.Fields?.Any() == true)
            {
                query.Append(string.Join(", ", parameters.Fields));
            }
            else
            {
                query.Append("*"); // Select all fields if none specified
            }
            
            // FROM clause with database/table
            if (parameters.Measurement?.IsValid == true)
            {
                query.Append($" FROM \"{_config.Database}\".\"{parameters.Measurement.Text}\"");
            }
            else
            {
                // If no specific measurement, we need to query available tables
                // This is a limitation - SQL needs specific table names
                query.Append($" FROM \"{_config.Database}\".\"*\""); // May not work, but attempt wildcard
            }
            
            var whereConditions = new List<string>();
            
            // Time range conditions
            if (parameters.Timestamp?.IsValid == true)
            {
                if (parameters.Timestamp.Start.HasValue)
                {
                    var startTime = parameters.Timestamp.Start.Value.ToString("yyyy-MM-dd HH:mm:ss");
                    whereConditions.Add($"time >= '{startTime}'");
                }
                
                if (parameters.Timestamp.End.HasValue)
                {
                    var endTime = parameters.Timestamp.End.Value.ToString("yyyy-MM-dd HH:mm:ss");
                    whereConditions.Add($"time <= '{endTime}'");
                }
            }
            else
            {
                // Default to last 24 hours
                whereConditions.Add("time >= NOW() - INTERVAL '24 hours'");
            }
            
            // Tag filters
            if (parameters.Tags?.Any() == true)
            {
                foreach (var tag in parameters.Tags)
                {
                    if (!string.IsNullOrWhiteSpace(tag.Value))
                    {
                        whereConditions.Add($"\"{tag.Key}\" = '{tag.Value.Replace("'", "''")}'");
                    }
                }
            }
            
            // Add WHERE clause if we have conditions
            if (whereConditions.Any())
            {
                query.Append(" WHERE ");
                query.Append(string.Join(" AND ", whereConditions));
            }
            
            // Order by time
            query.Append(" ORDER BY time DESC");
            
            // Limit if specified
            if (parameters.Limit.HasValue && parameters.Limit.Value > 0)
            {
                query.Append($" LIMIT {parameters.Limit.Value}");
            }
            
            return query.ToString();
        }

        /// <summary>
        /// Build InfluxDB v3 Flux query from structured search parameters
        /// InfluxDB v3 approach: try bucket syntax with proper escaping
        /// </summary>
        private string BuildFluxQuery(MetricsSearchParameters parameters)
        {
            var query = new StringBuilder();
            
            // InfluxDB v3: Use bucket syntax with database name
            query.AppendLine($"from(bucket: \"{_config.Database}\")");

            // Time range filter
            if (parameters.Timestamp?.IsValid == true)
            {
                if (parameters.Timestamp.Start.HasValue)
                {
                    var startTime = parameters.Timestamp.Start.Value.ToString("yyyy-MM-ddTHH:mm:ssZ");
                    query.AppendLine($"  |> range(start: {startTime})");
                }
                
                if (parameters.Timestamp.End.HasValue)
                {
                    var endTime = parameters.Timestamp.End.Value.ToString("yyyy-MM-ddTHH:mm:ssZ");
                    query.AppendLine($"  |> range(stop: {endTime})");
                }
            }
            else
            {
                // Default to last 24 hours if no time range specified
                query.AppendLine("  |> range(start: -24h)");
            }

            // Measurement filter
            if (parameters.Measurement?.IsValid == true)
            {
                if (parameters.Measurement.ExactMatch)
                {
                    query.AppendLine($"  |> filter(fn: (r) => r._measurement == \"{parameters.Measurement.Text}\")");
                }
                else
                {
                    query.AppendLine($"  |> filter(fn: (r) => r._measurement =~ /{parameters.Measurement.Text}/)");
                }
            }

            // Tag filters
            if (parameters.Tags?.Any() == true)
            {
                foreach (var tag in parameters.Tags)
                {
                    if (!string.IsNullOrWhiteSpace(tag.Value))
                    {
                        query.AppendLine($"  |> filter(fn: (r) => r.{tag.Key} == \"{tag.Value}\")");
                    }
                }
            }

            // Field selection
            if (parameters.Fields?.Any() == true)
            {
                var fields = string.Join(", ", parameters.Fields.Select(f => $"\"{f}\""));
                query.AppendLine($"  |> keep(columns: [\"_time\", \"_measurement\", {fields}])");
            }

            // Limit if specified
            if (parameters.Limit.HasValue && parameters.Limit.Value > 0)
            {
                query.AppendLine($"  |> limit(n: {parameters.Limit.Value})");
            }

            // Sort by time descending (most recent first)
            query.AppendLine("  |> sort(columns: [\"_time\"], desc: true)");

            return query.ToString();
        }

        /// <summary>
        /// Parse InfluxDB InfluxQL JSON response into Metric objects
        /// Handles the JSON format returned by /query?db= endpoint
        /// </summary>
        private IEnumerable<Metric> ParseInfluxQlResponse(string jsonResponse)
        {
            var metrics = new List<Metric>();
            
            try
            {
                // Validate response is not empty and doesn't start with invalid characters
                if (string.IsNullOrWhiteSpace(jsonResponse))
                {
                    _logger.LogWarning("⚠️ InfluxDbMetricsProvider: Received empty InfluxQL response");
                    return metrics;
                }
                
                // Check for common non-JSON responses (but allow valid CSV responses)
                var trimmedResponse = jsonResponse.TrimStart();
                
                // Allow CSV responses that start with _measurement (InfluxDB v3 format)
                bool isValidCsvResponse = trimmedResponse.StartsWith("_measurement") && 
                                        trimmedResponse.Contains("_time") && 
                                        trimmedResponse.Contains("_field") && 
                                        trimmedResponse.Contains("_value");
                
                if (!isValidCsvResponse && 
                    (trimmedResponse.StartsWith("_") || 
                     trimmedResponse.StartsWith("-") || 
                     trimmedResponse.StartsWith("{") == false && 
                     (trimmedResponse.Contains("error") || 
                      trimmedResponse.Contains("Error") || 
                      trimmedResponse.Contains("ERROR") ||
                      trimmedResponse.Contains("not found") ||
                      trimmedResponse.Contains("unauthorized") ||
                      trimmedResponse.Contains("forbidden"))))
                {
                    _logger.LogError("❌ InfluxDbMetricsProvider: Response appears to be invalid - doesn't start with '{{' or valid CSV header, and contains error keywords. This might be an error message. Full response: {Response}", 
                        jsonResponse.Length > 1000 ? jsonResponse.Substring(0, 1000) + "..." : jsonResponse);
                    return metrics;
                }
                
                // Check if response looks like HTML (error page)
                if (trimmedResponse.StartsWith("<") || 
                    trimmedResponse.Contains("<!DOCTYPE") || 
                    trimmedResponse.Contains("<html") ||
                    trimmedResponse.Contains("<body"))
                {
                    _logger.LogError("❌ InfluxDbMetricsProvider: Response appears to be HTML error page instead of JSON. Full response: {Response}", 
                        jsonResponse.Length > 1000 ? jsonResponse.Substring(0, 1000) + "..." : jsonResponse);
                    return metrics;
                }
                
                using var document = JsonDocument.Parse(jsonResponse);
                var root = document.RootElement;
                
                if (!root.TryGetProperty("results", out var results) || results.ValueKind != JsonValueKind.Array)
                {
                    _logger.LogWarning("⚠️ InfluxDbMetricsProvider: InfluxQL response missing 'results' array or invalid format. Root properties: {Properties}", 
                        string.Join(", ", root.EnumerateObject().Select(p => p.Name)));
                    return metrics;
                }
                
                foreach (var result in results.EnumerateArray())
                {
                    if (!result.TryGetProperty("series", out var series) || series.ValueKind != JsonValueKind.Array)
                        continue;
                        
                    foreach (var serie in series.EnumerateArray())
                    {
                        if (!serie.TryGetProperty("name", out var nameElement) ||
                            !serie.TryGetProperty("columns", out var columnsElement) ||
                            !serie.TryGetProperty("values", out var valuesElement))
                            continue;
                            
                        var measurementName = nameElement.GetString() ?? "unknown";
                        var columns = new List<string>();
                        
                        // Parse columns
                        foreach (var column in columnsElement.EnumerateArray())
                        {
                            columns.Add(column.GetString() ?? "");
                        }
                        
                        // Parse values (each row)
                        foreach (var valueRow in valuesElement.EnumerateArray())
                        {
                            var metric = new Metric
                            {
                                Measurement = measurementName,
                                Tags = new Dictionary<string, string>(),
                                Fields = new Dictionary<string, object>(),
                                Timestamp = DateTime.UtcNow
                            };
                            
                            var rowValues = valueRow.EnumerateArray().ToArray();
                            
                            for (int i = 0; i < columns.Count && i < rowValues.Length; i++)
                            {
                                var columnName = columns[i];
                                var valueElement = rowValues[i];
                                
                                if (columnName.Equals("time", StringComparison.OrdinalIgnoreCase))
                                {
                                    if (DateTime.TryParse(valueElement.GetString(), out var timestamp))
                                    {
                                        metric.Timestamp = timestamp;
                                    }
                                }
                                else
                                {
                                    // Add as field
                                    object? value = valueElement.ValueKind switch
                                    {
                                        JsonValueKind.String => valueElement.GetString() ?? "",
                                        JsonValueKind.Number => valueElement.TryGetDouble(out var d) ? d : 0,
                                        JsonValueKind.True => true,
                                        JsonValueKind.False => false,
                                        JsonValueKind.Null => null,
                                        _ => valueElement.ToString()
                                    };
                                    
                                    if (value != null)
                                    {
                                        metric.Fields[columnName] = value;
                                    }
                                }
                            }
                            
                            if (metric.Fields.Any())
                            {
                                metrics.Add(metric);
                            }
                        }
                    }
                }
            }
            catch (System.Text.Json.JsonException jsonEx)
            {
                _logger.LogError(jsonEx, "❌ InfluxDbMetricsProvider: Failed to parse InfluxQL JSON response - invalid JSON format. Response starts with: '{Start}'", 
                    jsonResponse.Length > 50 ? jsonResponse.Substring(0, 50) : jsonResponse);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ InfluxDbMetricsProvider: Failed to parse InfluxQL JSON response - unexpected error");
            }
            
            return metrics;
        }

        /// <summary>
        /// Parse InfluxDB response into Metric objects
        /// Handles both CSV format (InfluxDB v3) and JSON format (legacy)
        /// </summary>
        private IEnumerable<Metric> ParseFluxResponse(string response)
        {
            var metrics = new List<Metric>();
            
            try
            {
                var trimmedResponse = response.TrimStart();
                
                // Check if this is CSV format (starts with _measurement header)
                if (trimmedResponse.StartsWith("_measurement"))
                {
                    return ParseInfluxDbV3CsvResponse(response);
                }
                
                // Otherwise, try to parse as traditional Flux CSV
                var lines = response.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries);
                if (lines.Length < 2) return metrics; // Need at least header + data
                
                // Simple CSV parsing - production should use proper CSV library
                var headers = lines[0].Split(',');
                
                for (int i = 1; i < lines.Length; i++)
                {
                    if (string.IsNullOrWhiteSpace(lines[i]) || lines[i].StartsWith("#")) continue;
                    
                    var values = lines[i].Split(',');
                    if (values.Length != headers.Length) continue;
                    
                    var metric = ParseFluxRow(headers, values);
                    if (metric != null) metrics.Add(metric);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ InfluxDbMetricsProvider: Failed to parse response");
            }
            
            return metrics;
        }

        /// <summary>
        /// Parse InfluxDB v3 CSV response format
        /// Format: _measurement,_time,_field,_value
        /// Data: measurement_name,timestamp,field_name,field_value
        /// </summary>
        private IEnumerable<Metric> ParseInfluxDbV3CsvResponse(string csvResponse)
        {
            var metrics = new List<Metric>();
            
            try
            {
                var lines = csvResponse.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries);
                if (lines.Length < 2) return metrics; // Need at least header + data
                
                // Verify header format
                var header = lines[0].Trim();
                if (!header.Equals("_measurement,_time,_field,_value", StringComparison.OrdinalIgnoreCase))
                {
                    _logger.LogWarning("⚠️ InfluxDbMetricsProvider: Unexpected CSV header format: {Header}", header);
                    return metrics;
                }
                
                // Parse data rows
                for (int i = 1; i < lines.Length; i++)
                {
                    var line = lines[i].Trim();
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    
                    var parts = line.Split(',');
                    if (parts.Length != 4)
                    {
                        _logger.LogWarning("⚠️ InfluxDbMetricsProvider: Invalid CSV row format (expected 4 columns): {Row}", line);
                        continue;
                    }
                    
                    try
                    {
                        var measurement = parts[0].Trim();
                        var timeStr = parts[1].Trim();
                        var fieldName = parts[2].Trim();
                        var fieldValueStr = parts[3].Trim();
                        
                        // Parse timestamp
                        if (!DateTime.TryParse(timeStr, out var timestamp))
                        {
                            _logger.LogWarning("⚠️ InfluxDbMetricsProvider: Invalid timestamp format: {Timestamp}", timeStr);
                            continue;
                        }
                        
                        // Parse field value
                        object fieldValue;
                        if (double.TryParse(fieldValueStr, out var numValue))
                        {
                            fieldValue = numValue;
                        }
                        else if (fieldValueStr.Equals("true", StringComparison.OrdinalIgnoreCase))
                        {
                            fieldValue = true;
                        }
                        else if (fieldValueStr.Equals("false", StringComparison.OrdinalIgnoreCase))
                        {
                            fieldValue = false;
                        }
                        else
                        {
                            fieldValue = fieldValueStr;
                        }
                        
                        var metric = new Metric
                        {
                            Measurement = measurement,
                            Timestamp = timestamp,
                            Tags = new Dictionary<string, string>(),
                            Fields = new Dictionary<string, object> { { fieldName, fieldValue } }
                        };
                        
                        metrics.Add(metric);
                        _logger.LogDebug("📊 InfluxDbMetricsProvider: Parsed metric - Measurement: {Measurement}, Time: {Time}, Field: {Field}={Value}", 
                            measurement, timestamp, fieldName, fieldValue);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "⚠️ InfluxDbMetricsProvider: Failed to parse CSV row: {Row}", line);
                    }
                }
                
                _logger.LogDebug("✅ InfluxDbMetricsProvider: Successfully parsed {Count} metrics from InfluxDB v3 CSV response", metrics.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ InfluxDbMetricsProvider: Failed to parse InfluxDB v3 CSV response");
            }
            
            return metrics;
        }

        /// <summary>
        /// Parse a single row from Flux CSV response
        /// </summary>
        private Metric? ParseFluxRow(string[] headers, string[] values)
        {
            try
            {
                var metric = new Metric
                {
                    Tags = new Dictionary<string, string>(),
                    Fields = new Dictionary<string, object>()
                };

                for (int i = 0; i < headers.Length && i < values.Length; i++)
                {
                    var header = headers[i].Trim();
                    var value = values[i].Trim();
                    
                    switch (header)
                    {
                        case "_measurement":
                            metric.Measurement = value;
                            break;
                        case "_time":
                            if (DateTime.TryParse(value, out var timestamp))
                                metric.Timestamp = timestamp;
                            break;
                        case "_field":
                            // Field name - will be used with _value
                            break;
                        case "_value":
                            if (double.TryParse(value, out var numValue))
                                metric.Fields["value"] = numValue;
                            else
                                metric.Fields["value"] = value;
                            break;
                        default:
                            // Other columns are typically tags
                            if (!header.StartsWith("_") && !string.IsNullOrEmpty(value))
                                metric.Tags[header] = value;
                            break;
                    }
                }

                return string.IsNullOrEmpty(metric.Measurement) ? null : metric;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ InfluxDbMetricsProvider: Failed to parse Flux row");
                return null;
            }
        }

        #endregion
    }
}
