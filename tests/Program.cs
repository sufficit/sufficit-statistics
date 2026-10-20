using Sufficit.Statistics;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Concurrent;
using System.Net;
using System.Reflection;

static void Check(bool condition, string message) { if (!condition) throw new Exception(message); }
static async Task Until(Func<bool> condition)
{
    for (var i = 0; i < 300; i++) { if (condition()) return; await Task.Delay(20); }
    throw new Exception("Timed out awaiting queue state.");
}
static Metric Item(int value = 1) => new() { Measurement = "queue_test", Fields = new() { ["value"] = value, ["bool"] = true, ["text"] = "a\"b", ["decimal"] = 1.23456789m } };
static MetricsQueueOptions Options(string path) => new() { DirectoryPath = path, PollInterval = TimeSpan.FromMilliseconds(10), MaxRetryDelay = TimeSpan.FromMilliseconds(40), BatchSize = 2 };
if (args.Length == 2 && args[0] == "crash-writer")
{
    var child = new StatisticsRuntime(new FakeProvider { Fail = true }, NullLogger<StatisticsRuntime>.Instance, Options(args[1]));
    child.EnqueueBatch(new[] { Item(1), Item(2) });
    Environment.Exit(0); // Intentionally no Dispose/host shutdown.
}
var root = Path.Combine(Path.GetTempPath(), "metrics-tests-" + Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(root);
try
{
    var crashPath = Path.Combine(root, "crash");
    var start = new System.Diagnostics.ProcessStartInfo("dotnet") { UseShellExecute = false };
    start.ArgumentList.Add(typeof(FakeProvider).Assembly.Location);
    start.ArgumentList.Add("crash-writer"); start.ArgumentList.Add(crashPath);
    using (var child = System.Diagnostics.Process.Start(start)!)
    {
        Check(child.WaitForExit(15000) && child.ExitCode == 0, "Crash writer failed");
    }
    var recovered = new FakeProvider();
    using (var runtime = new StatisticsRuntime(recovered, NullLogger<StatisticsRuntime>.Instance, Options(crashPath)))
    {
        await Until(() => runtime.GetCachedMetricsCount() == 0);
        Check(recovered.Received.Count == 2, "Accepted batch must survive abrupt process exit");
    }
    Console.WriteLine("PASS recovery after process exit without shutdown");

    var options = Options(Path.Combine(root, "restart"));
    var failing = new FakeProvider { Fail = true };
    using (var runtime = new StatisticsRuntime(failing, NullLogger<StatisticsRuntime>.Instance, options))
    {
        runtime.EnqueueBatch(new[] { Item(0), Item(1) });
        for (var i = 2; i < 5; i++) runtime.Enqueue(Item(i));
        try { runtime.EnqueueBatch(new[] { Item(), new Metric() }); throw new Exception("Expected invalid batch rejection"); } catch (ArgumentException) { }
        Check(runtime.GetCachedMetricsCount() == 5, "Invalid batch must roll back all inserts");
        await Until(() => runtime.GetQueueStatus().DeliveryFailures > 0);
        Check(runtime.GetCachedMetricsCount() == 5, "Failed delivery must retain all pending metrics");
        Check(runtime.GetQueueStatus().DatabaseBytes > 0, "Queue must exist on disk");
        var locked = false;
        try { using var duplicate = new StatisticsRuntime(failing, NullLogger<StatisticsRuntime>.Instance, options); }
        catch (IOException) { locked = true; }
        Check(locked, "Second owner must not acquire same spool");
        using var canceled = new CancellationTokenSource(); canceled.Cancel();
        try { await runtime.HandleAsync(Item(), canceled.Token); throw new Exception("Expected cancellation"); } catch (OperationCanceledException) { }
        Check(runtime.GetCachedMetricsCount() == 5, "Canceled acceptance must not enqueue");
        try { runtime.Enqueue(new Metric()); throw new Exception("Expected invalid metric rejection"); } catch (ArgumentException) { }
    }
    var healthy = new FakeProvider();
    using (var runtime = new StatisticsRuntime(healthy, NullLogger<StatisticsRuntime>.Instance, options))
    {
        await Until(() => runtime.GetCachedMetricsCount() == 0);
        Check(healthy.Received.Count == 5 && healthy.LargestBatch <= 2, "Restart must drain bounded batches without new enqueue");
        var item = healthy.Received.First();
        Check(item.Fields["value"] is int && item.Fields["bool"] is true && item.Fields["text"] is string && item.Fields["decimal"] is decimal, "Primitive types must survive disk");
        Check(item.Timestamp != default && item.Timestamp.Kind == DateTimeKind.Utc, "Stable UTC timestamp assigned before persistence");
        Check(runtime.GetQueueStatus().LastDeliveryUtc.HasValue, "Delivery timestamp missing");
    }
    Console.WriteLine("PASS restart, failure retention, bounded batches, types, cancellation, ownership");

    var quota = Options(Path.Combine(root, "quota")); quota.MaxDatabaseBytes = 65536; quota.MaxMetricBytes = 8192;
    using (var runtime = new StatisticsRuntime(failing, NullLogger<StatisticsRuntime>.Instance, quota))
    {
        var failed = false; var accepted = 0;
        for (var i = 0; i < 100; i++)
        {
            var metric = Item(); metric.Fields["text"] = new string('x', 4000);
            try { runtime.Enqueue(metric); accepted++; } catch (Microsoft.Data.Sqlite.SqliteException) { failed = true; break; }
        }
        Check(failed && accepted > 0, "Disk quota must reject instead of silently dropping");
        Check(runtime.GetCachedMetricsCount() == accepted, "Accepted data must remain after full disk");
        Check(runtime.GetQueueStatus().DatabaseBytes <= quota.MaxDatabaseBytes, "SQLite file exceeds configured cap");
        var oversized = Item(); oversized.Fields["text"] = new string('x', 9000);
        try { runtime.Enqueue(oversized); throw new Exception("Expected size rejection"); } catch (ArgumentException) { }
    }
    Console.WriteLine("PASS capacity and oversized metric rejection");

    // Simulate remote acceptance followed by an uncertain acknowledgement; retransmission must preserve payload identity.
    var ambiguous = new FakeProvider { FailAfterAcceptOnce = true };
    using (var runtime = new StatisticsRuntime(ambiguous, NullLogger<StatisticsRuntime>.Instance, Options(Path.Combine(root, "replay"))))
    {
        runtime.Enqueue(Item());
        await Until(() => runtime.GetCachedMetricsCount() == 0);
        var attempts = ambiguous.Received.ToArray();
        Check(attempts.Length == 2 && attempts[0].Timestamp == attempts[1].Timestamp, "Replay must preserve original timestamp");
    }
    Console.WriteLine("PASS ambiguous acknowledgement retains stable replay identity (at-least-once)");

    using var http = new HttpClient(new Handler());
    var vm = new VictoriaMetricsProvider(NullLogger<VictoriaMetricsProvider>.Instance, http, new() { Url = "http://isolated.test" });
    typeof(VictoriaMetricsProvider).GetField("_circuitBreakerOpenUntil", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(vm, DateTime.UtcNow.AddMinutes(1));
    try { await vm.WriteAsync(Item()); throw new Exception("Open circuit falsely acknowledged delivery"); } catch (HttpRequestException) { }
    Console.WriteLine("PASS open VictoriaMetrics circuit rejects delivery");
}
finally { Directory.Delete(root, true); }

sealed class FakeProvider : IMetricsProvider
{
    public bool Fail;
    public bool FailAfterAcceptOnce;
    public ConcurrentQueue<Metric> Received { get; } = new();
    public int LargestBatch;
    public Task WriteBulkAsync(IEnumerable<Metric> metrics, int? size, CancellationToken token = default)
    {
        token.ThrowIfCancellationRequested();
        if (Fail) throw new IOException("Simulated backend outage");
        var batch = metrics.ToArray(); LargestBatch = Math.Max(LargestBatch, batch.Length);
        foreach (var m in batch) Received.Enqueue(m);
        if (FailAfterAcceptOnce) { FailAfterAcceptOnce = false; throw new IOException("Simulated lost acknowledgement"); }
        return Task.CompletedTask;
    }
    public Task WriteBulkAsync(IEnumerable<Metric> metrics, CancellationToken token = default) => WriteBulkAsync(metrics, null, token);
    public Task WriteAsync(Metric metric, CancellationToken token = default) => WriteBulkAsync(new[] { metric }, null, token);
    public Task WriteAsync<T>(string m, T v, Dictionary<string,string>? tags = null, DateTime? timestamp = null, CancellationToken token = default) where T:struct => throw new NotSupportedException();
    public Task<IEnumerable<Metric>> QueryAsync(string query, CancellationToken token = default) => throw new NotSupportedException();
    public Task<IEnumerable<Metric>> SearchAsync(MetricsSearchParameters parameters, CancellationToken token = default) => throw new NotSupportedException();
    public Task<bool> IsHealthyAsync(CancellationToken token = default) => Task.FromResult(!Fail);
}
sealed class Handler : HttpMessageHandler
{
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.NoContent));
}
