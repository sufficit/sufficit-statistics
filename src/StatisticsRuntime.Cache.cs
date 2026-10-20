using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;

namespace Sufficit.Statistics
{
    public partial class StatisticsRuntime : IDisposable
    {
        private readonly LocalMetricsQueue _queue;
        private readonly MetricsQueueOptions _queueOptions;
        private readonly CancellationTokenSource _queueStop = new CancellationTokenSource();
        private readonly Task _queueWorker;
        private long _queueWriteFailures;
        private long _queueDeliveryFailures;
        private int _queueDisposed;

        private void CacheMetricAndProcess(Metric metric) => Enqueue(metric);

        /// <summary>Returns only after local durable acceptance. Does not await remote delivery.</summary>
        public void Enqueue(Metric metric)
        {
            try { _queue.Enqueue(metric); }
            catch { Interlocked.Increment(ref _queueWriteFailures); throw; }
        }

        /// <summary>Atomically accepts a bounded batch locally. Remote delivery is asynchronous.</summary>
        public void EnqueueBatch(IReadOnlyCollection<Metric> metrics)
        {
            if (metrics == null) throw new ArgumentNullException(nameof(metrics));
            if (metrics.Count > _queueOptions.BatchSize) throw new ArgumentException("Batch exceeds configured queue BatchSize.", nameof(metrics));
            try { _queue.EnqueueBatch(metrics); }
            catch { Interlocked.Increment(ref _queueWriteFailures); throw; }
        }

        public int GetCachedMetricsCount() => (int)Math.Min(int.MaxValue, GetQueueStatus().PendingCount);

        public MetricsQueueStatus GetQueueStatus()
        {
            var status = _queue.Status();
            status.WriteFailures = Interlocked.Read(ref _queueWriteFailures);
            status.DeliveryFailures = Interlocked.Read(ref _queueDeliveryFailures);
            return status;
        }

        private async Task RunQueueAsync()
        {
            var token = _queueStop.Token;
            var retry = _queueOptions.PollInterval;
            while (!token.IsCancellationRequested)
            {
                try
                {
                    var batch = _queue.ReadBatch(_queueOptions.BatchSize);
                    if (batch.Count == 0)
                    {
                        await Task.Delay(_queueOptions.PollInterval, token).ConfigureAwait(false);
                        continue;
                    }
                    // Call provider directly: direct runtime writes and local acceptance are distinct contracts.
                    await _metricsProvider.WriteBulkAsync(batch.Select(item => item.Metric), _queueOptions.BatchSize, token).ConfigureAwait(false);
                    token.ThrowIfCancellationRequested();
                    _queue.Acknowledge(batch[batch.Count - 1].Id);
                    retry = _queueOptions.PollInterval;
                    await Task.Delay(_queueOptions.PollInterval, token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { break; }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref _queueDeliveryFailures);
                    _logger.LogWarning(ex, "Metrics delivery failed; persisted batch retained. Retry in {Delay}", retry);
                    try { await Task.Delay(retry, token).ConfigureAwait(false); }
                    catch (OperationCanceledException) when (token.IsCancellationRequested) { break; }
                    retry = TimeSpan.FromMilliseconds(Math.Min(_queueOptions.MaxRetryDelay.TotalMilliseconds, retry.TotalMilliseconds * 2));
                }
            }
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _queueDisposed, 1) != 0) return;
            _queueStop.Cancel();
            // Do not close SQLite while the sender may still acknowledge a completed request.
            _queueWorker.GetAwaiter().GetResult();
            _queue.Dispose();
            _queueStop.Dispose();
        }
    }
}
