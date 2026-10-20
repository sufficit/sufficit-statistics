using Microsoft.Extensions.Hosting;
using System.Threading;
using System.Threading.Tasks;

namespace Sufficit.Statistics
{
    // Resolving the runtime during host startup resumes disk backlog even with no new events.
    internal sealed class MetricsQueueHostedService : IHostedService
    {
        private readonly StatisticsRuntime _runtime;
        public MetricsQueueHostedService(StatisticsRuntime runtime) => _runtime = runtime;
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken)
        {
            _runtime.Dispose();
            return Task.CompletedTask;
        }
    }
}
