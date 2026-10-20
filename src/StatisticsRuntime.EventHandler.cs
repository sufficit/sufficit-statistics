using Sufficit.Events;
using System.Threading;
using System.Threading.Tasks;

namespace Sufficit.Statistics
{
    public partial class StatisticsRuntime : IEventHandler<Metric>
    {
        /// <summary>Accepts on local disk before returning; persistence failures propagate to the publisher.</summary>
        public Task HandleAsync(Metric metric, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Enqueue(metric);
            return Task.CompletedTask;
        }
    }
}
