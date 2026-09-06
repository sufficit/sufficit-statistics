using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Sufficit.EFData.Statistics;
using Sufficit.Events;

namespace Sufficit.Statistics
{
    /// <summary>
    /// Extension methods for registering Sufficit Statistics services.
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddSufficitStatistics(this IServiceCollection services)
        {
            var provider = services.BuildServiceProvider(false);
            var configuration = provider.GetRequiredService<IConfiguration>();
            var factory = provider.GetService<ILoggerFactory>();
            return services.AddSufficitStatistics(configuration, factory);
        }

        public static IServiceCollection AddSufficitStatistics(this IServiceCollection services, IConfiguration configuration, ILoggerFactory? factory = null)
        {
            services.AddVictoriaMetricsProvider(configuration);
            services.AddSingleton<StatisticsRuntime>();
            services.AddSingleton<IMetricController>(provider => provider.GetRequiredService<StatisticsRuntime>());
            services.AddSingleton<IEventHandler<Metric>>(provider => provider.GetRequiredService<StatisticsRuntime>());

            return services;
        }

        /// <summary>
        /// Adds the Entity Framework metrics provider registration.
        /// </summary>
        public static IServiceCollection AddEFMetricsProvider(this IServiceCollection services, IConfiguration configuration, ILoggerFactory? factory = null)
            => services.AddSufficitStatisticsEntityFramework(configuration, factory);
    }
}
