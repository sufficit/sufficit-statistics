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
            var queue = new MetricsQueueOptions();
            var section = configuration.GetSection("Sufficit:Statistics:Queue");
            if (!string.IsNullOrWhiteSpace(section["DirectoryPath"])) queue.DirectoryPath = section["DirectoryPath"]!;
            if (long.TryParse(section["MaxDatabaseBytes"], out var capacity)) queue.MaxDatabaseBytes = capacity;
            if (int.TryParse(section["MaxMetricBytes"], out var maxMetric)) queue.MaxMetricBytes = maxMetric;
            if (int.TryParse(section["BatchSize"], out var batch)) queue.BatchSize = batch;
            if (double.TryParse(section["PollSeconds"], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var poll)) queue.PollInterval = System.TimeSpan.FromSeconds(poll);
            if (double.TryParse(section["MaxRetrySeconds"], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var retry)) queue.MaxRetryDelay = System.TimeSpan.FromSeconds(retry);
            queue.Validate();
            services.AddSingleton(queue);
            services.AddSingleton<StatisticsRuntime>();
            services.AddSingleton<Microsoft.Extensions.Hosting.IHostedService, MetricsQueueHostedService>();
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
