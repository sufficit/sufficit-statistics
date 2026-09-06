using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Sufficit.EFData.Statistics;
using System;

namespace Sufficit.Statistics
{
    /// <summary>
    /// Extension methods for configuring composite metrics provider
    /// </summary>
    public static class CompositeMetricsServiceCollectionExtensions
    {
        /// <summary>
        /// Add composite metrics provider with configurable primary and secondary providers
        /// </summary>
        /// <param name="services">Service collection</param>
        /// <param name="configuration">Configuration instance</param>
        /// <param name="configSectionName">Configuration section name (defaults to "Statistics:Composite")</param>
        /// <returns>Service collection for chaining</returns>
        public static IServiceCollection AddCompositeMetricsProvider(
            this IServiceCollection services,
            IConfiguration configuration,
            string configSectionName = CompositeMetricsConfiguration.SECTIONNAME)
        {
            // Register composite configuration
            var compositeConfig = new CompositeMetricsConfiguration();
            configuration.GetSection(configSectionName).Bind(compositeConfig);
            services.AddSingleton(compositeConfig);

            // Register individual providers based on configuration
            RegisterProvider(services, configuration, compositeConfig.PrimaryProvider);
            RegisterProvider(services, configuration, compositeConfig.SecondaryProvider);

            // Register composite provider
            services.AddScoped<IMetricsProvider>(serviceProvider =>
            {
                var logger = serviceProvider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<CompositeMetricsProvider>>();

                // Get primary provider
                IMetricsProvider primaryProvider = ResolveRequiredProvider(serviceProvider, compositeConfig.PrimaryProvider);

                // Get secondary provider if configured
                IMetricsProvider? secondaryProvider = null;
                if (!string.IsNullOrEmpty(compositeConfig.SecondaryProvider))
                {
                    secondaryProvider = ResolveOptionalProvider(serviceProvider, compositeConfig.SecondaryProvider);
                }

                return new CompositeMetricsProvider(logger, compositeConfig, primaryProvider, secondaryProvider);
            });

            return services;
        }

        private static void RegisterProvider(IServiceCollection services, IConfiguration configuration, string? providerName)
        {
            switch (NormalizeProviderName(providerName))
            {
                case "":
                    return;
                case "influxdb":
                case "influx":
                    services.AddInfluxDbMetricsProvider(configuration);
                    return;
                case "entityframework":
                case "ef":
                    services.AddSufficitDbContextStatistics(configuration);
                    services.AddScoped<EFMetricsProvider>();
                    return;
                case "victoriametrics":
                case "victoria":
                case "vitoria":
                    services.AddVictoriaMetricsProvider(configuration);
                    return;
                default:
                    throw new InvalidOperationException($"Unknown provider: {providerName}");
            }
        }

        private static IMetricsProvider ResolveRequiredProvider(IServiceProvider serviceProvider, string providerName)
        {
            switch (NormalizeProviderName(providerName))
            {
                case "influxdb":
                case "influx":
                    return serviceProvider.GetRequiredService<InfluxDbMetricsProvider>();
                case "entityframework":
                case "ef":
                    return serviceProvider.GetRequiredService<EFMetricsProvider>();
                case "victoriametrics":
                case "victoria":
                case "vitoria":
                    return serviceProvider.GetRequiredService<VictoriaMetricsProvider>();
                default:
                    throw new InvalidOperationException($"Unknown provider: {providerName}");
            }
        }

        private static IMetricsProvider? ResolveOptionalProvider(IServiceProvider serviceProvider, string providerName)
        {
            switch (NormalizeProviderName(providerName))
            {
                case "influxdb":
                case "influx":
                    return serviceProvider.GetService<InfluxDbMetricsProvider>();
                case "entityframework":
                case "ef":
                    return serviceProvider.GetService<EFMetricsProvider>();
                case "victoriametrics":
                case "victoria":
                case "vitoria":
                    return serviceProvider.GetService<VictoriaMetricsProvider>();
                default:
                    throw new InvalidOperationException($"Unknown provider: {providerName}");
            }
        }

        private static string NormalizeProviderName(string? providerName)
            => providerName?.Replace(" ", string.Empty).Trim().ToLowerInvariant() ?? string.Empty;
    }
}
