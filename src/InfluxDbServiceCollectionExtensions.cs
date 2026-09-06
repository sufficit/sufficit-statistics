using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;

namespace Sufficit.Statistics
{
    /// <summary>
    /// Extension methods for configuring InfluxDB metrics provider
    /// </summary>
    public static class InfluxDbServiceCollectionExtensions
    {
        /// <summary>
        /// Add InfluxDB metrics provider to dependency injection container
        /// </summary>
        /// <param name="services">Service collection</param>
        /// <param name="configuration">Configuration instance</param>
        /// <param name="configSectionName">Configuration section name (defaults to "Statistics:InfluxDB")</param>
        /// <returns>Service collection for chaining</returns>
        public static IServiceCollection AddInfluxDbMetricsProvider(
            this IServiceCollection services,
            IConfiguration configuration,
            string configSectionName = InfluxDbConfiguration.SECTIONNAME)
        {
            // Register configuration
            var config = new InfluxDbConfiguration();
            configuration.GetSection(configSectionName).Bind(config);
            
            // Check if configuration is valid - if not, system will work with empty responses
            var isValid = IsConfigurationValid(config);
            
            services.AddSingleton(config);

            // Only register HTTP client if configuration is valid
            if (!isValid)
            {
                // Log warning if configuration is invalid
                var logger = services.BuildServiceProvider(false).GetService<ILogger<InfluxDbMetricsProvider>>();
                logger?.LogWarning("Invalid InfluxDB configuration. Metrics provider will not be registered.");

                return services;
            }
            
            // Register HTTP client for InfluxDB
            services.AddHttpClient<InfluxDbMetricsProvider>(client =>
            {
                client.Timeout = TimeSpan.FromMilliseconds(config.TimeoutMs);
                client.DefaultRequestHeaders.Add("Accept", "application/json");
                client.DefaultRequestHeaders.Add("User-Agent", "SufficitMetricsProvider/1.0");
            });

            // Register the provider
            services.AddSingleton<IMetricsProvider, InfluxDbMetricsProvider>();

            return services;
        }

        /// <summary>
        /// Validate InfluxDB configuration without throwing exceptions
        /// </summary>
        /// <param name="config">Configuration to validate</param>
        /// <returns>True if configuration is valid, false otherwise</returns>
        private static bool IsConfigurationValid(InfluxDbConfiguration config)
        {
            if (string.IsNullOrWhiteSpace(config.Url))
                return false;
                
            if (string.IsNullOrWhiteSpace(config.Token))
                return false;
                
            if (string.IsNullOrWhiteSpace(config.Database))
                return false;

            if (config.TimeoutMs <= 0)
                return false;

            if (config.BatchSize <= 0)
                return false;

            if (!Uri.TryCreate(config.Url, UriKind.Absolute, out _))
                return false;

            return true;
        }
    }
}
