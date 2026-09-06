using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Net.Http;
using System.Net.Security;

namespace Sufficit.Statistics
{
    /// <summary>
    /// Extension methods for configuring the VictoriaMetrics provider.
    /// </summary>
    public static class VictoriaMetricsServiceCollectionExtensions
    {
        /// <summary>
        /// Adds the VictoriaMetrics provider to the dependency injection container.
        /// </summary>
        public static IServiceCollection AddVictoriaMetricsProvider(
            this IServiceCollection services,
            IConfiguration configuration,
            string configSectionName = VictoriaMetricsConfiguration.SECTIONNAME)
        {
            var config = new VictoriaMetricsConfiguration();
            configuration.GetSection(configSectionName).Bind(config);

            var isValid = IsConfigurationValid(config);
            services.AddSingleton(config);

            if (!isValid)
            {
                var logger = services.BuildServiceProvider(false).GetService<ILogger<VictoriaMetricsProvider>>();
                logger?.LogWarning("Invalid VictoriaMetrics configuration. Metrics provider will not be registered.");
                return services;
            }

            services.AddHttpClient<VictoriaMetricsProvider>(client =>
            {
                client.Timeout = TimeSpan.FromMilliseconds(config.TimeoutMs);
                client.DefaultRequestHeaders.Add("Accept", "application/json");

                // Build User-Agent with hostname and assembly name
                var userAgent = $"Sufficit{nameof(VictoriaMetricsProvider)}/1.0";
                try
                {
                    var hostname = System.Net.Dns.GetHostName();
                    var assembly = System.Reflection.Assembly.GetEntryAssembly()?.GetName().Name ??
                                  System.Reflection.Assembly.GetExecutingAssembly().GetName().Name;
                    if (!string.IsNullOrWhiteSpace(hostname) || !string.IsNullOrWhiteSpace(assembly))
                    {
                        userAgent += $" ({hostname}/{assembly})";
                    }
                }
                catch
                {
                    // Silently continue with default User-Agent if we can't get hostname/assembly
                }
                client.DefaultRequestHeaders.Add("User-Agent", userAgent);
            })
            .ConfigurePrimaryHttpMessageHandler(() =>
            {
                var handler = new HttpClientHandler();
                if (config.AllowInvalidCertificateName)
                {
                    handler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;
                }

                return handler;
            });

            services.AddSingleton<IMetricsProvider>(provider => provider.GetRequiredService<VictoriaMetricsProvider>());

            return services;
        }

        private static bool IsConfigurationValid(VictoriaMetricsConfiguration config)
        {
            if (string.IsNullOrWhiteSpace(config.Url))
                return false;

            if (string.IsNullOrWhiteSpace(config.Database))
                return false;

            if (config.TimeoutMs <= 0)
                return false;

            if (config.BatchSize <= 0)
                return false;

            if (!Uri.TryCreate(config.Url, UriKind.Absolute, out _))
                return false;

            var hasToken = !string.IsNullOrWhiteSpace(config.Token);
            var hasBasicCredentials =
                !string.IsNullOrWhiteSpace(config.Username) &&
                !string.IsNullOrWhiteSpace(config.Password);

            return hasToken || hasBasicCredentials;
        }
    }
}
