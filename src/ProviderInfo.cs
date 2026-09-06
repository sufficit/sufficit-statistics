using System;

namespace Sufficit.Statistics
{
    /// <summary>
    /// Information about a metrics provider
    /// </summary>
    public class ProviderInfo
    {
        public string Name { get; set; } = string.Empty;
        public string Type { get; set; } = string.Empty;
        public string FullTypeName { get; set; } = string.Empty;
    }
}