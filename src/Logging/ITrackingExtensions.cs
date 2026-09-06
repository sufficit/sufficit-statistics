using System;

namespace Sufficit.Logging
{
    public static class ITrackingExtensions
    {
        public static LogStopWatch<TClass, TClass> Track<TClass>(this ITracking source)
            => source.Track<TClass, TClass>();

        public static LogStopWatchBackground<TClass, TClass> TrackBackground<TClass>(this ITracking source)
            => source.TrackBackground<TClass, TClass>();

        /// <summary>
        /// Track with content
        /// </summary>
        public static LogStopWatch<TClass, TClass> Track<TClass>(this ITracking source, TClass content)
            => source.Track<TClass, TClass>().WithContent(content);
    }
}
