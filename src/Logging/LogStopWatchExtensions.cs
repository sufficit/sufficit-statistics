using System;

namespace Sufficit.Logging
{
    public static class LogStopWatchExtensions
    {
        /// <summary>
        ///     Stop Watch Json Log <br />
        ///     Used to long term log actions
        /// </summary>
        public static LogStopWatch<TClass, TContent> WithContent<TClass, TContent>(this LogStopWatch<TClass, TContent> source, TContent content)
        {
            source.Content = content;
            return source;
        }        
    }
}
