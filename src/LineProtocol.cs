using System.Text;

namespace Sufficit.Statistics
{
    /// <summary>
    /// Escaping for the InfluxDB line protocol (also accepted by VictoriaMetrics).
    /// Line protocol has no escape for line breaks: a raw "\n" ends the line, and a single
    /// malformed line makes the server reject the whole batch it was sent with. Control
    /// characters are therefore replaced by a space before the protocol escapes are applied.
    /// </summary>
    public static class LineProtocol
    {
        /// <summary>Measurement names, tag keys, tag values and field keys.</summary>
        public static string EscapeIdentifier(string value)
        {
            if (string.IsNullOrEmpty(value))
                return value;

            var builder = new StringBuilder(value.Length + 8);
            foreach (var character in value)
            {
                switch (character)
                {
                    case '\\': builder.Append("\\\\"); break;
                    case ',': builder.Append("\\,"); break;
                    case '=': builder.Append("\\="); break;
                    case ' ': builder.Append("\\ "); break;
                    default:
                        if (char.IsControl(character))
                            builder.Append("\\ ");
                        else
                            builder.Append(character);
                        break;
                }
            }
            return builder.ToString();
        }

        /// <summary>String field values, including the surrounding quotes.</summary>
        public static string QuoteStringField(string value)
        {
            var builder = new StringBuilder((value?.Length ?? 0) + 8);
            builder.Append('"');
            foreach (var character in value ?? string.Empty)
            {
                switch (character)
                {
                    case '\\': builder.Append("\\\\"); break;
                    case '"': builder.Append("\\\""); break;
                    default:
                        builder.Append(char.IsControl(character) ? ' ' : character);
                        break;
                }
            }
            builder.Append('"');
            return builder.ToString();
        }
    }
}
