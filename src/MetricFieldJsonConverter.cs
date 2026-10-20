using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Sufficit.Statistics
{
    // Preserve primitive field types across restart (object would otherwise deserialize as JsonElement).
    internal sealed class MetricFieldJsonConverter : JsonConverter<object>
    {
        public override object Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            using var document = JsonDocument.ParseValue(ref reader);
            var root = document.RootElement;
            var code = (TypeCode)root.GetProperty("type").GetInt32();
            var value = root.GetProperty("value").GetString();
            return Convert.ChangeType(value, code, CultureInfo.InvariantCulture)!;
        }

        public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
        {
            if (value is JsonElement element)
            {
                switch (element.ValueKind)
                {
                    case JsonValueKind.String: value = element.GetString()!; break;
                    case JsonValueKind.True: value = true; break;
                    case JsonValueKind.False: value = false; break;
                    case JsonValueKind.Number:
                        value = element.TryGetInt64(out var integer) ? (object)integer : element.GetDecimal(); break;
                    default: throw new ArgumentException("Unsupported metric field JSON value.");
                }
            }
            var code = Type.GetTypeCode(value.GetType());
            if (code == TypeCode.Object || code == TypeCode.Empty || code == TypeCode.DBNull)
                throw new ArgumentException("Metric fields must be primitive values.");
            writer.WriteStartObject();
            writer.WriteNumber("type", (int)code);
            writer.WriteString("value", Convert.ToString(value, CultureInfo.InvariantCulture));
            writer.WriteEndObject();
        }
    }
}
