using System;
using System.Collections;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace DemoService.Core.Converters
{
    /// <summary>
    /// Converts a <see cref="DateTime"/> to and from Unix epoch time
    /// </summary>
    public class DateTimeConverter : JsonConverter, IEqualityComparer, IEqualityComparer<DateTime>
    {
        internal static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public const long NullDateTimeValue = -62135596800;

        /// <summary>
        /// Convert object of either DateTime, Long or String to DateTime
        /// </summary>
        /// <param name="x">Unknown type of object to be converted to DateTime</param>
        /// <returns>Converted DateTime object if x is DateTime, Long or String, otherwise thrown NotSupportedException</returns>
        public static DateTime AsDateTime(object x)
        {
            if (x is DateTime)
            {
                return (DateTime)x;
            }
            else if (x is long xLong)
            {
                return FromUnixLong(xLong);
            }
            else if (x is string xString)
            {
                return FromUnixLong(xString);
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        /// <summary>
        /// Convert the ERM specific Unix Long value to DateTime
        /// </summary>
        /// <param name="seconds">Seconds elapsed since 1970/01/01 with extra 36000 seconds that is 10H offset of Brisbane time</param>
        /// <returns>Brisbane local time represented as UTC time</returns>
        public static DateTime FromUnixLong(double seconds)
        {
            DateTime d = UnixEpoch.AddSeconds(seconds);
            return d;
        }

        /// <summary>
        /// Convert the ERM specific Unix Long value string to DateTime
        /// </summary>
        /// <param name="secondsString">Seconds elapsed since 1970/01/01 with extra 36000 seconds that is 10H offset of Brisbane time</param>
        /// <returns>Brisbane local time represented as UTC time</returns>
        public static DateTime FromUnixLong(string secondsString)
        {
            long seconds = long.Parse(secondsString);

            return FromUnixLong(seconds);
        }

        /// <summary>
        /// Convert the DateTime to ERM specific Unix Long value including extra 36000 seconds that is 10H offset of Brisbane time
        /// </summary>
        /// <param name="date">DateTime value to be converted</param>
        /// <returns>ERM specific Unix Long value including extra 36000 seconds that is 10H offset of Brisbane time</returns>
        public static long ToUnixLong(DateTime date)
        {
            DateTimeOffset dateTimeOffset = date;
            long unixTime = dateTimeOffset.ToUnixTimeMilliseconds();
            return unixTime;
        }

        /// <summary>
        /// Determines whether this instance can convert the specified object type.
        /// </summary>
        /// <param name="objectType">Type of the object.</param>
        /// <returns>
        /// 	<c>true</c> if this instance can convert the specified object type; otherwise, <c>false</c>.
        /// </returns>
        public override bool CanConvert(Type objectType)
        {
            if (objectType == typeof(DateTime) || objectType == typeof(DateTime?))
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Writes the JSON representation of the object.
        /// </summary>
        /// <param name="writer">The <see cref="JsonWriter"/> to write to.</param>
        /// <param name="value">The value.</param>
        /// <param name="serializer">The calling serializer.</param>
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            long seconds;

            if (value is DateTime dateTime)
            {
                TimeSpan elapsedTime = dateTime - UnixEpoch;
                seconds = (long)elapsedTime.TotalSeconds;
            }
            else
            {
                throw new JsonSerializationException("Expected date object value.");
            }

            if (seconds < 0)
            {
                throw new JsonSerializationException(
                    "Cannot convert date value that is before Unix epoch of 00:00:00 UTC on 1 January 1970.");
            }

            writer.WriteValue(seconds);
        }

        /// <summary>
        /// Reads the JSON representation of the object.
        /// </summary>
        /// <param name="reader">The <see cref="JsonReader"/> to read from.</param>
        /// <param name="objectType">Type of the object.</param>
        /// <param name="existingValue">The existing property value of the JSON that is being converted.</param>
        /// <param name="serializer">The calling serializer.</param>
        /// <returns>The object value.</returns>
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue,
            JsonSerializer serializer)
        {
            bool nullable = Nullable.GetUnderlyingType(objectType) != null;
            if (reader.TokenType == JsonToken.Null)
            {
                if (nullable)
                {
                    return null;
                }

                throw new JsonSerializationException($"Cannot convert null value to {objectType}");
            }

            long seconds;
            if (reader.TokenType == JsonToken.Integer)
            {
                seconds = (long)reader.Value;
            }
            else if (reader.TokenType == JsonToken.String)
            {
                string valueString = reader.ReadAsString();
                if (DateTime.TryParse(valueString, out DateTime dateTime))
                {
                    return dateTime;
                }
                else if (!long.TryParse((string)reader.Value, out seconds))
                {
                    throw new JsonSerializationException(
                        $"Cannot convert invalid value '{reader.Value}' to {objectType}");
                }
            }
            else
            {
                throw new JsonSerializationException(
                    $"Unexpected token parsing date. Expected Integer or String, got {reader.TokenType}.");
            }

            if (seconds == NullDateTimeValue)
            {
                if (nullable)
                    return null;
                else
                    throw new JsonSerializationException(
                        $"The Non-nullable DateTime property cannot be set with null!");
            }

            //TODO: handle negative values by throwing exception?
            DateTime d = UnixEpoch.AddSeconds(seconds);
            return d;
        }

        public bool Equals(object x, object y)
        {
            if (x == null && y == null)
                return true;
            else if (x == null || y == null)
                return false;

            DateTime xDateTime = AsDateTime(x);
            DateTime yDateTime = AsDateTime(y);

            return Equals(xDateTime, yDateTime);
        }

        public int GetHashCode(object obj)
        {
            DateTime dateTime = AsDateTime(obj);
            return GetHashCode(dateTime);
        }

        public bool Equals(DateTime x, DateTime y)
        {
            return x.Equals(y);
        }

        public int GetHashCode(DateTime obj)
        {
            return obj.GetHashCode();
        }
    }

}
