using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Dynamic;
using System.Linq;
using System.Text;
using DemoService.Core.Converters;
using DemoService.Core.Models;
using FluentAssertions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using DateTimeConverter = DemoService.Core.Converters.DateTimeConverter;

namespace DemoService.Core.Helpers
{
    /**
     * Strategy to be applied when values with duplicated keys are to be merged.
     */
    public enum MergeDuplicatesStrategy
    {
        Override        // Always use the new values to replace the existing old values
        , Strict        // Ensure all values identified by same key are identical, otherwise throw exception
        , Ignore        // Keep the existing value by ignoring any new values identified by the same key
    }

    public static class DynamicHelper
    {
        private static readonly DateTimeConverter DateTimeEqualityComparer = new DateTimeConverter();
        private static readonly NumberComparer NumberComparer = new NumberComparer();
        public static JsonSerializerSettings DefaultJsonSettings = new JsonSerializerSettings()
        {
            Converters = new List<JsonConverter>() { DateTimeEqualityComparer }
        };

        //Pre-defined pairs of data can be treated as equal
        private static HashSet<(object, object)> objectsAsEqual = new HashSet<(object, object)>()
        {
            (null, null),
            //Un-Comment the next line if empty strings are equal with null
            (null, ""),
            //Un-Comment the next line if Unspecified DateTime are equal with null
            (null, DateTimeConverter.NullDateTimeValue),
            //Un-Comment the next line if default primary values are equal with null
            (null, 0), (null, 0L), (null, 0d), (null, 0f)
        };

        public static Dictionary<(Type, Type), IEqualityComparer> InterTypesComparers = new Dictionary<(Type, Type), IEqualityComparer>()
        {
            {(typeof(long), typeof(DateTime)), DateTimeEqualityComparer },
            {(typeof(string), typeof(DateTime)), DateTimeEqualityComparer },
            {(typeof(float), typeof(string)), NumberComparer },
            {(typeof(double), typeof(string)), NumberComparer },
            {(typeof(decimal), typeof(string)), NumberComparer },
            {(typeof(float), typeof(float)), NumberComparer },
            {(typeof(double), typeof(double)), NumberComparer },
            {(typeof(decimal), typeof(decimal)), NumberComparer },
            {(typeof(float), typeof(double)), NumberComparer },
            {(typeof(double), typeof(decimal)), NumberComparer },
            {(typeof(decimal), typeof(float)), NumberComparer },
        };

        public static dynamic AsDynamic(this Dictionary<string, object> dict)
        {
            dict.Should().NotBeNull();
            IDictionary<string, object> dynObj = new ExpandoObject();
            foreach (var key in dict.Keys)
            {
                dynObj[key] = dict[key];
            }

            return dynObj;
        }

        private static dynamic GetResultsOrSelf(dynamic table)
        {
            try
            {
                return table.Results;
            }
            catch
            {
                return table;
            }
        }

        /// <summary>
        /// Merge tables of correlated items in exact orders to a single aggregated table with specific merge strategy to handle duplicated key-value-pairs.
        /// </summary>
        /// <typeparam name="T">Type of the entities described by the aggregated table.</typeparam>
        /// <param name="tables">Standard named tables returned by the Data Query, their Results contains the actual data with exactly same orders pertaining to a list of items</param>
        /// <param name="strategy">When there are columns of same name in different tables, then if and how to handle the conflicted values</param>
        /// <param name="valueExtractor">Specify how the values are extracted from the given table. By default, returning the Results property or the table itself if no such property presented.</param>
        /// <returns>A list of expected models described by the aggregated table</returns>
        public static List<T> MergeTablesToModels<T>(dynamic[] tables, MergeDuplicatesStrategy strategy = MergeDuplicatesStrategy.Override, Func<dynamic, dynamic> valueExtractor = null)
        {
            //Specify how row values are extracted: if not specified, it would return the 'Results' property of the table or the given table itself
            valueExtractor = valueExtractor ?? GetResultsOrSelf;

            string[] tableResultStrings = tables.Select(table => (string)JsonConvert.SerializeObject(table, DefaultJsonSettings)).ToArray();

            //Note: Assume the table contains identical values even if there are duplicated columns
            List<Dictionary<string, object>> aggregatedEntries = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(tableResultStrings[0]);

            for (int i = 1; i < tables.Length; i++)
            {
                dynamic table = tables[i];
                List<Dictionary<string, object>> tableDictList = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(tableResultStrings[i]);
                if (strategy == MergeDuplicatesStrategy.Strict && tableDictList.Count != aggregatedEntries.Count)
                {
                    throw new ArgumentException($"The rows of the {i + 1} table has {tableDictList.Count} rows, instead of {aggregatedEntries.Count}.");
                }

                for (int rowIndex = 0; rowIndex < tableDictList.Count; rowIndex++)
                {
                    if (aggregatedEntries.Count <= rowIndex)
                    {
                        if (strategy == MergeDuplicatesStrategy.Strict)
                        {
                            throw new ArgumentException(
                                $"No matching row[{rowIndex}] returned in the first table[{tables[0].Name}]");
                        }
                        else
                        {
                            aggregatedEntries.Add(new Dictionary<string, object>());
                        }
                    }
                    Dictionary<string, object> rowDict = aggregatedEntries[rowIndex];

                    foreach (var kvp in tableDictList[rowIndex])
                    {
                        var key = kvp.Key;
                        var value = kvp.Value;
                        if (!rowDict.ContainsKey(kvp.Key))
                        {
                            rowDict.Add(key, value);
                        }
                        else if (strategy == MergeDuplicatesStrategy.Override)
                        {
                            rowDict[key] = value;
                        }
                        else if (strategy == MergeDuplicatesStrategy.Ignore)
                        {
                            continue;
                        }
                        else if (!Object.Equals(rowDict[key], value))   // Strict strategy: expecting the values to be identical
                        {
                            throw new ArgumentException($"The row[{i}]['{key}'] has value '{rowDict[key]}' that is conflicted with '{value}'");
                        }
                    }
                }
            }

            if (aggregatedEntries is List<T>)
            {
                return aggregatedEntries as List<T>;
            }

            string recordsJson = JsonConvert.SerializeObject(aggregatedEntries);

            List<T> entities = JsonConvert.DeserializeObject<List<T>>(recordsJson, DefaultJsonSettings);

            return entities;
        }


        public static List<dynamic> AsDynamics(string json)
        {
            Dictionary<string, object>[] dictionaries =
                JsonConvert.DeserializeObject<Dictionary<string, object>[]>(json);
            List<dynamic> dynObjects = dictionaries.Select(d => AsDynamic(d)).ToList();
            return dynObjects;
        }

        public static string GetFriendlyPropertyTable(IEnumerable<string> concernedProperties,
            params Dictionary<string, object>[] results)
        {
            int rows = 1 + results.Length;
            StringBuilder[] rowBuilders = Enumerable.Range(0, rows).Select(i => new StringBuilder("|")).ToArray();

            string[] column = new string[rows];
            int columeWidth;
            try
            {
                foreach (var propertName in concernedProperties)
                {
                    column[0] = propertName;
                    for (int i = 0; i < rows - 1; i++)
                    {
                        column[i + 1] = results[i][propertName].ToString();
                    }

                    columeWidth = column.Select(c => c.Length).Max();
                    string columnFormat = " {0,-" + columeWidth + "} |";
                    for (int i = 0; i < rows; i++)
                    {
                        rowBuilders[i].AppendFormat(columnFormat, column[i]);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            return string.Join(Environment.NewLine, rowBuilders.Select(sb => sb.ToString()));

        }

        public static List<IDictionary<string, object>> AsDictionaries(List<dynamic> dynamicList)
        {
            List<IDictionary<string, object>> result = new List<IDictionary<string, object>>();
            foreach (var d in dynamicList)
            {
                result.Add(PropertiesToDictionary(d));
            }

            return result;
        }

        public static IDictionary<string, object> AsDictionary(object dynamicObj)
        {
            if (dynamicObj is Dictionary<string, object>)
            {
                return dynamicObj as Dictionary<string, object>;
            }

            string jsonString = JsonConvert.SerializeObject(dynamicObj);
            if (jsonString.StartsWith("["))
            {
                throw new ArgumentException("Array cannot be cast to Dictionary");
            }

            Dictionary<string, object> dict = JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonString);
            List<string> dateKeys =
                dict.Keys.Where(k => k.EndsWith("date", StringComparison.OrdinalIgnoreCase)).ToList();

            foreach (var dateKey in dateKeys)
            {
                object value = dict[dateKey];
                if (value is long)
                {
                    dict[dateKey] = DateTimeConverter.FromUnixLong((long)value);
                }
                else if (value is string)
                {
                    dict[dateKey] = DateTimeConverter.FromUnixLong((string)value);
                }
            }

            return dict;
        }

        public static IDictionary<string, object> PropertiesToDictionary(dynamic anyObject)
        {
            if (anyObject == null)
            {
                return null;
            }
            else if (anyObject is IDictionary<string, object> dictObject)
            {
                return dictObject;
            }

            var dictionary = new Dictionary<string, object>();
            foreach (PropertyDescriptor propertyDescriptor in TypeDescriptor.GetProperties(anyObject))
            {
                object obj = propertyDescriptor.GetValue(anyObject);
                dictionary.Add(propertyDescriptor.Name, obj);
            }
            return dictionary;
        }

        public static object GetValue(this object dynamicObj, string key)
        {
            IDictionary<string, object> dict = AsDictionary(dynamicObj);
            string matchedKey = dict.Keys.FirstOrDefault(k => k.Equals(key, StringComparison.OrdinalIgnoreCase));
            if (matchedKey == null)
            {
                throw new ArgumentException($"No key {key} exists in {dynamicObj}");
            }

            return dict[matchedKey];
        }

        public static IDictionary<string, IDelta> GetDeltas(IDictionary<string, object> leftDict,
            IDictionary<string, object> rightDict)
        {
            //TODO: 
            Dictionary<string, string> sqlColumnToJsonPropertyMapptings =
                leftDict.Keys.Where(rightDict.ContainsKey).ToDictionary(k => k, k => k);

            Dictionary<string, IDelta> deltas = new Dictionary<string, IDelta>();

            foreach (var kvp in sqlColumnToJsonPropertyMapptings)
            {
                object leftValue = leftDict[kvp.Key];
                leftValue = leftValue is JValue lValue ? lValue.Value : leftValue;
                object rightValue = rightDict[kvp.Value];
                rightValue = rightValue is JValue rValue ? rValue.Value : rightValue;

                //Ignore predefined pairs that are regarded as equal
                if (objectsAsEqual.Contains((leftValue, rightValue)) || objectsAsEqual.Contains((rightValue, leftValue)))
                {
                    continue;
                }

                if (leftValue == null || rightValue == null)
                {
                    deltas.Add(kvp.Key, Delta.NewDelta(kvp.Key, kvp.Value, leftValue, rightValue));
                    continue;
                }
                else if (leftValue is IDictionary<string, object> lDict || rightValue is IDictionary<string, object> rDict)
                {
                    IDictionary<string, IDelta> dictDeltas = GetDeltas(leftValue, rightValue);
                    if (dictDeltas.Count != 0)
                    {
                        deltas.Add(kvp.Key, new CompositeDelta(dictDeltas));
                    }
                    continue;
                }
                else if (leftValue.Equals(rightValue) || leftValue.ToString().Trim().Equals(rightValue.ToString().Trim()))
                {
                    continue;
                }
                else if (!(leftValue is string) && !(rightValue is string))
                {
                    //Compare if both leftValue and rightValue are IEnumerable
                    if (leftValue is IEnumerable leftValues && rightValue is IEnumerable rightValues)
                    {
                        var valueDeltas = GetAllDeltas(leftValues, rightValues);
                        if (valueDeltas.Count > 0)
                        {
                            IDelta compositeDelta = new CompositeDelta(valueDeltas);
                            deltas.Add(kvp.Key, compositeDelta);
                        }
                        continue;
                    }
                }

                IEqualityComparer comparer;
                Type leftValueType = leftValue.GetType();
                Type rightValueType = rightValue.GetType();
                if ((InterTypesComparers.TryGetValue((leftValueType, rightValueType), out comparer) || InterTypesComparers.TryGetValue((rightValueType, leftValueType), out comparer))
                    && comparer.Equals(leftValue, rightValue))
                {
                    continue;
                }
                else
                {
                    deltas.Add(kvp.Key, Delta.NewDelta(kvp.Key, kvp.Value, leftValue, rightValue));
                }
            }

            //TODO: checking missing key pairs?
            return deltas;
        }

        public static List<(string, object, object)> GetDifferences(dynamic expected, dynamic actual)
        {
            IDictionary<string, object> dict1 = PropertiesToDictionary(expected);
            IDictionary<string, object> dict2 = PropertiesToDictionary(actual);

            return GetDifferences(dict1, dict2);
        }

        public static List<(string, object, object)> GetDifferences(IDictionary<string, object> dict1,
            IDictionary<string, object> dict2)
        {
            List<(string, object, object)> differences = new List<(string, object, object)>();
            List<string> dict2Keys = dict2.Keys.ToList();

            foreach (var key in dict1.Keys)
            {
                object value1 = dict1[key];
                object value2;
                if (dict2.ContainsKey(key))
                {
                    value2 = dict2[key];
                }
                else
                {
                    string key2 = dict2Keys.FirstOrDefault(k => k.Equals(key));
                    value2 = key2 == null ? null : dict2[key2];
                }

                if (objectsAsEqual.Contains((value1, value2)))
                {
                    continue;
                }

                if (value1 == null)
                {
                    if (value2 != null)
                    {
                        differences.Add((key, value1, value2));
                    }
                }
                else
                {
                    if (!value1.Equals(value2) && !value1.ToString().Trim().Equals(value2.ToString().Trim()))
                    {
                        differences.Add((key, value1, value2));
                    }
                }
            }

            return differences;
        }


        // public static Dictionary<PropertyDescriptor, string> GetMapping(dynamic dynObj,
        //     IEnumerable<string> mappedNames)
        // {
        //     Dictionary<PropertyDescriptor, string> mappings = null;
        //
        //     PropertyDescriptorCollection propertyDescriptors = TypeDescriptor.GetProperties(dynObj);
        //     HashSet<string> dynPropertyNames = Enumerable.Range(0, propertyDescriptors.Count)
        //         .Select(i => propertyDescriptors[i].Name).ToHashSet();
        //
        //     string[] exactlyMatchedNames = mappedNames.Where(dynPropertyNames.Contains).ToArray();
        //     mappings = exactlyMatchedNames.ToDictionary(
        //         pn => propertyDescriptors[pn],
        //         pn => pn
        //     );
        //
        //     //dynPropertyNames.RemoveWhere(exactlyMatchedNames.Contains);
        //     return mappings;
        // }

        // public static Dictionary<string, Delta> GetPropertyDeltas(dynamic dynObj, Dictionary<string, object> jsonDictionary)
        // {
        //     Dictionary<PropertyDescriptor, string> mappings = GetMapping(dynObj, jsonDictionary.Keys);
        //
        //     Dictionary<string, Delta> deltas = new Dictionary<string, Delta>();
        //     foreach (var mapping in mappings)
        //     {
        //         object sqlValue = mapping.Key.GetValue(dynObj);
        //         object jsonValue = jsonDictionary[mapping.Value];
        //         if (!Object.Equals(sqlValue, jsonValue))
        //         {
        //            deltas.Add(mapping.Value, new Delta()
        //             {
        //                 SqlColumnName = mapping.Key.Name,
        //                 JsonKeyName = mapping.Value,
        //                 SqlValue = sqlValue,
        //                 JsonValue = jsonValue
        //             });
        //         }
        //     }
        //
        //     return deltas;
        // }

        public static IDictionary<string, IDelta> GetDeltas(object source, object target)
        {
            IDictionary<string, object> sqlDict = PropertiesToDictionary(source);
            IDictionary<string, object> jsonDict = PropertiesToDictionary(target);
            return GetDeltas(sqlDict, jsonDict);
        }

        public static IDictionary<IComparable, IDictionary<string, object>> AsKeyedDictionary(IEnumerable elements,
            params string[] keys)
        {
            IEnumerator enumerator = elements.GetEnumerator();
            List<IDictionary<string, object>> dictList = new List<IDictionary<string, object>>();
            while (enumerator.MoveNext())
            {
                dictList.Add(PropertiesToDictionary(enumerator.Current));
            }

            switch (keys.Length)
            {
                case 0:
                    //Use natural sequence as the key
                    return Enumerable.Range(0, dictList.Count).ToDictionary(
                        i => i as IComparable,
                        i => dictList[i]
                    );
                case 1:
                    //Use the only unique key as the key
                    return dictList.ToDictionary(
                        dict => dict[keys[0]] as IComparable,
                        dict => dict
                    );
                case 2:
                    //Use combination of the given 2 keys of ValueTuple<T1, T2> as the key
                    return dictList.ToDictionary(
                        dict => ValueTuple.Create(dict[keys[0]], dict[keys[1]]) as IComparable,
                        dict => dict
                    );
                case 3:
                    //Use combination of the given 3 keys of ValueTuple<T1, T2, T3> as the key
                    return dictList.ToDictionary(
                        dict => ValueTuple.Create(dict[keys[0]], dict[keys[1]], dict[keys[2]]) as IComparable,
                        dict => dict
                    );
                case 4:
                    //Use combination of the given 4 keys of ValueTuple<T1, T2, T3, T4> as the key
                    return dictList.ToDictionary(
                        dict => ValueTuple.Create(dict[keys[0]], dict[keys[1]], dict[keys[2]], dict[keys[3]]) as IComparable,
                        dict => dict
                    );
                default:
                    throw new NotSupportedException("Please extend this method to support composite key from more properties");
            }
        }

        public static void LogDeltas(IDictionary<IComparable, ICollection<IDelta>> allDeltas)
        {
            foreach (var key in allDeltas.Keys)
            {
                Console.WriteLine($"For element with keys {key}:\n\t{string.Join("\n\t", string.Join("\n\t", allDeltas[key]))}");
            }
        }

        public static void LogDeltas(IDictionary<string, IDelta> allDeltas)
        {
            foreach (var key in allDeltas.Keys)
            {
                Console.WriteLine($"{key}: {allDeltas[key]}");
            }
        }

        public static IDictionary<IComparable, ICollection<IDelta>> GetAllDeltas(IEnumerable sources, IEnumerable targets, params string[] keys)
        {
            IDictionary<IComparable, IDictionary<string, object>> keyedDictionary1 = AsKeyedDictionary(sources, keys);
            IDictionary<IComparable, IDictionary<string, object>> keyedDictionary2 = AsKeyedDictionary(targets, keys);

            IDictionary<IComparable, ICollection<IDelta>> allDeltas = new Dictionary<IComparable, ICollection<IDelta>>();
            foreach (var key in keyedDictionary1.Keys)
            {
                if (!keyedDictionary2.ContainsKey(key))
                {
                    Delta missingRight = Delta.MissingRight(key.ToString());
                    allDeltas.Add(key, new[] { missingRight });
                }

                IDictionary<string, IDelta> elementDelta = GetDeltas(keyedDictionary1[key], keyedDictionary2[key]);
                if (elementDelta.Count > 0)
                {
                    allDeltas.Add(key, elementDelta.Values);
                }
            }

            List<IComparable> missingsOnLeft =
                keyedDictionary2.Keys.Where(k => !keyedDictionary1.ContainsKey(k)).ToList();
            missingsOnLeft.ForEach(k => allDeltas.Add(k, new[] { Delta.MissingLeft(k.ToString()) }));

            return allDeltas;
        }
    }

}
