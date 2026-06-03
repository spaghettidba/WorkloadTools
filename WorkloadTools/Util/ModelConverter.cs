using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using WorkloadTools;

namespace WorkloadTools.Util
{
    public class ModelConverter : JsonConverter
    {
        public override bool CanWrite => false;

        public override bool CanRead => true;

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.Null)
                return null;

            var jObject = JObject.Load(reader);
            return DeserializeObject(jObject.ToObject<IDictionary<string, object>>(), objectType, serializer);
        }

        public override bool CanConvert(Type objectType)
        {
            // Convert all types in the WorkloadTools namespace
            if (objectType != null && objectType.FullName != null && objectType.FullName.StartsWith("WorkloadTools"))
            {
                return !objectType.FullName.Contains("+");
            }
            return false;
        }

        private object DeserializeObject(IDictionary<string, object> dictionary, Type type, JsonSerializer serializer)
        {
            // Check if a concrete type is specified in the JSON via __type
            // If so, resolve it and use that instead of the potentially abstract base type
            if (dictionary.ContainsKey("__type"))
            {
                var typeName = dictionary["__type"].ToString();
                var resolvedType = ResolveType(type, typeName);
                if (resolvedType != null && !resolvedType.IsAbstract)
                {
                    type = resolvedType;
                }
            }

            // Skip abstract types - they cannot be instantiated
            if (type.IsAbstract || type.IsInterface)
            {
                throw new ArgumentException($"The specified class '{type.Name}' is abstract and cannot be instantiated.");
            }

            object p;
            try
            {
                // try to create the object using its parameterless constructor
                p = Activator.CreateInstance(type);
            }
            catch
            {
                // try to create the object using this scary initializer that 
                // doesn't need the parameterless constructor
                p = System.Runtime.Serialization.FormatterServices.GetUninitializedObject(type);
            }

            var props = type.GetProperties();

            // Remove __type from the dictionary since it's not a property to set
            dictionary = new Dictionary<string, object>(dictionary);
            dictionary.Remove("__type");

            foreach (var key in dictionary.Keys)
            {
                var prop = props.Where(t => t.Name == key).FirstOrDefault();
                if (prop != null)
                {
                    if (prop.Name.EndsWith("Filter"))
                    {
                        if (dictionary[key] is string stringValue)
                        {
                            prop.SetValue(p, new string[] { stringValue }, null);
                        }
                        else if (dictionary[key] is JArray jArray)
                        {
                            prop.SetValue(p, jArray.ToObject<string[]>(), null);
                        }
                        else if (dictionary[key] is IEnumerable<object> enumerable)
                        {
                            prop.SetValue(p, enumerable.Cast<string>().ToArray(), null);
                        }
                        else
                        {
                            prop.SetValue(p, (string[])((ArrayList)dictionary[key]).ToArray(typeof(string)), null);
                        }
                    }
                    else
                    {
                        if (dictionary[key] is IDictionary<string, object> dictionaryValue)
                        {
                            if (prop.PropertyType.IsGenericType && prop.PropertyType.GetGenericTypeDefinition() == typeof(Dictionary<,>))
                            {
                                var rawDic = dictionaryValue;

                                var obj = Activator.CreateInstance(prop.PropertyType);
                                foreach (var itm in rawDic.Keys)
                                {
                                    ((Dictionary<string, string>)obj).Add(itm, rawDic[itm].ToString());
                                }
                                prop.SetValue(p, obj, null);
                            }
                            else
                            {
                                prop.SetValue(p, DeserializeObject(dictionaryValue, prop.PropertyType, serializer), null);
                            }
                        }
                        else if (dictionary[key] is JObject jObj)
                        {
                            if (prop.PropertyType.IsGenericType && prop.PropertyType.GetGenericTypeDefinition() == typeof(Dictionary<,>))
                            {
                                var rawDic = jObj.ToObject<IDictionary<string, object>>();
                                var obj = Activator.CreateInstance(prop.PropertyType);
                                foreach (var itm in rawDic.Keys)
                                {
                                    ((Dictionary<string, string>)obj).Add(itm, rawDic[itm].ToString());
                                }
                                prop.SetValue(p, obj, null);
                            }
                            else
                            {
                                prop.SetValue(p, DeserializeObject(jObj.ToObject<IDictionary<string, object>>(), prop.PropertyType, serializer), null);
                            }
                        }
                        else if (dictionary[key] is IList && prop.PropertyType.IsGenericType)
                        {
                            var obj = Activator.CreateInstance(prop.PropertyType);
                            var elementType = prop.PropertyType.GetGenericArguments()[0];
                            foreach (var itm in (IEnumerable)dictionary[key])
                            {
                                if (itm is IDictionary<string, object> itemDict)
                                {
                                    var deserializedItem = DeserializeObject(itemDict, elementType, serializer);
                                    ((IList)obj).Add(deserializedItem);
                                }
                                else if (itm is JObject itemJObj)
                                {
                                    var deserializedItem = DeserializeObject(
                                        itemJObj.ToObject<IDictionary<string, object>>(),
                                        elementType,
                                        serializer);
                                    ((IList)obj).Add(deserializedItem);
                                }
                                else
                                {
                                    ((IList)obj).Add(itm);
                                }
                            }
                            prop.SetValue(p, obj, null);
                        }
                        else if (dictionary[key] is JArray jArrayValue && prop.PropertyType.IsGenericType)
                        {
                            var obj = Activator.CreateInstance(prop.PropertyType);
                            var elementType = prop.PropertyType.GetGenericArguments()[0];
                            foreach (var itm in jArrayValue)
                            {
                                if (itm is JObject itemJObj)
                                {
                                    var deserializedItem = DeserializeObject(
                                        itemJObj.ToObject<IDictionary<string, object>>(),
                                        elementType,
                                        serializer);
                                    ((IList)obj).Add(deserializedItem);
                                }
                                else
                                {
                                    ((IList)obj).Add(itm);
                                }
                            }
                            prop.SetValue(p, obj, null);
                        }
                        else
                        {
                            prop.SetValue(p, GetValueOfType(dictionary[key], prop.PropertyType), null);
                        }
                    }
                }
            }

            return p;
        }

        private object GetValueOfType(object v, Type propertyType)
        {
            if (propertyType == typeof(string))
            {
                return (string)v;
            }
            else if (propertyType == typeof(bool))
            {
                return Convert.ToBoolean(v);
            }
            else if (propertyType == typeof(int))
            {
                return Convert.ToInt32(v);
            }
            else if (propertyType == typeof(long))
            {
                return Convert.ToInt64(v);
            }
            else if (propertyType == typeof(DateTime))
            {
                return Convert.ToDateTime(v);
            }
            else
            {
                return v;
            }
        }

        private Type ResolveType(Type baseType, string typeName)
        {
            try
            {
                var assembly = baseType.Assembly;

                // First, try direct lookup (for fully qualified names)
                var resolvedType = assembly.GetType(typeName, throwOnError: false);
                if (resolvedType != null)
                {
                    return resolvedType;
                }

                // If just a simple name, search all types in the assembly
                if (!typeName.Contains("."))
                {
                    resolvedType = assembly.GetTypes()
                        .FirstOrDefault(t => t.Name == typeName && !t.IsAbstract);

                    if (resolvedType != null)
                    {
                        return resolvedType;
                    }
                }
            }
            catch
            {
                // If resolution fails, return null and use the base type
            }

            return null;
        }
    }
}
