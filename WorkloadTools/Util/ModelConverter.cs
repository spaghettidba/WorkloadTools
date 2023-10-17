using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using WorkloadTools;

namespace WorkloadTools.Util
{
    public class ModelConverter : JavaScriptConverter
    {
        public override IEnumerable<Type> SupportedTypes
        {
            get
            {
                var result = new List<Type>();
                var currentAssembly = Assembly.GetExecutingAssembly();
                var nameSpace = "WorkloadTools";
                var types = currentAssembly.GetTypes().Where(t => t != null && t.FullName.StartsWith(nameSpace) & !t.FullName.Contains("+")).ToArray();
                foreach (var t in types)
                {
                    try
                    {
                        result.Add(t);
                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
                return result;
            }
        }

        public override object Deserialize(IDictionary<string, object> dictionary, Type type, JavaScriptSerializer serializer)
        {
            object p;
            try
            {
                // try to create the object using its parameterless constructor
                p = Activator.CreateInstance(type);
            }
            catch {
                // try to create the object using this scary initializer that 
                // doesn't need the parameterless constructor
                p = System.Runtime.Serialization.FormatterServices.GetUninitializedObject(type);
            }

            var props = type.GetProperties();

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
                        else
                        {
                            prop.SetValue(p, (string[])((ArrayList)dictionary[key]).ToArray(typeof(string)), null);
                        }
                    }
                    else
                    {
                        if (dictionary[key] is Dictionary<string, object> dictionaryValue)
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
                                prop.SetValue(p, Deserialize(dictionaryValue, prop.PropertyType, serializer), null);
                            }
                        }
                        else
                        {
                            if (dictionary[key] is IList && prop.PropertyType.IsGenericType)
                            {
                                var obj = Activator.CreateInstance(prop.PropertyType);
                                foreach (var itm in (IEnumerable)dictionary[key])
                                {
                                    _ = ((IList)obj).Add(itm);
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

        public override IDictionary<string, object> Serialize(object obj, JavaScriptSerializer serializer)
        {
            throw new NotImplementedException();
        }

    }
}
