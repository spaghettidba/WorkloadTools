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
    public class ModelConverter<T> : JavaScriptConverter where T: new()
    {
        public override IEnumerable<Type> SupportedTypes
        {
            get
            {
                return new[] { typeof(T) };
            }
        }



        public override object Deserialize(IDictionary<string, object> dictionary, Type type, JavaScriptSerializer serializer)
        {
            T p = new T();

            var props = typeof(T).GetProperties();

            foreach (string key in dictionary.Keys)
            {
                var prop = props.Where(t => t.Name == key).FirstOrDefault();
                if (prop != null)
                {
                    if (prop.Name.EndsWith("Filter"))
                    {
                        if (dictionary[key] is string)
                            prop.SetValue(p, new string[] { (string)dictionary[key] }, null);
                        else
                            prop.SetValue(p, (string[])((ArrayList)dictionary[key]).ToArray(typeof(string)), null);
                    }
                    else
                    {
                        prop.SetValue(p, dictionary[key], null);
                    }
                }
            }

            return p;
        }


        public override IDictionary<string, object> Serialize(object obj, JavaScriptSerializer serializer)
        {
            T p = (T)obj;
            IDictionary<string, object> serialized = new Dictionary<string, object>();

            foreach (PropertyInfo pi in typeof(T).GetProperties())
            {
                serialized[pi.Name] = pi.GetValue(p, null);
            }

            return serialized;
        }

    }
}
