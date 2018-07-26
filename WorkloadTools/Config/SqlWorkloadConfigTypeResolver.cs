using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Web.Script.Serialization;

namespace WorkloadTools.Config
{
    internal class SqlWorkloadConfigTypeResolver : SimpleTypeResolver
    {

        private static Dictionary<string, Type> mappedTypes = new Dictionary<string, Type>();

        static SqlWorkloadConfigTypeResolver()
        {
            Assembly currentAssembly = Assembly.GetExecutingAssembly();
            string nameSpace = "WorkloadTools";
            Type[] types = currentAssembly.GetTypes().Where(t => t != null && t.FullName.StartsWith(nameSpace) & !t.FullName.Contains("+")).ToArray();
            foreach (Type t in types)
            {
                try
                {
                    mappedTypes.Add(t.AssemblyQualifiedName, t);
                    mappedTypes.Add(t.Name, t);
                }
                catch(Exception e)
                {
                    throw;
                }
            }
        }

        public override Type ResolveType(string id)
        {
            if (mappedTypes.ContainsKey(id))
                return mappedTypes[id];
            else return base.ResolveType(id);
        }

        public override string ResolveTypeId(Type type)
        {
            return base.ResolveTypeId(type);
        }
    }
}