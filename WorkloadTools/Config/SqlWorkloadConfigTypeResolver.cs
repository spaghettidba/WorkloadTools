using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Web.Script.Serialization;

namespace WorkloadTools.Config
{
    internal class SqlWorkloadConfigTypeResolver : SimpleTypeResolver
    {

        private static readonly Dictionary<string, Type> mappedTypes = new Dictionary<string, Type>();

        static SqlWorkloadConfigTypeResolver()
        {
            var currentAssembly = Assembly.GetExecutingAssembly();
            var nameSpace = "WorkloadTools";
            var types = currentAssembly.GetTypes().Where(t => t != null && t.FullName.StartsWith(nameSpace) & !t.FullName.Contains("+")).ToArray();
            foreach (var t in types)
            {
                try
                {
                    mappedTypes.Add(t.AssemblyQualifiedName, t);
                    mappedTypes.Add(t.Name, t);
                }
                catch(Exception)
                {
                    throw;
                }
            }
        }

        public override Type ResolveType(string id)
        {
            if (mappedTypes.ContainsKey(id))
            {
                return mappedTypes[id];
            }
            else
            {
                return base.ResolveType(id);
            }
        }

        public override string ResolveTypeId(Type type)
        {
            return base.ResolveTypeId(type);
        }
    }
}