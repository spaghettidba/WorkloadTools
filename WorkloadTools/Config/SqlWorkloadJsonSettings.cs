using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace WorkloadTools.Config
{
    internal class SqlWorkloadJsonSettings : JsonSerializerSettings
    {
        private static readonly Dictionary<string, Type> mappedTypes = new Dictionary<string, Type>();

        static SqlWorkloadJsonSettings()
        {
            var currentAssembly = Assembly.GetExecutingAssembly();
            var nameSpace = "WorkloadTools";
            var types = currentAssembly.GetTypes().Where(t => t != null && t.FullName.StartsWith(nameSpace) && !t.FullName.Contains("+")).ToArray();
            foreach (var t in types)
            {
                try
                {
                    mappedTypes.Add(t.AssemblyQualifiedName, t);
                    mappedTypes.Add(t.Name, t);
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }

        public SqlWorkloadJsonSettings()
        {
            TypeNameHandling = TypeNameHandling.Objects;
            Converters = new JsonConverter[] { new WorkloadTools.Util.ModelConverter() };
            SerializationBinder = new CustomBinder();
        }

        private class CustomBinder : ISerializationBinder
        {
            public Type BindToType(string assemblyName, string typeName)
            {
                // First try the mapped types dictionary
                if (mappedTypes.ContainsKey(typeName))
                {
                    return mappedTypes[typeName];
                }

                // Then try to resolve using full name
                if (mappedTypes.ContainsKey(assemblyName + "." + typeName))
                {
                    return mappedTypes[assemblyName + "." + typeName];
                }

                // Fallback to default behavior
                Type t = Type.GetType($"{typeName}, {assemblyName}");
                return t ?? throw new InvalidOperationException($"Unable to resolve type '{typeName}' from assembly '{assemblyName}'");
            }

            public void BindToName(Type serializedType, out string assemblyName, out string typeName)
            {
                assemblyName = serializedType.AssemblyQualifiedName.Split(',')[1].Trim();
                typeName = serializedType.FullName;
            }
        }
    }
}
