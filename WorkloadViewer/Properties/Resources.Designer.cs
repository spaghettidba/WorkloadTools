﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace WorkloadViewer.Properties {
    using System;
    
    
    /// <summary>
    ///   A strongly-typed resource class, for looking up localized strings, etc.
    /// </summary>
    // This class was auto-generated by the StronglyTypedResourceBuilder
    // class via a tool like ResGen or Visual Studio.
    // To add or remove a member, edit your .ResX file then rerun ResGen
    // with the /str option, or rebuild your VS project.
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Resources.Tools.StronglyTypedResourceBuilder", "17.0.0.0")]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    public class Resources {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal Resources() {
        }
        
        /// <summary>
        ///   Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        public static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("WorkloadViewer.Properties.Resources", typeof(Resources).Assembly);
                    resourceMan = temp;
                }
                return resourceMan;
            }
        }
        
        /// <summary>
        ///   Overrides the current thread's CurrentUICulture property for all
        ///   resource lookups using this strongly typed resource class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        public static global::System.Globalization.CultureInfo Culture {
            get {
                return resourceCulture;
            }
            set {
                resourceCulture = value;
            }
        }
        
        /// <summary>
        ///   Looks up a localized resource of type System.Byte[].
        /// </summary>
        public static byte[] TSQL {
            get {
                object obj = ResourceManager.GetObject("TSQL", resourceCulture);
                return ((byte[])(obj));
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to WITH baseData AS (
        ///	SELECT 
        ///		DATEDIFF(minute, Base.end_time, bIn.end_time) AS offset_minutes,
        ///		bWD.sql_hash, 
        ///		bWD.avg_cpu_us, 
        ///		bWD.min_cpu_us, 
        ///		bWD.max_cpu_us, 
        ///		bWD.sum_cpu_us, 
        ///		bWD.avg_reads, 
        ///		bWD.min_reads, 
        ///		bWD.max_reads, 
        ///		bWD.sum_reads, 
        ///		bWD.avg_writes, 
        ///		bWD.min_writes, 
        ///		bWD.max_writes, 
        ///		bWD.sum_writes, 
        ///		bWD.avg_duration_us, 
        ///		bWD.min_duration_us, 
        ///		bWD.max_duration_us, 
        ///		bWD.sum_duration_us, 
        ///		bWD.execution_count,
        ///		bIn.duration_minutes, 
        ///		bNQ.norm [rest of string was truncated]&quot;;.
        /// </summary>
        public static string WorkloadAnalysis {
            get {
                return ResourceManager.GetString("WorkloadAnalysis", resourceCulture);
            }
        }
    }
}
