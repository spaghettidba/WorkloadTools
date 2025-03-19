using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadViewer.ViewModel
{
    public static class DictionaryExtensions
    {
        public static void AddOrUpdate<TKey, TValue>(
            this IDictionary<TKey, TValue> dict,
            TKey key,
            TValue addValue)
        {
            if (dict.ContainsKey(key))
            {
                dict[key] = addValue;
            }
            else
            {
                dict.Add(key, addValue);
            }
        }

        public static TValue AddOrUpdate<TKey, TValue>(
            this IDictionary<TKey, TValue> dict,
            TKey key,
            TValue addValue,
            Func<TKey, TValue, TValue> updateValueFactory)
        {
            TValue existing;
            if (dict.TryGetValue(key, out existing))
            {
                addValue = updateValueFactory(key, existing);
                dict[key] = addValue;
            }
            else
            {
                dict.Add(key, addValue);
            }

            return addValue;
        }


        public static TValue AddOrUpdate<TKey, TValue>(
            this IDictionary<TKey, TValue> dict,
            TKey key,
            Func<TKey, TValue> addValueFactory,
            Func<TKey, TValue, TValue> updateValueFactory)
        {
            TValue existing;
            if (dict.TryGetValue(key, out existing))
            {
                existing = updateValueFactory(key, existing);
                dict[key] = existing;
            }
            else
            {
                existing = addValueFactory(key);
                dict.Add(key, existing);
            }

            return existing;
        }
    }
}
