using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadTools.Listener.ExtendedEvents.DBStreamReader
{
    public sealed class DatabaseFactory
    {
        public enum DatabaseProvider
        {
            LiteDb = 1,
            Sqlite = 2
        }

        private static Lazy<IDbUtility> _currentUtility = null;
        private static DatabaseProvider _databaseProvider = DatabaseProvider.LiteDb;
        public static IDbUtility Current
        {
            get
            {
                if (_currentUtility == null)
                {
                    _currentUtility = new Lazy<IDbUtility>(() => Create());
                }
                return _currentUtility.Value;
            }
        }

        public static void SetProvider(DatabaseProvider providerValue)
        {
            _databaseProvider = providerValue;
        }

        public static IDbUtility Create()
        {
            switch (_databaseProvider)
            {
                case DatabaseProvider.LiteDb:
                    return new LiteDBProvider();
                case DatabaseProvider.Sqlite:
                    return new SQLiteProvider();
                default:
                    throw new NotSupportedException($"Valore:{_databaseProvider} non supportato");
            }


        }
    }

}
