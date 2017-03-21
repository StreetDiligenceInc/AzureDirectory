using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lucene.Net.Store.Azure
{
    internal class AzureLockFactory : LockFactory
    {
        private AzureDirectory _directory;
        private Directory _cacheDirectory;
        private string _rootFolder;
        private Dictionary<string, AzureLock> _locks = new Dictionary<string, AzureLock>();

        public AzureLockFactory(AzureDirectory directory, Directory cacheDirectory, string rootFolder)
        {
            _directory = directory;
            _cacheDirectory = cacheDirectory;
            _rootFolder = rootFolder;
        }

        public override void ClearLock(string name)
        {
            lock (_locks)
            {
                if (_locks.ContainsKey(name))
                {
                    _locks[name].BreakLock();
                }
            }
            _cacheDirectory.ClearLock(name);

        }

        public override Lock MakeLock(string name)
        {
            lock (_locks)
            {
                if (!_locks.ContainsKey(name))
                {
                    _locks.Add(name, new AzureLock(_rootFolder + name, _directory));
                }
                return _locks[name];
            }
        }
    }
}
