using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using Path = System.IO.Path;
using DirectoryInfo = System.IO.DirectoryInfo;
using Lucene.Net.Store;
using System.Net;
using System.Diagnostics;

namespace Lucene.Net.Store.Azure
{
    public class AzureDirectory : Directory
    {
        private string _containerName;
        private string _rootFolder;
        private CloudBlobClient _blobClient;
        private CloudBlobContainer _blobContainer;
        private Directory _cacheDirectory;

        private AzureLockFactory _lockFactory;


        /// <summary>
        /// Create an AzureDirectory
        /// </summary>
        /// <param name="storageAccount">storage account to use</param>
        /// <param name="containerName">name of container (folder in blob storage)</param>
        /// <param name="cacheDirectory">local Directory object to use for local cache</param>
        /// <param name="rootFolder">path of the root folder inside the container</param>
        public AzureDirectory(
            CloudStorageAccount storageAccount,
            string containerName = null,
            Directory cacheDirectory = null,
            bool compressBlobs = false,
            string rootFolder = null)
        {
            if (storageAccount == null)
                throw new ArgumentNullException("storageAccount");

            if (string.IsNullOrEmpty(containerName))
                _containerName = "lucene";
            else
                _containerName = containerName.ToLower();


            if (string.IsNullOrEmpty(rootFolder))
                _rootFolder = string.Empty;
            else
            {
                rootFolder = rootFolder.Trim('/');
                _rootFolder = rootFolder + "/";
            }


            _blobClient = storageAccount.CreateCloudBlobClient();
            _initCacheDirectory(cacheDirectory);

            _lockFactory = new AzureLockFactory(this, _cacheDirectory, _rootFolder);

            this.CompressBlobs = compressBlobs;
        }

        public CloudBlobContainer BlobContainer
        {
            get
            {
                return _blobContainer;
            }
        }

        public bool CompressBlobs
        {
            get;
            set;
        }

        public void ClearCache()
        {
            foreach (string file in _cacheDirectory.ListAll())
            {
                _cacheDirectory.DeleteFile(file);
            }
        }

        public Directory CacheDirectory
        {
            get
            {
                return _cacheDirectory;
            }
            set
            {
                _cacheDirectory = value;
            }
        }

        public override LockFactory LockFactory
        {
            get
            {
                return _lockFactory;
            }

            set
            {
                throw new NotImplementedException();
            }
        }

        private void _initCacheDirectory(Directory cacheDirectory)
        {
            if (cacheDirectory != null)
            {
                // save it off
                _cacheDirectory = cacheDirectory;
            }
            else
            {
                var cachePath = Path.Combine(Path.GetPathRoot(Environment.SystemDirectory), "AzureDirectory");
                var azureDir = new DirectoryInfo(cachePath);
                if (!azureDir.Exists)
                    azureDir.Create();

                var catalogPath = Path.Combine(cachePath, _containerName);
                var catalogDir = new DirectoryInfo(catalogPath);
                if (!catalogDir.Exists)
                    catalogDir.Create();

                _cacheDirectory = FSDirectory.Open(new DirectoryInfo(catalogPath));
            }

            CreateContainer();
        }

        public void CreateContainer()
        {
            _blobContainer = _blobClient.GetContainerReference(_containerName);
            _blobContainer.CreateIfNotExists();
        }

        /// <summary>Returns an array of strings, one for each file in the directory. </summary>
        public override String[] ListAll()
        {
            var results = from blob in _blobContainer.ListBlobs(_rootFolder)
                          select blob.Uri.AbsolutePath.Substring(blob.Uri.AbsolutePath.LastIndexOf('/') + 1);
            return results.ToArray<string>();
        }

        /// <summary>Returns true if a file with the given name exists. </summary>
        public override bool FileExists(String name)
        {
            Debug.WriteLine(string.Format("File Exists: {0}", name));
            // this always comes from the server
            try
            {
                return _blobContainer.GetBlockBlobReference(_rootFolder + name).Exists();
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>Returns the time the named file was last modified. </summary>
        public long FileModified(String name)
        {
            Debug.WriteLine(string.Format("FileModified: {0}", name));
            // this always has to come from the server
            try
            {
                var blob = _blobContainer.GetBlockBlobReference(_rootFolder + name);
                blob.FetchAttributes();
                return blob.Properties.LastModified.Value.UtcDateTime.ToFileTimeUtc();
            }
            catch
            {
                return 0;
            }
        }

        /// <summary>Set the modified time of an existing file to now. </summary>
        public void TouchFile(System.String name)
        {
            Debug.WriteLine(string.Format("TouchFile: {0}", name));

            //BlobProperties props = _blobContainer.GetBlobProperties(_rootFolder + name);
            //_blobContainer.UpdateBlobMetadata(props);
            // I have no idea what the semantics of this should be...hmmmm...
            // we never seem to get called
            //_cacheDirectory.TouchFile(name);
            //SetCachedBlobProperties(props);

            //throw new NotImplementedException();
        }

        /// <summary>Removes an existing file in the directory. </summary>
        public override void DeleteFile(System.String name)
        {
            Debug.WriteLine(string.Format("DeleteFile: {0}", name));

            // We're going to try to remove this from the cache directory first,
            // because the IndexFileDeleter will call this file to remove files 
            // but since some files will be in use still, it will retry when a reader/searcher
            // is refreshed until the file is no longer locked. So we need to try to remove 
            // from local storage first and if it fails, let it keep throwing the IOExpception
            // since that is what Lucene is expecting in order for it to retry.
            // If we remove the main storage file first, then this will never retry to clean out
            // local storage because the FileExist method will always return false.
            try
            {
                if (_cacheDirectory.FileExists(name + ".blob"))
                {
                    _cacheDirectory.DeleteFile(name + ".blob");
                }

                if (_cacheDirectory.FileExists(name))
                {
                    _cacheDirectory.DeleteFile(name);
                }
            }
            catch (System.IO.IOException ex)
            {
                // This will occur because this file is locked, when this is the case, we don't really want to delete it from the master either because
                // if we do that then this file will never get removed from the cache folder either! This is based on the Deletion Policy which the
                // IndexFileDeleter uses. We could implement our own one of those to deal with this scenario too but it seems the easiest way it to just 
                // let this throw so Lucene will retry when it can and when that is successful we'll also clear it from the master
                throw;
            }

            //if we've made it this far then the cache directly file has been successfully removed so now we'll do the master

            var blob = _blobContainer.GetBlockBlobReference(_rootFolder + name);
            blob.DeleteIfExists();

        }


        /// <summary>Returns the length of a file in the directory. </summary>
        public override long FileLength(String name)
        {
            Debug.WriteLine(string.Format("FileLength: {0}", name));

            return _cacheDirectory.FileLength(name);
            /*
            var blob = _blobContainer.GetBlockBlobReference(_rootFolder + name);
            blob.FetchAttributes();

            // index files may be compressed so the actual length is stored in metatdata
            string blobLegthMetadata;
            bool hasMetadataValue = blob.Metadata.TryGetValue("CachedLength", out blobLegthMetadata);

            long blobLength;
            if (hasMetadataValue && long.TryParse(blobLegthMetadata, out blobLength))
            {
                return blobLength;
            }
            return blob.Properties.Length; // fall back to actual blob size
        */
        }

        /// <summary>Creates a new, empty file in the directory with the given name.
        /// Returns a stream writing this file. 
        /// </summary>
        public override IndexOutput CreateOutput(System.String name, IOContext context)
        {
            Debug.WriteLine(string.Format("CreateOutput: {0}", name));
            var blob = _blobContainer.GetBlockBlobReference(_rootFolder + name);
            return new AzureIndexOutput(this, blob);
        }

        /// <summary>Returns a stream reading an existing file. </summary>
        public override IndexInput OpenInput(System.String name, IOContext context)
        {
            Debug.WriteLine(string.Format("OpenInput: {0}", name));
            try
            {
                var blob = _blobContainer.GetBlockBlobReference(_rootFolder + name);
                blob.FetchAttributes();
                return new AzureIndexInput(this, blob);
            }
            catch (Exception err)
            {
                throw new System.IO.FileNotFoundException(name, err);
            }
        }

        public override void Copy(Directory to, string src, string dest, IOContext context)
        {
            Debug.WriteLine(string.Format("Copy: {0} to {1}", src, dest));


            base.Copy(_cacheDirectory, src, dest, context);
        }


        /// <summary>Construct a {@link Lock}.</summary>
        /// <param name="name">the name of the lock file
        /// </param>
        public override Lock MakeLock(System.String name)
        {
            return _lockFactory.MakeLock(name);
        }

        public override void ClearLock(string name)
        {
            _lockFactory.ClearLock(name);
        }

        /// <summary>Closes the store. </summary>
        protected void Dispose(bool disposing)
        {
            _blobContainer = null;
            _blobClient = null;
        }

        public virtual bool ShouldCompressFile(string path)
        {
            if (!CompressBlobs)
                return false;

            var ext = System.IO.Path.GetExtension(path);
            switch (ext)
            {
                case ".cfs":
                case ".fdt":
                case ".fdx":
                case ".frq":
                case ".tis":
                case ".tii":
                case ".nrm":
                case ".tvx":
                case ".tvd":
                case ".tvf":
                case ".prx":
                    return true;
                default:
                    return false;
            };
        }

        


        public override void Sync(ICollection<string> names)
        {
            //_cacheDirectory.Sync(names);
            /*
            foreach (var name in _cacheDirectory.ListAll())
            {
                var blob = _blobContainer.GetBlockBlobReference(_rootFolder + name);
                try
                {
                    var output = CreateOutput(name, IOContext.DEFAULT);
                    output.Dispose();
                }
                catch (Exception ex)
                {

                }
            }
            */
            //throw new NotImplementedException();
        }

        public override void Dispose()
        {
        }
    }

}
