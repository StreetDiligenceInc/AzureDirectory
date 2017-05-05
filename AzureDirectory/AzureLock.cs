using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace Lucene.Net.Store.Azure
{
    /// <summary>
    /// Implements lock semantics on AzureDirectory via a blob lease
    /// </summary>
    public class AzureLock : Lock
    {
        private string _lockFile;
        private AzureDirectory _azureDirectory;
        private string _leaseid;

        public AzureLock(string lockFile, AzureDirectory directory)
        {
            _lockFile = lockFile;
            _azureDirectory = directory;
        }

        #region Lock methods
        private bool IsLocked()
        {
            var blob = _azureDirectory.BlobContainer.GetBlockBlobReference(_lockFile);
            try
            {
                Debug.Print("IsLocked() : {0}", _leaseid);
                if (String.IsNullOrEmpty(_leaseid))
                {
                    var tempLease = blob.AcquireLease(TimeSpan.FromSeconds(60), _leaseid);
                    if (String.IsNullOrEmpty(tempLease))
                    {
                        Debug.Print("IsLocked() : TRUE");
                        return true;
                    }
                    blob.ReleaseLease(new AccessCondition() { LeaseId = tempLease });
                }
                Debug.Print("IsLocked() : {0}", _leaseid);
                return String.IsNullOrEmpty(_leaseid);
            }
            catch (StorageException webErr)
            {
                if (_handleWebException(blob, webErr))
                    return IsLocked();
            }
            _leaseid = null;
            return false;
        }

        public override bool Obtain()
        {
            var blob = _azureDirectory.BlobContainer.GetBlockBlobReference(_lockFile);
            try
            {
                Debug.Print("AzureLock:Obtain({0}) : {1}", _lockFile, _leaseid);
                if (String.IsNullOrEmpty(_leaseid))
                {
                    //infinite lease
                    _leaseid = blob.AcquireLease(null, _leaseid);
                    Debug.Print("AzureLock:Obtain({0}): AcquireLease : {1}", _lockFile, _leaseid);
                }
                return !String.IsNullOrEmpty(_leaseid);
            }
            catch (StorageException webErr)
            {
                if (_handleWebException(blob, webErr))
                    return Obtain();
            }
            return false;
        }

        public override bool Locked
        {
            get
            {
                return IsLocked();
            }
        }

        public void Renew()
        {
            if (!String.IsNullOrEmpty(_leaseid))
            {
                Debug.Print("AzureLock:Renew({0} : {1}", _lockFile, _leaseid);
                var blob = _azureDirectory.BlobContainer.GetBlockBlobReference(_lockFile);
                blob.RenewLease(new AccessCondition { LeaseId = _leaseid });
            }
        }

        public override void Release()
        {
            Debug.Print("AzureLock:Release({0}) {1}", _lockFile, _leaseid);
            if (!String.IsNullOrEmpty(_leaseid))
            {
                var blob = _azureDirectory.BlobContainer.GetBlockBlobReference(_lockFile);
                blob.ReleaseLease(new AccessCondition { LeaseId = _leaseid });
                _leaseid = null;
            }
        }
        #endregion

        public void BreakLock()
        {
            Debug.Print("AzureLock:BreakLock({0}) {1}", _lockFile, _leaseid);
            var blob = _azureDirectory.BlobContainer.GetBlockBlobReference(_lockFile);
            try
            {
                blob.BreakLease();
            }
            catch (Exception)
            {
            }
            _leaseid = null;
        }

        public override System.String ToString()
        {
            return String.Format("AzureLock@{0}.{1}", _lockFile, _leaseid);
        }

        private bool _handleWebException(ICloudBlob blob, StorageException err)
        {
            if (err.RequestInformation.HttpStatusCode == 404 || err.RequestInformation.HttpStatusCode == 409)
            {
                _azureDirectory.CreateContainer();
                using (var stream = new MemoryStream())
                using (var writer = new StreamWriter(stream))
                {
                    writer.Write(_lockFile);
                    blob.UploadFromStream(stream);
                }
                return true;
            }
            return false;
        }

    }

}
