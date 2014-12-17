
using System;
using System.IO;
using System.Text;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
namespace Fetcher
{
    internal sealed class Lock
    {
        #region Singleton implementation
        private static readonly Lock instance = new Lock();
        private static readonly CloudBlockBlob lockBlob;

        // Explicit static constructor to tell C# compiler
        // not to mark type as beforefieldinit
        static Lock() {
            try
            {
                var connStr = RoleEnvironment.GetConfigurationSettingValue("StorageAccount"); // LocalAzureStorageEmulator_PortOverrides 
                var storageAccount = CloudStorageAccount.Parse(connStr);

                var lockContainer = storageAccount.CreateCloudBlobClient().GetContainerReference(
                    RoleEnvironment.GetConfigurationSettingValue("LockBlobContainer"));
                lockContainer.CreateIfNotExists();

                lockBlob = lockContainer.GetBlockBlobReference(RoleEnvironment.GetConfigurationSettingValue("LockBlob"));
                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes("LOCK")))
                {
                    lockBlob.UploadFromStream(stream);
                }

                var results = storageAccount.CreateCloudTableClient().GetTableReference(RoleEnvironment.GetConfigurationSettingValue("PollResultsTable"));
                results.CreateIfNotExists();
            }
            catch (Exception ex)
            {
                C.Log("Error during initialization: ", ex);
            }
        }

        private Lock() { }

        public static Lock _ { get { return instance; } }
        #endregion

        #region Methods
        internal static void Init() { } // do nothing

        internal bool AcquireLease()
        {
            try
            {
                lockBlob.AcquireLease(TimeSpan.FromSeconds(45), null);
                return true;
            }
            catch (Exception ex)
            {
                C.Log("Lease acquisition failure, role instance: {0}", ex, C.Id);
                return false;
            }
        }
        #endregion
    }
}
