using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;

namespace CleanupBlobFunction
{
    public static class CleanUpBlobStorage
    {
        [FunctionName("CleanUpBlobStorageAtMidnight")]
        public static async Task Run([TimerTrigger("0/10 * * * * *")]TimerInfo myTimer, ILogger log)
        {
            var connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var cloudBlobClient = storageAccount.CreateCloudBlobClient();

            var containerName = Environment.GetEnvironmentVariable("ContainerName") ?? "supercoolcontainer";
            var container = cloudBlobClient.GetContainerReference(containerName);
            await container.CreateIfNotExistsAsync();

            var blobs = container.ListBlobs();
            using (var memoryStream = new MemoryStream())
            {
                using (var archive = new ZipArchive(memoryStream, ZipArchiveMode.Create, true))
                {
                    var blobsToPack = blobs.Where(b =>
                                            b.GetType() == typeof(CloudBlockBlob))
                                            .Cast<CloudBlockBlob>()
                                            .Where(a => !a.Name.EndsWith(".zip"));

                    foreach (var blob in blobsToPack)
                    {
                        var entry = archive.CreateEntry(blob.Name);

                        using (var entryStream = entry.Open())
                        {
                            await blob.DownloadToStreamAsync(entryStream);
                            log.LogInformation($"added {blob.Name} to the zip");
                        }

                        await blob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, AccessCondition.GenerateIfExistsCondition(), null, null);
                        log.LogInformation($"remove {blob.Name} from the container");

                    }
                }
                var filename = DateTime.Now.ToString("yyyyMMdd");
                var reference = container.GetBlockBlobReference(filename + ".zip");
                memoryStream.Position = 0;
                await reference.UploadFromStreamAsync(memoryStream);
            }
        }
    }
}
