using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace SFBulkAPIStarter
{
    public class Batch
    {
        public String Id { get; set; }
        public String JobId { get; set; }
        public String State { get; set; }
        public DateTime CreatedDate { get; set; }
        public DateTime SystemModStamp { get; set; }
        public int NumberRecordsProcessed { get; set; }
        public int NumberRecordsFailed { get; set; }
        public int TotalProcessingTime { get; set; }
        public int ApiActiveProcessingTime { get; set; }
        public int ApexProcessingTime { get; set; }

        public static List<Batch> CreateBatches(String batchesJson)
        {
            List<Batch> batches = JsonConvert.DeserializeObject<List<Batch>>(batchesJson);
            return batches;
        }

        public static Batch CreateFromJson(string batchJson)
        {
            Batch deserializedBatch = JsonConvert.DeserializeObject<Batch>(batchJson);
            return deserializedBatch;
        }
    }
}
