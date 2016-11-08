using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using System.Xml.Serialization;
using Newtonsoft.Json;

namespace SFBulkAPIStarter
{
    public class Job
    {
#pragma region properties
        //public String Id { get; set; }
        //public String Operation { get; set; }
        //public String Object { get; set; }
        //public String CreatedById { get; set; }
        //public DateTime CreatedDate { get; set; }
        //public DateTime SystemModStamp { get; set; }
        //public String State { get; set; }
        //public String ConcurrencyMode { get; set; }
        //public String ContentType { get; set; }
        //public int NumberBatchesQueued { get; set; }
        //public int NumberBatchesInProgress { get; set; }
        //public int NumberBatchesCompleted { get; set; }
        //public int NumberBatchesFailed { get; set; }
        //public int NumberBatchesTotal { get; set; }
        //public int NumberRecordsProcessed { get; set; }
        //public int NumberRecordsFailed { get; set; }
        //public int NumberRetries { get; set; }
        //public int TotalProcessingTime { get; set; }
        //public int ApiActiveProcessingTime { get; set; }
        //public int ApexProcessingTime { get; set; }  
#pragma endregion
        public String id ;
        public String operation ;
        public String _object;
        public String createdById ;
        public DateTime createdDate ;
        public DateTime systemModStamp ;
        public String state ;
        public String concurrencyMode ;
        public String contentType ;
        public int numberBatchesQueued ;
        public int numberBatchesInProgress ;
        public int numberBatchesCompleted ;
        public int numberBatchesFailed ;
        public int numberBatchesTotal ;
        public int numberRecordsProcessed ;
        public int numberRecordsFailed ;
        public int numberRetries ;
        public int totalProcessingTime ;
        public int apiActiveProcessingTime ;
        public int apexProcessingTime ;

        public static Job CreateFromJson(String jobJson)
        {
            Job deserializedJob = JsonConvert.DeserializeObject<Job>(jobJson);
            return deserializedJob;
        }

        public bool IsDone
        {
            get
            {
                return numberBatchesTotal == (numberBatchesCompleted + numberBatchesFailed) ||
                       state == "Aborted";
            }
        }
    }

    public enum JobOperation
    {
        Query,
        Insert,
        Update,
        Delete,
        HardDelete,
        Upsert
    }

    public enum JobContentType
    {
        CSV,
        XML,
        JSON
    }
}
