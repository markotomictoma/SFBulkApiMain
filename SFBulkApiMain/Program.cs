using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SFBulkAPIStarter;
using System.Threading;
using System.Xml.Serialization;

namespace SFBulkApiMain
{
    class Program
    {
        static void Main(string[] args)
        {
            //Job j = new Job();
            //var stringwriter = new System.IO.StringWriter();
            //var serializer = new XmlSerializer(j.GetType());
            //serializer.Serialize(stringwriter, j);
            //string s = stringwriter.ToString();



            String username = ConfigurationManager.AppSettings["Username"];
            String password = ConfigurationManager.AppSettings["Password"];
            String loginUrl = ConfigurationManager.AppSettings["LoginUrl"];
            String securityToken = ConfigurationManager.AppSettings["SecurityToken"];

            SFBulkAPIStarter.BulkApiClient _apiClient = new SFBulkAPIStarter.BulkApiClient(username, password + securityToken, loginUrl);
            Program p = new Program();
            //p.QueryAccountTest(_apiClient);
            //p.InsertAccount(_apiClient);
            BatchZdfSachMsn batchzdfsachmsn = new BatchZdfSachMsn();
            batchzdfsachmsn.Execute(_apiClient);
            Console.ReadLine();
        }

        private void InsertAccount(SFBulkAPIStarter.BulkApiClient _apiClient)
        {
            JobRequest jobrequest = buildDefaultCreateJobRequest(JobOperation.Insert, "Account");
            Job job = _apiClient.CreateJob(jobrequest);

            string batchcontents = "name" + Environment.NewLine;
            string accountname = "test name";
            batchcontents += accountname;

            BatchRequest batchrequest = buildCreateBatchRequest(job.id, batchcontents);

            Batch accountbatch = _apiClient.CreateBatch(batchrequest);

            job = _apiClient.GetJob(job.id);

            while (job.IsDone == false)
            {
                Thread.Sleep(2000);
                job = _apiClient.GetJob(job.id);
            }
            _apiClient.CloseJob(job.id);
        }

        private void QueryTest(SFBulkAPIStarter.BulkApiClient _apiClient)
        {
            DateTime start = DateTime.Now;
            JobRequest queryAccountJobRequest = buildDefaultCreateJobRequest(JobOperation.Query, "opk_301__c");
            Job queryJob = _apiClient.CreateJob(queryAccountJobRequest);

            String accountQuery = "SELECT Id, Name FROM opk_301__c";
            BatchRequest queryBatchRequest = buildCreateBatchRequest(queryJob.id, accountQuery);
            queryBatchRequest.BatchContentType = BatchContentType.CSV;
            List<Batch> batches = new List<Batch>();

            Batch queryBatch = _apiClient.CreateBatch(queryBatchRequest);
            //batches.Add(queryBatch);
            //for(int i = 0; i < 19; i++)
            //{
            //    batches.Add(_apiClient.CreateBatch(queryBatchRequest));
            //}

            

            queryJob = _apiClient.GetJob(queryJob.id);

            while (queryJob.IsDone == false)
            {
                Thread.Sleep(2000);
                queryJob = _apiClient.GetJob(queryJob.id);
            }

            String batchQueryResultsList = _apiClient.GetBatchResults(queryBatch.JobId, queryBatch.Id);
            Console.WriteLine(batchQueryResultsList);
            Console.WriteLine("____________________________________________________________________");
            List<String> resultIds = _apiClient.GetResultIds(batchQueryResultsList);


            String batchQueryResults = _apiClient.GetBatchResult(queryBatch.JobId, queryBatch.Id, resultIds[0]);
            _apiClient.CloseJob(queryJob.id);
            Console.WriteLine(batchQueryResults);
            DateTime end = DateTime.Now;

            Console.WriteLine((start - end).TotalSeconds);
        }

        private JobRequest buildDefaultInsertAccountCreateJobRequest()
        {
            return buildDefaultCreateJobRequest(JobOperation.Insert, "Account");
        }

        private JobRequest buildDefaultCreateJobRequest(JobOperation operation, string objName)
        {
            JobRequest jobRequest = new JobRequest();
            jobRequest.ContentType = JobContentType.CSV;
            jobRequest.Operation = operation;
            jobRequest.Object = objName;

            return jobRequest;
        }

        private BatchRequest buildCreateBatchRequest(String jobId, String batchContents)
        {
            return new BatchRequest()
            {
                JobId = jobId,
                BatchContents = batchContents
            };
        }
    }
}
