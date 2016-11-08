using System;
using System.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SFBulkAPIStarter;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace SFBulkAPIStarterTest
{
    [TestClass]
    public class TestBulkApiClient
    {
        SFBulkAPIStarter.BulkApiClient _apiClient = null;

        [TestInitialize]
        public void Setup()
        {
            String username = ConfigurationManager.AppSettings["Username"];
            String password = ConfigurationManager.AppSettings["Password"];
            String loginUrl = ConfigurationManager.AppSettings["LoginUrl"];
            String securityToken = ConfigurationManager.AppSettings["SecurityToken"];

            _apiClient = new SFBulkAPIStarter.BulkApiClient(username, password + securityToken, loginUrl);
        }

        [TestMethod]
        public void CreateAccountJobTest()
        {
            JobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();

            Job job = _apiClient.CreateJob(jobRequest);

            Assert.IsTrue(job != null);
            Assert.IsTrue(String.IsNullOrWhiteSpace(job.id) == false);
            Assert.AreEqual("Open", job.state);

            Job closedJob = _apiClient.CloseJob(job.id);

            Assert.AreEqual("Closed", closedJob.state);
        }

        [TestMethod]
        public void GetAccountJobTest()
        {
            JobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();

            Job job = _apiClient.CreateJob(jobRequest);
            Job closedJob = _apiClient.CloseJob(job.id);

            closedJob = _apiClient.GetJob(closedJob.id);

            Assert.IsTrue(closedJob != null);
            Assert.AreEqual("Closed", closedJob.state);
        }
        [TestMethod]
        public void InsertAccountsWith1BatchTest()
        {
            JobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            BatchRequest batchRequest = buildCreateBatchRequest(job.id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            Assert.IsTrue(accountBatch != null);
            Assert.IsTrue(String.IsNullOrWhiteSpace(accountBatch.Id) == false);

            _apiClient.CloseJob(job.id);

            job = _apiClient.GetJob(job.id);

            while (job.IsDone == false)
            {
                Thread.Sleep(2000);
                job = _apiClient.GetJob(job.id);
            }

            Assert.IsTrue(job.numberRecordsFailed == 0);
            Assert.IsTrue(job.numberRecordsProcessed == 1);
        }

        [TestMethod]
        public void GetBatchTest()
        {
            JobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            BatchRequest batchRequest = buildCreateBatchRequest(job.id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            Assert.IsTrue(String.IsNullOrWhiteSpace(accountBatch.JobId) == false);
            Assert.IsTrue(String.IsNullOrWhiteSpace(accountBatch.Id) == false);

            Batch batch = _apiClient.GetBatch(accountBatch.JobId, accountBatch.Id);

            Assert.AreEqual(accountBatch.JobId, batch.JobId);
            Assert.AreEqual(accountBatch.Id, batch.Id);
        }

        [TestMethod]
        public void GetBatchesTest()
        {
            JobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            BatchRequest batchRequest = buildCreateBatchRequest(job.id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            String batchContents2 = "Name" + Environment.NewLine;
            String accountName2 = "Test Name2";
            batchContents2 += accountName2;

            BatchRequest batchRequest2 = buildCreateBatchRequest(job.id, batchContents2);

            Batch accountBatch2 = _apiClient.CreateBatch(batchRequest2);

            List<Batch> batches = _apiClient.GetBatches(job.id);

            Assert.AreEqual(2, batches.Count);
        }

        [TestMethod]
        public void GetBatchRequestTest()
        {
            JobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            BatchRequest batchRequest = buildCreateBatchRequest(job.id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            String batchRequestContents = _apiClient.GetBatchRequest(accountBatch.JobId, accountBatch.Id);

            Assert.AreEqual(batchContents, batchRequestContents);
        }

        [TestMethod]
        public void GetBatchResultsTest()
        {
            JobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            BatchRequest batchRequest = buildCreateBatchRequest(job.id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            String batchResults = _apiClient.GetBatchResults(accountBatch.JobId, accountBatch.Id);

            Assert.IsTrue(String.IsNullOrWhiteSpace(batchResults) == false);
        }

        [TestMethod]
        public void QueryAccountTest()
        {
            // Insert an account so there's at least one to query

            JobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            BatchRequest batchRequest = buildCreateBatchRequest(job.id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            _apiClient.CloseJob(job.id);

            job = _apiClient.GetJob(job.id);

            while (job.IsDone == false)
            {
                Thread.Sleep(2000);
                job = _apiClient.GetJob(job.id);
            }

            JobRequest queryAccountJobRequest = buildDefaultQueryAccountCreateJobRequest();
            Job queryJob = _apiClient.CreateJob(queryAccountJobRequest);

            String accountQuery = "SELECT Id, Name FROM Account WHERE Name = '" + accountName + "'";

            BatchRequest queryBatchRequest = buildCreateBatchRequest(queryJob.id, accountQuery);
            queryBatchRequest.BatchContentType = BatchContentType.CSV;
            Batch queryBatch = _apiClient.CreateBatch(queryBatchRequest);

            _apiClient.CloseJob(queryJob.id);

            queryJob = _apiClient.GetJob(queryJob.id);

            while (job.IsDone == false)
            {
                Thread.Sleep(2000);
                queryJob = _apiClient.GetJob(queryJob.id);
            }

            String batchQueryResultsList = _apiClient.GetBatchResults(queryBatch.JobId, queryBatch.Id);

            List<String> resultIds = _apiClient.GetResultIds(batchQueryResultsList);

            Assert.AreEqual(1, resultIds.Count);

            String batchQueryResults = _apiClient.GetBatchResult(queryBatch.JobId, queryBatch.Id, resultIds[0]);

            Assert.IsTrue(batchQueryResults.Contains(accountName));
        }

        [TestMethod]
        public void UpsertAccountTest()
        {
            // Upsert an account
            String externalFieldName = ConfigurationManager.AppSettings["ExternalFieldName"];
            JobRequest jobRequest = buildDefaultUpsertAccountCreateJobRequest(externalFieldName);

            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            BatchRequest batchRequest = buildCreateBatchRequest(job.id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            _apiClient.CloseJob(job.id);

            job = _apiClient.GetJob(job.id);

            while (job.IsDone == false)
            {
                Thread.Sleep(2000);
                job = _apiClient.GetJob(job.id);
            }

            Assert.IsTrue(job.numberBatchesFailed == 0);
            Assert.IsTrue(job.numberRecordsProcessed > 0);
        }

        [TestMethod]
        public void DeleteAccountTest()
        {
            // Insert an account so there's at least one to delete

            JobRequest jobRequest = buildDefaultInsertAccountCreateJobRequest();
            Job job = _apiClient.CreateJob(jobRequest);

            String batchContents = "Name" + Environment.NewLine;
            String accountName = "Test Name";
            batchContents += accountName;

            BatchRequest batchRequest = buildCreateBatchRequest(job.id, batchContents);

            Batch accountBatch = _apiClient.CreateBatch(batchRequest);

            _apiClient.CloseJob(job.id);

            job = _apiClient.GetJob(job.id);

            while (job.IsDone == false)
            {
                Thread.Sleep(2000);
                job = _apiClient.GetJob(job.id);
            }

            // Query the accounts to dynamically retreive the account id to delete
            JobRequest queryAccountJobRequest = buildDefaultQueryAccountCreateJobRequest();
            Job queryJob = _apiClient.CreateJob(queryAccountJobRequest);

            String accountQuery = "SELECT Id FROM Account WHERE Name = '" + accountName + "'";

            BatchRequest queryBatchRequest = buildCreateBatchRequest(queryJob.id, accountQuery);
            queryBatchRequest.BatchContentType = BatchContentType.CSV;
            Batch queryBatch = _apiClient.CreateBatch(queryBatchRequest);

            _apiClient.CloseJob(queryJob.id);

            queryJob = _apiClient.GetJob(queryJob.id);

            while (queryJob.IsDone == false)
            {
                Thread.Sleep(2000);
                queryJob = _apiClient.GetJob(queryJob.id);
            }

            String batchQueryResultsList = _apiClient.GetBatchResults(queryBatch.JobId, queryBatch.Id);

            List<String> resultIds = _apiClient.GetResultIds(batchQueryResultsList);

            Assert.AreEqual(1, resultIds.Count);

            String batchQueryResults = _apiClient.GetBatchResult(queryBatch.JobId, queryBatch.Id, resultIds[0]);

            String[] batchQueryResultsParts = batchQueryResults.Split(new String[] { "\n" }, StringSplitOptions.RemoveEmptyEntries);

            String firstAccountIdToDelete = batchQueryResultsParts[1].Replace(@"""", String.Empty);


            // Delete the account
            JobRequest deleteAccountJobRequest = buildDefaultDeleteAccountCreateJobRequest();
            Job deleteJob = _apiClient.CreateJob(deleteAccountJobRequest);

            String deleteBatchContents = "Id" + Environment.NewLine + firstAccountIdToDelete;

            BatchRequest deleteBatchRequest = buildCreateBatchRequest(deleteJob.id, deleteBatchContents);
            Batch deleteBatch = _apiClient.CreateBatch(deleteBatchRequest);

            _apiClient.CloseJob(deleteJob.id);

            deleteJob = _apiClient.GetJob(deleteJob.id);

            while (deleteJob.IsDone == false)
            {
                Thread.Sleep(2000);
                deleteJob = _apiClient.GetJob(deleteJob.id);
            }

            Assert.AreEqual(1, deleteJob.numberRecordsProcessed);
        }

        private JobRequest buildDefaultDeleteAccountCreateJobRequest()
        {
            return buildDefaultAccountCreateJobRequest(JobOperation.Delete);
        }

        private JobRequest buildDefaultQueryAccountCreateJobRequest()
        {
            return buildDefaultAccountCreateJobRequest(JobOperation.Query);
        }

        private JobRequest buildDefaultUpsertAccountCreateJobRequest(String externalIdFieldName)
        {
            JobRequest request = buildDefaultAccountCreateJobRequest(JobOperation.Upsert);

            request.ExternalIdFieldName = externalIdFieldName;

            return request;
        }

        private JobRequest buildDefaultInsertAccountCreateJobRequest()
        {
            return buildDefaultAccountCreateJobRequest(JobOperation.Insert);
        }

        private JobRequest buildDefaultAccountCreateJobRequest(JobOperation operation)
        {
            JobRequest jobRequest = new JobRequest();
            jobRequest.ContentType = JobContentType.CSV;
            jobRequest.Operation = operation;
            jobRequest.Object = "Account";

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
