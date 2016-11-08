using Newtonsoft.Json;
using SFBulkAPIStarter;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SFBulkApiMain
{
    class BatchZdfSachMsn
    {
        public void Execute(SFBulkAPIStarter.BulkApiClient _apiClient)
        {
            //List<Account> accs = GetAccounts(_apiClient);
            //SFDataContext dataCtx = new SFDataContext();
            //dataCtx.Accounts.InsertAllOnSubmit(accs);

            //List<Maklerverknuepfung__c> accVtnrs = GetAccVtnrs(_apiClient);
            //dataCtx.Maklerverknuepfung__cs.InsertAllOnSubmit(accVtnrs);
            //dataCtx.SubmitChanges();
            List<ZDF_Aggregation__c> vtrnAggrs = GetVtnrAggrs(_apiClient);
        }

       

        private List<Account> GetAccounts(SFBulkAPIStarter.BulkApiClient _apiClient)
        {
            //Database.getQueryLocator([select Id, ParentId, MSN06__c from Account where MSN06__c != null AND Anzahl_VTNR_in_der_Maklerakte_Number__c > 0]);
            DateTime start = DateTime.Now;
            JobRequest queryAccountJobRequest = buildDefaultCreateJobRequest(JobOperation.Query, "Account");
            Job queryJob = _apiClient.CreateJob(queryAccountJobRequest);

            String accountQuery = "select Id, ParentId, MSN06__c from Account where MSN12__c != null AND Anzahl_VTNR_in_der_Maklerakte_Number__c > 0";
            BatchRequest queryBatchRequest = buildCreateBatchRequest(queryJob.id, accountQuery);
            queryBatchRequest.BatchContentType = BatchContentType.JSON;

            Batch queryBatch = _apiClient.CreateBatch(queryBatchRequest);
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
            List<Account> accs = JsonConvert.DeserializeObject<List<Account>>(batchQueryResults);
            DateTime end = DateTime.Now;

            Console.WriteLine("Time for accounts: " + (start - end).TotalSeconds);
            return accs;
        }

        private List<Maklerverknuepfung__c> GetAccVtnrs(SFBulkAPIStarter.BulkApiClient _apiClient)
        {
            DateTime start = DateTime.Now;
            JobRequest queryAccountJobRequest = buildDefaultCreateJobRequest(JobOperation.Query, "Maklerverknuepfung__c");
            Job queryJob = _apiClient.CreateJob(queryAccountJobRequest);

            String accvtnrQuery = "select Id, CustomID__c, Maklerakte__c, Vermittler__c from Maklerverknuepfung__c";
            BatchRequest queryBatchRequest = buildCreateBatchRequest(queryJob.id, accvtnrQuery);
            queryBatchRequest.BatchContentType = BatchContentType.JSON;

            Batch queryBatch = _apiClient.CreateBatch(queryBatchRequest);
            queryJob = _apiClient.GetJob(queryJob.id);

            while (queryJob.IsDone == false)
            {
                Thread.Sleep(2000);
                queryJob = _apiClient.GetJob(queryJob.id);
            }

            string batchQueryResultsList = _apiClient.GetBatchResults(queryBatch.JobId, queryBatch.Id);
            Console.WriteLine(batchQueryResultsList);
            Console.WriteLine("____________________________________________________________________");
            List<string> resultIds = _apiClient.GetResultIds(batchQueryResultsList);


            string batchQueryResults = _apiClient.GetBatchResult(queryBatch.JobId, queryBatch.Id, resultIds[0]);
            _apiClient.CloseJob(queryJob.id);
            List<Maklerverknuepfung__c> accvtnrs = JsonConvert.DeserializeObject<List<Maklerverknuepfung__c>>(batchQueryResults);
            DateTime end = DateTime.Now;

            Console.WriteLine("Time for Maklerverknuepfung__c: " + (start - end).TotalSeconds);
            return accvtnrs;
        }

        public List<ZDF_Aggregation__c> GetVtnrAggrs(SFBulkAPIStarter.BulkApiClient _apiClient)
        {
            DateTime start = DateTime.Now;
            JobRequest queryAccountJobRequest = buildDefaultCreateJobRequest(JobOperation.Query, "ZDF_Aggregation__c");
            Job queryJob = _apiClient.CreateJob(queryAccountJobRequest);
            string s = queryJob.id;
            //String accvtnrQuery = "select Branche__c ,Angebote_Z_Offen_VJ__c,Angebote_Z_Offen_GJ__c,Angebote_Z_Ang_VJ__c,Angebote_Z_Ang_GJ__c,Angebote_Z_Abg_VJ__c,Angebote_Z_Abg_GJ__c,Angebote_JBTR_Offen_VJ__c,Angebote_JBTR_Offen_GJ__c,Angebote_JBTR_Ang_VJ__c,Angebote_JBTR_Ang_GJ__c,Angebote_JBTR_Abg_VJ__c,Angebote_JBTR_Abg_GJ__c,Angebote_Gesamtst_cke_VJ__c,Angebote_Gesamtst_cke_GJ__c,Angebote_Gesamtbetrag_VJ__c,Angebote_Gesamtbetrag_GJ__c,Angebote_Annahmequote_St_cke_VJ__c,Angebote_Annahmequote_St_cke_GJ__c,Angebote_Annahmequote_Beitrag_VJ__c,Angebote_Annahmequote_Beitrag_GJ__c,Produktion_Neustueck_YTD_VJ__c,Produktion_Neustueck_YTD_GJ__c,Produktion_Neustueck_DEC_VJ__c,Produktion_Neustueck_DEC_GJ2__c,Produktion_NMBTR_YTD_VJ__c,Produktion_NMBTR_YTD_GJ__c,Produktion_NMBTR_DEC_VJ__c,Produktion_NMBTR_DEC_GJ2__c,Storno_Stueck_YTD_GJ__c,Storno_Beitrag_YTD_VJ__c,Storno_Stueck_YTD_VJ__c,Storno_Beitrag_YTD_GJ__c,Storno_Beitrag_Dec_GJ2__c,Storno_Stueck_Dec_GJ2__c,Storno_Beitrag_Dec_VJ__c,Storno_Stueck_Dec_VJ__c,Verm_Storno_Stueck_YTD_GJ__c,Verm_Storno_Beitrag_YTD_VJ__c,Verm_Storno_Stueck_YTD_VJ__c,Verm_Storno_Beitrag_YTD_GJ__c,Verm_Storno_Beitrag_Dec_GJ2__c,Verm_Storno_Stueck_Dec_GJ2__c,Verm_Storno_Beitrag_Dec_VJ__c,Verm_Storno_Stueck_Dec_VJ__c,Bestand_BBTR_YTD_GJ__c,Bestand_BBTR_YTD_VJ__c,Bestand_BBTR_DEC_VJ__c,Bestand_BBTR_DEC_GJ2__c,Bestand_Bestandsstueck_YTD_GJ__c,Bestand_Bestandsstueck_YTD_VJ__c,Bestand_Bestandsstueck_DEC_VJ__c,Bestand_Bestandsstueck_DEC_GJ2__c,BAQ_SoW_Beitrag_Abweichung__c,BAQ_SoW_Beitrag_YTD_GJ__c,BAQ_SoW_Beitrag_YTD_VJ__c,Courtage_YTD_GJ__c,Courtage_YTD_VJ__c,Courtage_GJ1__c,Courtage_GJ2__c from ZDF_Aggregation__c where Qualifier__c = 'mrz2016'";
            String accvtnrQuery = "select id from ZDF_Aggregation__c";
            BatchRequest queryBatchRequest = buildCreateBatchRequest(s, accvtnrQuery);
            //queryBatchRequest.BatchContentType = BatchContentType.JSON;
            queryBatchRequest.BatchContentType = BatchContentType.CSV;

            Batch queryBatch = _apiClient.CreateBatch(queryBatchRequest);
            queryJob = _apiClient.GetJob(queryJob.id);

            while (queryJob.IsDone == false)
            {
                Thread.Sleep(2000);
                queryJob = _apiClient.GetJob(queryJob.id);
            }

            string batchQueryResultsList = _apiClient.GetBatchResults(queryBatch.JobId, queryBatch.Id);
            Console.WriteLine(batchQueryResultsList);
            Console.WriteLine("____________________________________________________________________");
            List<string> resultIds = _apiClient.GetResultIds(batchQueryResultsList);
            List<ZDF_Aggregation__c> accvtnrs = new List<ZDF_Aggregation__c>();
            foreach (string resultId in resultIds)
            {
                string batchQueryResults = _apiClient.GetBatchResult(queryBatch.JobId, queryBatch.Id, resultIds[0]);
                accvtnrs.AddRange(JsonConvert.DeserializeObject<List<ZDF_Aggregation__c>>(batchQueryResults));
            }
            
            _apiClient.CloseJob(queryJob.id);
           
            DateTime end = DateTime.Now;

            Console.WriteLine("Time for Maklerverknuepfung__c: " + (start - end).TotalSeconds);
            return accvtnrs;
        }

        private JobRequest buildDefaultCreateJobRequest(JobOperation operation, string objName)
        {
            JobRequest jobRequest = new JobRequest();
            //jobRequest.ContentType = JobContentType.JSON;
            jobRequest.ContentType = JobContentType.XML;
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
