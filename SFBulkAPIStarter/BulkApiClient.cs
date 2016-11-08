﻿using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace SFBulkAPIStarter
{
    /// <summary>
    /// Wrapper class to Salesforce's Bulk API.
    /// </summary>
    /// <seealso cref="https://www.salesforce.com/us/developer/docs/api_asynch/"/>

    public class BulkApiClient
    {
        private String _UserName { get; set; }
        private String _Password { get; set; }
        private String _LoginURL { get; set; }

        SFEnterprise.SforceService _sfService = null;
        SFEnterprise.LoginResult _loginResult = null;

        public BulkApiClient(String username, String password, String loginUrl)
        {
            _UserName = username;
            _Password = password;
            _LoginURL = loginUrl;

            Login();
        }

        public Job CreateJob(JobRequest createJobRequest)
        {
            String jobRequestXML =
            @"<?xml version=""1.0"" encoding=""UTF-8""?>
             <jobInfo xmlns=""http://www.force.com/2009/06/asyncapi/dataload"">
               <operation>{0}</operation>
               <object>{1}</object>
               {3}
               <contentType>{2}</contentType>
             </jobInfo>";

            String externalField = String.Empty;

            if (String.IsNullOrWhiteSpace(createJobRequest.ExternalIdFieldName) == false)
            {
                externalField = "<externalIdFieldName>" + createJobRequest.ExternalIdFieldName + "</externalIdFieldName>";
            }

            jobRequestXML = String.Format(jobRequestXML,
                                          createJobRequest.OperationString,
                                          createJobRequest.Object,
                                          //createJobRequest.ContentTypeString,
                                          "CSV",
                                          externalField);

            String createJobUrl = "https://" + _sfService.Pod + ".salesforce.com/services/async/36.0/job";

            String resultXML = invokeRestAPI(createJobUrl, jobRequestXML); 

            return Job.CreateFromJson(resultXML);
        }

        public Job CloseJob(String jobId)
        {
            String closeJobUrl = buildSpecificJobUrl(jobId);
            String closeRequestXML = 
            @"<?xml version=""1.0"" encoding=""UTF-8""?>" + Environment.NewLine +
            @"<jobInfo xmlns=""http://www.force.com/2009/06/asyncapi/dataload"">" + Environment.NewLine +
             "<state>Closed</state>" + Environment.NewLine +
             "</jobInfo>";

            String resultXML = invokeRestAPI(closeJobUrl, closeRequestXML);

            return Job.CreateFromJson(resultXML);
        }

        public Job GetJob(String jobId)
        {
            String getJobUrl = buildSpecificJobUrl(jobId);

            String resultXML = invokeRestAPI(getJobUrl);

            return Job.CreateFromJson(resultXML);
        }

        public Batch CreateBatch(BatchRequest createBatchRequest){
            String requestUrl = "https://" + _sfService.Pod + ".salesforce.com/services/async/36.0/job/" + createBatchRequest.JobId + "/batch";

            String requestXML = createBatchRequest.BatchContents;

            String contentType = String.Empty;

            if (createBatchRequest.BatchContentType.HasValue)
            {
                contentType = createBatchRequest.BatchContentHeader;
            }

            String resultXML = invokeRestAPI(requestUrl, requestXML, "Post", contentType);

            return Batch.CreateFromJson(resultXML);
        }

        public Batch GetBatch(string jobId, string batchId)
        {
            String requestUrl = "https://" + _sfService.Pod + ".salesforce.com/services/async/36.0/job/" + jobId + "/batch/" + batchId;

            String resultXML = invokeRestAPI(requestUrl);

            return Batch.CreateFromJson(resultXML);
        }

        public List<Batch> GetBatches(String jobId){
            String requestUrl = "https://" + _sfService.Pod + ".salesforce.com/services/async/36.0/job/" + jobId + "/batch/";

            String resultXML = invokeRestAPI(requestUrl);

            return Batch.CreateBatches(resultXML);
        }

        public String GetBatchRequest(String jobId, String batchId)
        {
            String requestUrl = "https://" + _sfService.Pod + ".salesforce.com/services/async/36.0/job/" + jobId + "/batch/" + batchId + "/request";

            String resultXML = invokeRestAPI(requestUrl);

            return resultXML;
        }

        public String GetBatchResults(String jobId, String batchId)
        {
            String requestUrl = "https://" + _sfService.Pod + ".salesforce.com/services/async/36.0/job/" + jobId + "/batch/" + batchId + "/result";

            String resultXML = invokeRestAPI(requestUrl);

            return resultXML;
        }

        public List<String> GetResultIds(String queryBatchResultListXML)
        {
            //<result-list xmlns="http://www.force.com/2009/06/asyncapi/dataload"><result>752x000000000F1</result></result-list>

            List<String> resultIds = JsonConvert.DeserializeObject<List<String>>(queryBatchResultListXML);
            return resultIds;
        }

        public String GetBatchResult(String jobId, String batchId, String resultId)
        {
            String requestUrl = "https://" + _sfService.Pod + ".salesforce.com/services/async/36.0/job/" + jobId + "/batch/" + batchId + "/result/" + resultId;

            String resultXML = invokeRestAPI(requestUrl);

            return resultXML;
        }

        private String buildSpecificJobUrl(String jobId)
        {
            return "https://" + _sfService.Pod + ".salesforce.com/services/async/36.0/job/" + jobId;
        }

        private void Login()
        {
            _sfService = new SFEnterprise.SforceService();

            _sfService.Url = _LoginURL;
            _loginResult = _sfService.login(_UserName, _Password);
            _sfService.Url = _loginResult.serverUrl;
        }

        private String invokeRestAPI(String endpointURL)
        {
            WebClient wc = buildWebClient();

            return wc.DownloadString(endpointURL);
        }

        private String invokeRestAPI(String endpointURL, String postData)
        {
            return invokeRestAPI(endpointURL, postData, "Post", String.Empty);
        }

        private String invokeRestAPI(String endpointURL, String postData, String httpVerb, String contentType)
        {
            WebClient wc = buildWebClient();
            wc.Headers.Add("Sforce-Enable-PKChunking: chunkSize=10000;");
            if (String.IsNullOrWhiteSpace(contentType) == false)
            {
                wc.Headers.Add("Content-Type: " + contentType);
                
            }

            try
            {
                return wc.UploadString(endpointURL, httpVerb, postData);
            }
            catch (WebException webEx)
            {
                String error = String.Empty;

                if (webEx.Response != null){
                    using (var errorResponse = (HttpWebResponse)webEx.Response)
                    {
                        using (var reader = new StreamReader(errorResponse.GetResponseStream())) {
                            error = reader.ReadToEnd();
                        }
                    }
                }

                throw;
            }
        }

        private WebClient buildWebClient()
        {
            WebClient wc = new WebClient();
            wc.Encoding = Encoding.UTF8;
            wc.Headers.Add("X-SFDC-Session: " + _loginResult.sessionId);

            return wc;
        }
    }
}
