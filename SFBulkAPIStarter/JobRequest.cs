﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SFBulkAPIStarter
{
    public class JobRequest
    {
        public JobOperation Operation { get; set; }

        internal String OperationString
        {
            get
            {
                switch (Operation)
                {
                    case JobOperation.Insert:
                        return "insert";
                    case JobOperation.Update:
                        return "update";
                    case JobOperation.HardDelete:
                        return "hardDelete";
                    case JobOperation.Delete:
                        return "delete";
                    case JobOperation.Query:
                        return "query";
                    default:
                        return "upsert";
                }
            }
        }

        public String Object { get; set; }
        public JobContentType ContentType { get; set; }
        public String ExternalIdFieldName { get; set; }
        internal String ContentTypeString
        {
            get
            {
                switch (ContentType)
                {
                    case JobContentType.CSV:
                        return "CSV";
                    case JobContentType.XML:
                        return "XML";
                    case JobContentType.JSON:
                        return "JSON";
                    default:
                        return "XML";
                }
            }
        }
    }
}
