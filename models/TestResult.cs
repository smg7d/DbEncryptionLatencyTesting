using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Identity.Client.Advanced;

namespace LatencyTesting
{
    public class TestResult
    {
        public string TestName { get; set; } = "";
        public string SchemaName { get; set; } = "";
        public string? EncryptionAlgo {get; set;}
        public long TestSize { get; set; }
        public long DatabaseSize { get; set; }
        public double Time {get; set;}
    }
}