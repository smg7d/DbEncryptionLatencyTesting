using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Identity.Client;

namespace LatencyTesting
{
    public class SchemaConfiguration
    {
    
        public string SchemaName { get; set; } = "";
        public string TableOneName {get; set;} = "";
        public string TableTwoName {get; set;} = "";
        public bool IsMultiTable {get; set;}
        public EncryptionConfiguration? EncryptionConfiguration { get; set; }
        
    }

    public class EncryptionConfiguration
    {
        public string CmkName {get; set;} = "";
        public string CekName {get; set;} = "";
        public bool EnclaveEnabled {get; set;}
        public string KeyVaultUrl {get; set;} = "";
        public EncryptionAlgoType? EncryptionAlgoType {get; set;}
    }

    public enum EncryptionAlgoType
    {
        Randomized,
        Deterministic
    }
}