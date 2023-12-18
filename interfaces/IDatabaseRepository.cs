using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LatencyTesting
{
    public interface IDatabaseRepository
    {
        public void InsertPatients(IEnumerable<Patient> patients, SchemaConfiguration schema);
        public void BulkInsertPatients(IEnumerable<Patient> patients, SchemaConfiguration schema);
        public void DeletePatientsByNameSqlSide(IEnumerable<Patient> patients, SchemaConfiguration schema);
        public void DeletePatientsByNameClientSide(IEnumerable<Patient> patients, SchemaConfiguration schema);
        public void FindPatientsByNameSqlSide(IEnumerable<Patient> patients, SchemaConfiguration schema);
        public void FindPatientsByNameClientSide(IEnumerable<Patient> patients, SchemaConfiguration schema);
        public void FindPatientsWhereDobSqlSide(DateOnly date, SchemaConfiguration schema);
        public void FindPatientsWhereDobClientSide(DateOnly date, SchemaConfiguration schema);
        public void UpdatePatientPhoneClientSide(IEnumerable<Patient> patients, SchemaConfiguration schema, string newNumber);
        public void UpdatePatientPhoneSqlSide(IEnumerable<Patient> patients, SchemaConfiguration schema, string newNumber);
        public void TearDownSchema(SchemaConfiguration schema);
        public void ProvisionKeys(EncryptionConfiguration encryptionConfig);
        public void CreateTables(SchemaConfiguration schema);
        public void ClearTables(SchemaConfiguration schema);
        
    }
}