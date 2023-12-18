using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient.AlwaysEncrypted.AzureKeyVaultProvider;
using Microsoft.Data.SqlClient;
using System.Security.Cryptography;
using Dapper;
using System.Data;

namespace LatencyTesting
{
    public class DatabaseRepository : IDatabaseRepository
    {
        private string? _dbConnectionString { get; set; }

        private SqlColumnEncryptionAzureKeyVaultProvider _kvProvider {get; set;}
        public DatabaseRepository(SqlColumnEncryptionAzureKeyVaultProvider kvProvider) 
        {
            this._kvProvider = kvProvider;
   
        }

        public DatabaseRepository(IConfiguration config, SqlColumnEncryptionAzureKeyVaultProvider keyVaultProvider)
        {
            _dbConnectionString = config.GetValue<string>("DbConnectionString");
            _kvProvider = keyVaultProvider;
        }

        public void InsertPatients(IEnumerable<Patient> patients, SchemaConfiguration schema)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();

            using var conn = new SqlConnection(_dbConnectionString);
            conn.Open();
            int patientsInserted = 0;
            
            foreach(var patient in patients)
            {
                dynamicParameters.Add($"@PatientId", patient.PatientId, DbType.Guid, ParameterDirection.Input);
                dynamicParameters.Add($"@Name", patient.Name, DbType.String, ParameterDirection.Input, 512);
                dynamicParameters.Add($"@Email", patient.Email, DbType.String, ParameterDirection.Input, 512);
                dynamicParameters.Add($"@PhoneNumber", patient.PhoneNumber, DbType.String, ParameterDirection.Input, 512);
                dynamicParameters.Add($"@DateOfBirth", patient.DateOfBirth, DbType.Date, ParameterDirection.Input);

                string sql = @$"insert into {schema.SchemaName}.{schema.TableOneName} (PatientId, Name, Email, PhoneNumber, DateOfBirth) 
                                values (@PatientId, @Name, @Email, @PhoneNumber, @DateOfBirth)";
                
                if(schema.IsMultiTable)
                {
                    sql += $"\ninsert into {schema.SchemaName}.{schema.TableTwoName} (PatientId, Name, Height, Weight, BloodPressure, ObservationDate) values(";
                    foreach(var observation in patient.Observations)
                    {
                        //to do -- this obviously won't work with more than one observation. fix it in the future
                        dynamicParameters.Add($"@ObservationPatientId", observation.PatientId, DbType.Guid, ParameterDirection.Input);
                        dynamicParameters.Add($"@ObservationName", observation.Name, DbType.String, ParameterDirection.Input, 512);
                        dynamicParameters.Add($"@Height", observation.Height, DbType.Int32, ParameterDirection.Input);
                        dynamicParameters.Add($"@Weight", observation.Weight, DbType.Int32, ParameterDirection.Input);
                        dynamicParameters.Add($"@BloodPressure", observation.BloodPressure, DbType.String, ParameterDirection.Input, 512);
                        dynamicParameters.Add($"@ObservationDate", observation.ObservationDate, DbType.Date, ParameterDirection.Input);
                        sql += "@ObservationPatientId, @ObservationName, @Height, @Weight, @BloodPressure, @ObservationDate)";
                    }
                }

                conn.Execute(sql, dynamicParameters);
                patientsInserted++;
            }
            Console.WriteLine($"patients created: {patientsInserted}");
        }

        public void FindPatientsByNameSqlSide(IEnumerable<Patient> patients, SchemaConfiguration schema)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();

            using var conn = new SqlConnection(_dbConnectionString);
            conn.Open();
            int patientsFound = 0;

            foreach(var patient in patients)
            {
                string sql = $"select * from {schema.SchemaName}.{schema.TableOneName} t1 where Name = @Name";
                dynamicParameters.Add($"@Name", patient.Name, DbType.String, ParameterDirection.Output, 512);

                if(schema.IsMultiTable)
                {
                    sql = @$"select * from {schema.SchemaName}.{schema.TableOneName} t1 
                            join {schema.SchemaName}.{schema.TableTwoName} t2 on t1.Name = t2.Name
                            where t1.Name = @Name";
                }

                var patientReturn = conn.Query<Patient>(sql, dynamicParameters);
                if(patientReturn != null)
                {
                    patientsFound++;
                }
            }
            Console.WriteLine($"patients found sql side: {patientsFound:N0}");
        }

        public void FindPatientsByNameClientSide(IEnumerable<Patient> patients, SchemaConfiguration schema)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();
            IEnumerable<Patient> dbPatients = GetAllPatientsFromDB(schema);
            var patientList = dbPatients.ToList(); //Enumeration for access to Exists method

            int patientsFound = 0;
            foreach(var patient in patients)
            {
                if(patientList.Exists(c => c.Name == patient.Name))
                {
                    patientsFound++;
                }
            }
            if(patientsFound < patients.Count()) throw new Exception("-------------some patients not found. test failed");
            Console.WriteLine($"patients found client side: {patientsFound:N0}");
        }

        public void FindPatientsWhereDobSqlSide(DateOnly date, SchemaConfiguration schema)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();

            using var conn = new SqlConnection(_dbConnectionString);
            conn.Open();
            int patientsFound = 0;

            string sql = $"select top 100 * from {schema.SchemaName}.{schema.TableOneName} where DateOfBirth > @Dob";
            dynamicParameters.Add($"@Dob", date, DbType.Date, ParameterDirection.Input);

            IEnumerable<Patient> patientReturn = conn.Query<Patient>(sql, dynamicParameters);
            if(patientReturn != null)
            {
                patientsFound = patientReturn.Count();
            }
    
            Console.WriteLine($"patients found for DOB condition sql side: {patientsFound}");
        }

        public void FindPatientsWhereDobClientSide(DateOnly date, SchemaConfiguration schema)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();
            IEnumerable<Patient> dbPatients = GetAllPatientsFromDB(schema);

            var patientsFound = dbPatients.Where(patient => patient.DateOfBirth > date).Take(100);
            
            Console.WriteLine($"patients found for DOB condition client side: {patientsFound.Count():N0}");
        }

        public void UpdatePatientPhoneSqlSide(IEnumerable<Patient> patients, SchemaConfiguration schema, string newNumber)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();

            using var conn = new SqlConnection(_dbConnectionString);
            conn.Open();
            int patientsUpdated = 0;

            foreach(var patient in patients)
            {
                string sql = $"update {schema.SchemaName}.{schema.TableOneName} set PhoneNumber = @Phone where Name = @Name";
                dynamicParameters.Add($"@Name", patient.Name, DbType.String, ParameterDirection.Input, 512);
                dynamicParameters.Add($"@Phone", newNumber, DbType.String, ParameterDirection.Input, 512);

                patientsUpdated += conn.Execute(sql, dynamicParameters);
            }
            Console.WriteLine($"patients updated sql side: {patientsUpdated}");
        }

        public void UpdatePatientPhoneClientSide(IEnumerable<Patient> patients, SchemaConfiguration schema, string newNumber)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();
            IEnumerable<Patient> dbPatients = GetAllPatientsFromDB(schema);
            var patientList = dbPatients.ToList(); //Enumeration for access to Find extension method

            using var conn = new SqlConnection(_dbConnectionString);
            conn.Open();
            int patientsUpdated = 0;

            foreach(var patient in patients)
            {
                var patientToUpdate = patientList.Find(c => c.Name == patient.Name);
                string sql = $"update {schema.SchemaName}.{schema.TableOneName} set PhoneNumber = @Phone where Id = @Id";
                dynamicParameters.Add($"@Id", patientToUpdate?.Id, DbType.String, ParameterDirection.Input, 512);
                dynamicParameters.Add($"@Phone", newNumber, DbType.String, ParameterDirection.Input, 512);

                patientsUpdated += conn.Execute(sql, dynamicParameters);
            }

            Console.WriteLine($"patients updated client side: {patientsUpdated}");
  
        }
        

        public void DeletePatientsByNameSqlSide(IEnumerable<Patient> patients, SchemaConfiguration schema)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();

            using var conn = new SqlConnection(_dbConnectionString);
            conn.Open();
            int patientsDeleted = 0;

            foreach(var patient in patients)
            {
                string sql = "";
                if(schema.IsMultiTable)
                {
                    sql = $"delete from {schema.SchemaName}.{schema.TableTwoName} where Name = @Name\n";
                    foreach(var observation in patient.Observations)
                    {
                        dynamicParameters.Add($"@ObservationName", observation.Name, DbType.String, ParameterDirection.Input, 512);
                    }
                }
                sql += $"delete from {schema.SchemaName}.{schema.TableOneName} where Name = @Name";
                dynamicParameters.Add($"@Name", patient.Name, DbType.String, ParameterDirection.Input, 512);

                conn.Execute(sql, dynamicParameters);
                patientsDeleted++;
            }
            Console.WriteLine($"patients deleted sql side: {patientsDeleted}");
        }

        public void DeletePatientsByNameClientSide(IEnumerable<Patient> patients, SchemaConfiguration schema)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();

            IEnumerable<Patient> dbPatients = GetAllPatientsFromDB(schema);

            IEnumerable<Guid> patientGuidsToFind = patients.Select(x => x.PatientId);
            IEnumerable<Patient> patientsToDelete = dbPatients.Where(x => patientGuidsToFind.Contains(x.PatientId));

            using var conn = new SqlConnection(_dbConnectionString);
            conn.Open();
            int patientsDeleted = 0;

            foreach(var patientToDelete in patientsToDelete)
            {
                string sql = "";
                if(schema.IsMultiTable)
                {
                    sql = $"delete from {schema.SchemaName}.{schema.TableTwoName} where Id = @ObservationId\n";
                    foreach(var observation in patientToDelete.Observations)
                    {
                        dynamicParameters.Add($"@ObservationId", observation.Id, DbType.String, ParameterDirection.Input, 512);
                    }
                }
                sql += $"delete from {schema.SchemaName}.{schema.TableOneName} where Id = @Id";
                dynamicParameters.Add($"@Id", patientToDelete.Id, DbType.String, ParameterDirection.Input, 512);

                conn.Execute(sql, dynamicParameters);
                patientsDeleted++;
            }
            Console.WriteLine($"patients deleted client side: {patientsDeleted}");
        }

        
        public IEnumerable<Patient> GetAllPatientsFromDB(SchemaConfiguration schema)
        {
            List<Patient> patientList = new();

            using var conn = new SqlConnection(_dbConnectionString);
            conn.Open();

            int returnCount;
            const int limit = 500;
            int index = 0;
            string sql;
            do
            {
                sql = @$"select top {limit} * from {schema.SchemaName}.{schema.TableOneName} t1 where Id >= {index}";
                var results = conn.Query<Patient>(sql);
                returnCount = results.Count();
                patientList.AddRange(results);
                index += limit;
            }
            while(returnCount == limit);

            if(schema.IsMultiTable)
            {
                var observationList = new List<Observation>();
                index = 0;
                do
                {
                    sql = $"select top {limit} * from {schema.SchemaName}.{schema.TableTwoName} t2 where Id >= {index}";
                    var results = conn.Query<Observation>(sql);
                    returnCount = results.Count();
                    observationList.AddRange(results);
                    index += limit;
                }
                while(returnCount == limit);

                foreach(var patient in patientList)
                {
                    var observationsForPatient = observationList.Where(x => x.Name == patient.Name);
                    patient.Observations.AddRange(observationsForPatient);
                }
            }

            return patientList;
        }

        public void BulkInsertPatients(IEnumerable<Patient> patients, SchemaConfiguration schema)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();

            using var conn = new SqlConnection(_dbConnectionString);
            conn.Open();
            
            int i = 0;
            int paramCounter = 0;
            string sql = $"insert into {schema.SchemaName}.{schema.TableOneName} (PatientId, Name, Email, PhoneNumber, DateOfBirth) values ";
            foreach(var patient in patients)
            {
                
                dynamicParameters.Add($"@PatientId_{i}", patient.PatientId, DbType.Guid, ParameterDirection.Input);
                dynamicParameters.Add($"@Name_{i}", patient.Name, DbType.String, ParameterDirection.Input, 512);
                dynamicParameters.Add($"@Email_{i}", patient.Email, DbType.String, ParameterDirection.Input, 512);
                dynamicParameters.Add($"@PhoneNumber_{i}", patient.PhoneNumber, DbType.String, ParameterDirection.Input, 512);
                dynamicParameters.Add($"@DateOfBirth_{i}", patient.DateOfBirth, DbType.Date, ParameterDirection.Input);

                sql += $"(@PatientId_{i}, @Name_{i}, @Email_{i}, @PhoneNumber_{i}, @DateOfBirth_{i}),";
                
                paramCounter += 5;
                i++;

                if(paramCounter >= 2000 || sql.Length >= 64000) //sql server command constraints
                {
                    sql = sql.Remove(sql.Length - 1, 1);
                    conn.Execute(sql, dynamicParameters);
                    sql = $"insert into {schema.SchemaName}.{schema.TableOneName} (PatientId, Name, Email, PhoneNumber, DateOfBirth) values ";
                    i = 0;
                    paramCounter = 0;
                }
            }

            if(i > 0)
            {
                sql = sql.Remove(sql.Length - 1, 1);
                conn.Execute(sql, dynamicParameters);
            }
            

            if(schema.IsMultiTable)
            {
                var observations = patients.SelectMany(x => x.Observations);

                dynamicParameters = new DynamicParameters();
                
                sql = $"insert into {schema.SchemaName}.{schema.TableTwoName} (PatientId, Name, Height, Weight, BloodPressure, ObservationDate) values ";
                i = 0;
                paramCounter = 0;
                foreach(var observation in observations)
                {
                    //to do -- this obviously won't work with more than one observation. fix it in the future
                    dynamicParameters.Add($"@ObservationPatientId_{i}", observation.PatientId, DbType.Guid, ParameterDirection.Input);
                    dynamicParameters.Add($"@ObservationName_{i}", observation.Name, DbType.String, ParameterDirection.Input, 512);
                    dynamicParameters.Add($"@Height_{i}", observation.Height, DbType.Int32, ParameterDirection.Input);
                    dynamicParameters.Add($"@Weight_{i}", observation.Weight, DbType.Int32, ParameterDirection.Input);
                    dynamicParameters.Add($"@BloodPressure_{i}", observation.BloodPressure, DbType.String, ParameterDirection.Input, 512);
                    dynamicParameters.Add($"@ObservationDate_{i}", observation.ObservationDate, DbType.Date, ParameterDirection.Input);
                    sql += $"(@ObservationPatientId_{i}, @ObservationName_{i}, @Height_{i}, @Weight_{i}, @BloodPressure_{i}, @ObservationDate_{i}),";

                    paramCounter += 6;
                    i++;

                    if(paramCounter >= 2000 || sql.Length >= 64000) //sql server command constraints
                    {
                        sql = sql.Remove(sql.Length - 1, 1);
                        conn.Execute(sql, dynamicParameters);
                        sql = $"insert into {schema.SchemaName}.{schema.TableTwoName} (PatientId, Name, Height, Weight, BloodPressure, ObservationDate) values ";
                        i = 0;
                        paramCounter = 0;
                    }
                }

                if(i > 0)
                {
                    sql = sql.Remove(sql.Length - 1, 1);
                    conn.ExecuteAsync(sql, dynamicParameters);
                }
                
            }        
            Console.WriteLine($"{schema.SchemaName} populated with {patients.Count():N0} patients");
        }

        public void ProvisionKeys(EncryptionConfiguration encryptConfig)
        {
            //helpful documentation https://learn.microsoft.com/en-us/sql/connect/ado-net/sql/azure-key-vault-example?view=sql-server-ver16
            
            //create the master key
            string createMasterKeySql = 
                $@"CREATE COLUMN MASTER KEY [{encryptConfig.CmkName}]
                WITH (
                    KEY_STORE_PROVIDER_NAME = N'{SqlColumnEncryptionAzureKeyVaultProvider.ProviderName}',
                    KEY_PATH = N'{encryptConfig.KeyVaultUrl}'";
            
            string endSql = ")";
            if(encryptConfig.EnclaveEnabled)
            {
                var mkSignature = _kvProvider.SignColumnMasterKeyMetadata(encryptConfig.KeyVaultUrl, true);
                var enclaveSignature = string.Concat("0x", BitConverter.ToString(mkSignature).Replace("-", string.Empty));

                endSql = $", ENCLAVE_COMPUTATIONS (SIGNATURE = {enclaveSignature}))";
            }

            createMasterKeySql += endSql;
            
            using var conn = new SqlConnection(_dbConnectionString);
            conn.Open();

            var cmkCommand = conn.CreateCommand();
            cmkCommand.CommandText = createMasterKeySql;
            cmkCommand.ExecuteNonQuery();
            
            //create the unencrypted cek
            var cekValue = new byte[32];
            var rng = RandomNumberGenerator.Create();
            rng.GetBytes(cekValue);

            //encrypt it
            var encryptedCEK = _kvProvider.EncryptColumnEncryptionKey(encryptConfig.KeyVaultUrl, "RSA_OAEP", cekValue);
            var encryptedCEKString = string.Concat("0x", BitConverter.ToString(encryptedCEK).Replace("-", string.Empty));

            //create the column key
            var createColumnEncryptionKeySql = 
            $@"CREATE COLUMN ENCRYPTION KEY [{encryptConfig.CekName}]
            WITH VALUES (
                COLUMN_MASTER_KEY = [{encryptConfig.CmkName}],
                ALGORITHM = N'RSA_OAEP',
                ENCRYPTED_VALUE = {encryptedCEKString}
            )";

            using var cekCommand = conn.CreateCommand();
            cekCommand.CommandText = createColumnEncryptionKeySql;
            cekCommand.ExecuteNonQuery();
        }


        public void CreateTables(SchemaConfiguration schema)
        {
            using var conn = new SqlConnection(_dbConnectionString);
            conn.Open();

            using var command = conn.CreateCommand();
            command.CommandText = $"if not exists (select * from sys.schemas where name = '{schema.SchemaName}') begin exec('create schema {schema.SchemaName}') end";
            command.ExecuteNonQuery();

            if(schema.EncryptionConfiguration is null)
            {
                //unencrypted tables
                command.CommandText = @$"create table {schema.SchemaName}.{schema.TableOneName}
                (
                    Id BIGINT IDENTITY(1,1) PRIMARY KEY, --because encrypted columns can't be a clustered index, and auto-inc makes inserts faster for clustered indexes
                    PatientId UNIQUEIDENTIFIER DEFAULT NEWID() NOT NULL UNIQUE,
                    Name NVARCHAR(512) NOT NULL,
                    Email NVARCHAR(512),
                    PhoneNumber NVARCHAR(512),
                    DateOfBirth DATE NOT NULL
                )";
                command.ExecuteNonQuery();

                command.CommandText = @$"create table {schema.SchemaName}.{schema.TableTwoName}
                (
                    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    PatientId UNIQUEIDENTIFIER FOREIGN KEY REFERENCES {schema.SchemaName}.{schema.TableOneName}(PatientId) NOT NULL,
                    Name NVARCHAR(512) NOT NULL,
                    Height INT NOT NULL,
                    Weight INT NOT NULL,
                    BloodPressure NVARCHAR(512) NOT NULL,
                    ObservationDate DATE NOT NULL
                )";
                command.ExecuteNonQuery();
                return;
            }
            
            //encrypted tables
            command.CommandText = @$"create table {schema.SchemaName}.{schema.TableOneName}
                (
                    Id BIGINT IDENTITY(1,1) PRIMARY KEY, --because encrypted columns can't be a clustered index, and auto-inc makes inserts faster for clustered indexes
                    PatientId UNIQUEIDENTIFIER DEFAULT NEWID() NOT NULL UNIQUE,
                    Name NVARCHAR(512) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{schema.EncryptionConfiguration.CekName}], ENCRYPTION_TYPE = {schema.EncryptionConfiguration.EncryptionAlgoType}, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NOT NULL,
                    Email NVARCHAR(512) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{schema.EncryptionConfiguration.CekName}], ENCRYPTION_TYPE = {schema.EncryptionConfiguration.EncryptionAlgoType}, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256'),
                    PhoneNumber NVARCHAR(512) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{schema.EncryptionConfiguration.CekName}], ENCRYPTION_TYPE = {schema.EncryptionConfiguration.EncryptionAlgoType}, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256'),
                    DateOfBirth DATE ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{schema.EncryptionConfiguration.CekName}], ENCRYPTION_TYPE = {schema.EncryptionConfiguration.EncryptionAlgoType}, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NOT NULL
                )";
            command.ExecuteNonQuery();

            command.CommandText = @$"create table {schema.SchemaName}.{schema.TableTwoName}
                (
                    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    PatientId UNIQUEIDENTIFIER FOREIGN KEY REFERENCES {schema.SchemaName}.{schema.TableOneName}(PatientId) NOT NULL,
                    Name NVARCHAR(512) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{schema.EncryptionConfiguration.CekName}], ENCRYPTION_TYPE = {schema.EncryptionConfiguration.EncryptionAlgoType}, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NOT NULL,
                    Height INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{schema.EncryptionConfiguration.CekName}], ENCRYPTION_TYPE = {schema.EncryptionConfiguration.EncryptionAlgoType}, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NOT NULL,
                    Weight INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{schema.EncryptionConfiguration.CekName}], ENCRYPTION_TYPE = {schema.EncryptionConfiguration.EncryptionAlgoType}, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NOT NULL,
                    BloodPressure NVARCHAR(512) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{schema.EncryptionConfiguration.CekName}], ENCRYPTION_TYPE = {schema.EncryptionConfiguration.EncryptionAlgoType}, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256'),
                    ObservationDate DATE ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{schema.EncryptionConfiguration.CekName}], ENCRYPTION_TYPE = {schema.EncryptionConfiguration.EncryptionAlgoType}, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NOT NULL
                )";
            command.ExecuteNonQuery();

            command.CommandText = $"ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE;";
            command.ExecuteNonQuery();

        }

        public void TearDownSchema(SchemaConfiguration schema)
        {
    
            using var conn = new SqlConnection(_dbConnectionString);
            conn.Open();

            using var cmd = conn.CreateCommand();
            
            cmd.CommandText = $"if (exists (select * from information_schema.tables where table_schema = '{schema.SchemaName}' and table_name = '{schema.TableTwoName}')) begin drop table {schema.SchemaName}.{schema.TableTwoName} end";
            cmd.ExecuteNonQuery();
            cmd.CommandText = $"if (exists (select * from information_schema.tables where table_schema = '{schema.SchemaName}' and table_name = '{schema.TableOneName}')) begin drop table {schema.SchemaName}.{schema.TableOneName} end";
            cmd.ExecuteNonQuery();

            if(!string.IsNullOrEmpty(schema.EncryptionConfiguration?.CekName))
            {
                
                cmd.CommandText = $"if exists (select * from sys.column_encryption_keys where name = '{schema.EncryptionConfiguration.CekName}') begin drop column encryption key {schema.EncryptionConfiguration.CekName} end";
                cmd.ExecuteNonQuery();
            }

            if(!string.IsNullOrEmpty(schema.EncryptionConfiguration?.CmkName))
            {
                cmd.CommandText = $"if exists (select * from sys.column_master_keys where name = '{schema.EncryptionConfiguration.CmkName}') begin drop column master key {schema.EncryptionConfiguration.CmkName} end";
                cmd.ExecuteNonQuery();
            }
            
            cmd.CommandText = $"drop schema if exists {schema.SchemaName}";
            cmd.ExecuteNonQuery();

            cmd.CommandText = $"ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE;";
            cmd.ExecuteNonQuery();
            
        }

        public void ClearTables(SchemaConfiguration schema)
        {

            using var conn = new SqlConnection(_dbConnectionString);
            conn.Open();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"if (exists (select * from information_schema.tables where table_schema = '{schema.SchemaName}' and table_name = '{schema.TableTwoName}')) begin delete from {schema.SchemaName}.{schema.TableTwoName} end";
            cmd.ExecuteNonQuery();
            cmd.CommandText = $"if (exists (select * from information_schema.tables where table_schema = '{schema.SchemaName}' and table_name = '{schema.TableOneName}')) begin delete from {schema.SchemaName}.{schema.TableOneName} end";
            cmd.ExecuteNonQuery();
        }
    }
}