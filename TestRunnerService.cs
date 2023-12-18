using System.Diagnostics;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Configuration;

namespace LatencyTesting
{
    public class TestRunnerService : ITestRunnerService
    {
        private int _dbSize;
        private string _encryptionRegime; //to help compare not encrypted test runs with encrypted test runs by encryption type
        private readonly IDatabaseRepository _repo;
        private SchemaConfiguration _encryptedWithEnclavesConfiguration {get; set;}
        private SchemaConfiguration _encryptedConfiguration {get; set;}
        private SchemaConfiguration _plainTextConfiguration {get; set;}
        private List<SchemaConfiguration> _schemas {get; set;}

        public TestRunnerService(IConfiguration config, IDatabaseRepository databaseRepository)
        {
            _repo = databaseRepository;

            _encryptedWithEnclavesConfiguration = new SchemaConfiguration()
            {
                SchemaName = "EncryptedWithEnclaves",
                TableOneName = "Patients",
                TableTwoName = "Observations",
                EncryptionConfiguration = new EncryptionConfiguration()
                {
                    KeyVaultUrl = config.GetValue<string>("KeyVaultUrl") ?? throw new Exception("Null config value"),
                    CmkName = "cmkEnclave",
                    CekName = "cekEnclave",
                    EnclaveEnabled = true,
                    EncryptionAlgoType = EncryptionAlgoType.Deterministic
                },
                IsMultiTable = true
            };

            _encryptedConfiguration = new SchemaConfiguration()
            {
                SchemaName = "Encrypted",
                TableOneName = "Patients",
                TableTwoName = "Observations",
                EncryptionConfiguration = new EncryptionConfiguration()
                {
                    KeyVaultUrl = config.GetValue<string>("KeyVaultUrl") ?? throw new Exception("Null config value"),
                    CmkName = "cmk",
                    CekName = "cek",
                    EnclaveEnabled = false,
                    EncryptionAlgoType = EncryptionAlgoType.Deterministic
                },
                IsMultiTable = true
            };

            _plainTextConfiguration = new SchemaConfiguration()
            {
                SchemaName = "NotEncrypted",
                TableOneName = "Patients",
                TableTwoName = "Observations",
                IsMultiTable = true
            };

            _schemas = new List<SchemaConfiguration>()
            {
                _encryptedWithEnclavesConfiguration,
                _encryptedConfiguration,
                _plainTextConfiguration
            };

            _encryptionRegime = "not set";
        }

        public void InvokeTests()
        {
            
            InitializeAllSchemas(EncryptionAlgoType.Deterministic);
            RunTests(4000, 20, "fullResults.csv");
            
            InitializeAllSchemas(EncryptionAlgoType.Randomized);
            RunTests(2000, 25, "fullResults.csv");

        }

        public void RunTests(int step, int iterations, string outputFile)
        {
            Random r = new();

            for(int i=0; i < iterations; i++)
            {
                
                //"background" patients
                _dbSize = r.Next((i)*step, (i+1)*step);
                var dbPatients = CreatePatients(_dbSize);
                InitializeAllData(dbPatients);
                
                //"target" patients
                int numOfTargetPatients = 100;
                var targetPatients = CreatePatients(numOfTargetPatients);

                RunCreateTest(targetPatients, outputFile);
                RunFindTest(targetPatients, outputFile);
                RunUpdateTest(targetPatients, outputFile);
                RunDeleteTest(targetPatients, outputFile);
                
                if (_dbSize < 50000) //Takes too long after this size
                {
                    RunFindWhereDOBTest(outputFile);
                }
                
            }
        }


        private void RunCreateTest(IEnumerable<Patient> patients, string outputFileName)
        {
            var testResults = new List<TestResult>();
            
            foreach(var schema in _schemas)
            {
                Console.WriteLine($"--->Running Create Test on {ConsoleHelper(schema)}");
                var time = TimeMethodCall(() => _repo.InsertPatients(patients, schema));
                var testResult = new TestResult()
                {
                    TestName = "RunCreates",
                    SchemaName = schema.SchemaName,
                    EncryptionAlgo = schema.EncryptionConfiguration?.EncryptionAlgoType.ToString() ?? _encryptionRegime,
                    TestSize = patients.Count(),
                    DatabaseSize = _dbSize,
                    Time = time.TotalSeconds
                };
                testResults.Add(testResult);
            }

            LogResults(testResults, outputFileName);
        }

        private void RunFindTest(IEnumerable<Patient> patients, string outputFileName)
        {
            var testResults = new List<TestResult>();

            foreach(var schema in _schemas)
            {
                Console.WriteLine($"--->Running Find Test on {ConsoleHelper(schema)}");
                //patients = patients.OrderBy(x => Guid.NewGuid());  //yes I'm using Guids for randomness. sue me.
                TimeSpan time;
                if(schema.EncryptionConfiguration?.EnclaveEnabled == false && 
                    schema.EncryptionConfiguration?.EncryptionAlgoType == EncryptionAlgoType.Randomized)
                {
                    time = TimeMethodCall(() => _repo.FindPatientsByNameClientSide(patients, schema));
                }
                else
                {
                    time = TimeMethodCall(() => _repo.FindPatientsByNameSqlSide(patients, schema));
                }

                var testResult = new TestResult()
                {
                    TestName = "RunFinds",
                    SchemaName = schema.SchemaName,
                    EncryptionAlgo = schema.EncryptionConfiguration?.EncryptionAlgoType.ToString() ?? _encryptionRegime,
                    TestSize = patients.Count(),
                    DatabaseSize = _dbSize,
                    Time = time.TotalSeconds
                };
                testResults.Add(testResult);

            }
            LogResults(testResults, outputFileName);
        }


        private void RunUpdateTest(IEnumerable<Patient> patients, string outputFileName)
        {
            var testResults = new List<TestResult>();

            //patients = patients.OrderBy(x => Guid.NewGuid()); //taking existing to delete
            foreach(var schema in _schemas)
            {
                Console.WriteLine($"--->Running Update Test on {ConsoleHelper(schema)}");
                TimeSpan time;
                if(schema.EncryptionConfiguration?.EnclaveEnabled == false && 
                    schema.EncryptionConfiguration?.EncryptionAlgoType == EncryptionAlgoType.Randomized)
                {
                    time = TimeMethodCall(() => _repo.UpdatePatientPhoneClientSide(patients, schema, Faker.Phone.Number()));
                }
                else
                {
                    time = TimeMethodCall(() => _repo.UpdatePatientPhoneSqlSide(patients, schema, Faker.Phone.Number()));
                }

                var testResult = new TestResult()
                {
                    TestName = "RunUpdates",
                    SchemaName = schema.SchemaName,
                    EncryptionAlgo = schema.EncryptionConfiguration?.EncryptionAlgoType.ToString() ?? _encryptionRegime,
                    TestSize = patients.Count(),
                    DatabaseSize = _dbSize,
                    Time = time.TotalSeconds
                };
                testResults.Add(testResult);
            }

            LogResults(testResults, outputFileName);
        }

        private void RunDeleteTest(IEnumerable<Patient> patients, string outputFileName)
        {
            var testResults = new List<TestResult>();

            foreach(var schema in _schemas)
            {
                Console.WriteLine($"--->Running Delete Test on {ConsoleHelper(schema)}");
                TimeSpan time;
                if(schema.EncryptionConfiguration?.EnclaveEnabled == false && 
                    schema.EncryptionConfiguration?.EncryptionAlgoType == EncryptionAlgoType.Randomized)
                {
                    time = TimeMethodCall(() => _repo.DeletePatientsByNameClientSide(patients, schema));
                }
                else
                {
                    time = TimeMethodCall(() => _repo.DeletePatientsByNameSqlSide(patients, schema));
                }

                var testResult = new TestResult()
                {
                    TestName = "RunDeletes",
                    SchemaName = schema.SchemaName,
                    EncryptionAlgo = schema.EncryptionConfiguration?.EncryptionAlgoType.ToString() ?? _encryptionRegime,
                    TestSize = patients.Count(),
                    DatabaseSize = _dbSize,
                    Time = time.TotalSeconds
                };
                testResults.Add(testResult);
            }

            LogResults(testResults, outputFileName);
        }

        private void RunFindWhereDOBTest(string outputFileName)
        {
            var testResults = new List<TestResult>();

            foreach(var schema in _schemas)
            {
                Console.WriteLine($"--->Running Find DOB Test on {ConsoleHelper(schema)}");
                TimeSpan time;
                DateOnly testDate = DateOnly.FromDateTime(Faker.Identification.DateOfBirth());
                if((schema.EncryptionConfiguration?.EnclaveEnabled == true && 
                    schema.EncryptionConfiguration?.EncryptionAlgoType == EncryptionAlgoType.Randomized) || schema.EncryptionConfiguration == null)
                {
                    time = TimeMethodCall(() => _repo.FindPatientsWhereDobSqlSide(testDate, schema));
                }
                else
                {
                    time = TimeMethodCall(() => _repo.FindPatientsWhereDobClientSide(testDate, schema));
                }

                var testResult = new TestResult()
                {
                    TestName = "FindWhereDOB",
                    SchemaName = schema.SchemaName,
                    EncryptionAlgo = schema.EncryptionConfiguration?.EncryptionAlgoType.ToString() ?? _encryptionRegime,
                    DatabaseSize = _dbSize,
                    Time = time.TotalSeconds
                };
                testResults.Add(testResult);
            }

            LogResults(testResults, outputFileName);
        }

        private void InitializeAllSchemas(EncryptionAlgoType encryptionAlgoType)
        {
            _encryptionRegime = encryptionAlgoType.ToString();
            foreach(var schema in _schemas)
            {
                if(schema.EncryptionConfiguration is not null)
                {
                    schema.EncryptionConfiguration.EncryptionAlgoType = encryptionAlgoType;
                }
                ResetSchema(schema);
            }
            EncryptionFieldCacheWarmup();
        }

        private void ResetSchema(SchemaConfiguration schema)
        {
            
            _repo.TearDownSchema(schema);
            if(schema.EncryptionConfiguration is not null)
            {
                _repo.ProvisionKeys(schema.EncryptionConfiguration);
            }
            _repo.CreateTables(schema);
        }

        private void EncryptionFieldCacheWarmup()
        {
            //create one patient in each schema and table, then delete them
            var patient = CreatePatients(1);
            foreach(var schema in _schemas)
            {
                _repo.InsertPatients(patient, schema);
                _repo.ClearTables(schema);
            }
        }

        private void InitializeAllData(IEnumerable<Patient> patients)
        {
            foreach(var schema in _schemas)
            {
                _repo.ClearTables(schema);
                _repo.BulkInsertPatients(patients, schema);
            }
        }


        private void ClearAllTables()
        {
            foreach(var schema in _schemas)
            {
                _repo.ClearTables(schema);
            }
        }

        
        private TimeSpan TimeMethodCall(Action func)
        {
            Stopwatch? sp = Stopwatch.StartNew();
            func.Invoke();
            return sp.Elapsed;
        }

        private static IEnumerable<Patient> CreatePatients(int n)
        {
            List<Patient> patients = new();
            for (int i = 0; i < n; i++)
            {
                var patient = new Patient
                {
                    PatientId = Guid.NewGuid(),
                    Name = Faker.Name.FullName(),
                    PhoneNumber = Faker.Phone.Number(),
                    DateOfBirth = DateOnly.FromDateTime(Faker.Identification.DateOfBirth())
                };
                patient.Email = Faker.Internet.Email(patient.Name);
                patients.Add(patient);


                var observation = new Observation
                {
                    PatientId = patient.PatientId,
                    Name = patient.Name,
                    Height = Faker.RandomNumber.Next(50, 74),
                    Weight = Faker.RandomNumber.Next(100, 350),
                    BloodPressure = $"{Faker.RandomNumber.Next(100, 200)}/{Faker.RandomNumber.Next(50,100)}",
                    ObservationDate = Faker.Identification.DateOfBirth()
                };
                patient.Observations.Add(observation);
            }

            return patients;
        }

        private static void LogResults(IEnumerable<TestResult> testResults, string filename)
        {
            
            string path = $"./results/{filename}";
            // Write to a file.
            if(!File.Exists(path))
            {
                using (var writer = new StreamWriter(path))
                using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteRecords(testResults);
                }
            }
            else
            {
                var config = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    // Don't write the header again.
                    HasHeaderRecord = false,
                };
                using (var stream = File.Open(path, FileMode.Append))
                using (var writer = new StreamWriter(stream))
                using (var csv = new CsvWriter(writer, config))
                {
                    csv.WriteRecords(testResults);
                }
            }
        }

        public string ConsoleHelper(SchemaConfiguration schema)
        {
            if(schema.EncryptionConfiguration is null) return "NotEncrypted";

            if(schema.EncryptionConfiguration.EnclaveEnabled)
            {
                return $"EncryptedWithEnclaves -- {schema.EncryptionConfiguration.EncryptionAlgoType}";
            }

            return $"Encrypted -- {schema.EncryptionConfiguration.EncryptionAlgoType}";
        }
    }
}