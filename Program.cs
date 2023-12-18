using LatencyTesting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Azure.Identity;
using Microsoft.Data.SqlClient.AlwaysEncrypted.AzureKeyVaultProvider;
using Microsoft.Data.SqlClient;
using Dapper;

IConfiguration config = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddEnvironmentVariables()
    .Build();


var token = new InteractiveBrowserCredential();
var kvProvider = new SqlColumnEncryptionAzureKeyVaultProvider(token);
var kvProviderDict = new Dictionary<string, SqlColumnEncryptionKeyStoreProvider>()
    {
        {SqlColumnEncryptionAzureKeyVaultProvider.ProviderName, kvProvider}
    };

SqlConnection.RegisterColumnEncryptionKeyStoreProviders(customProviders: kvProviderDict);

//for DateOnly use with Dapper, cuz I want to
SqlMapper.AddTypeHandler(new DapperSqlDateOnlyTypeHandler());

var host = Host.CreateDefaultBuilder()
    .ConfigureServices((_, services) =>
    {
        services.AddSingleton(config);
        services.AddSingleton(kvProvider);
        services.AddScoped<IDatabaseRepository, DatabaseRepository>();
        services.AddScoped<ITestRunnerService, TestRunnerService>();
    }).Build();


var testRunnerService = host.Services.GetRequiredService<ITestRunnerService>();


testRunnerService.InvokeTests();



