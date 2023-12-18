# DbEncryptionLatencyTesting
a project for testing the performance characteristics of encryption, encryption with enclaves, and no encryption

# Setup
## Infrastructure
Create and Entra Id user that you will use to log in to an azure sql server and an azure key vault.
Create an azure sql server instance and provision one azure sql db. Enable VBS enclaves during this setup (or later if you forget). Set your Entra Id user as the sql server admin.
Create an azure key vault. Permission your Entra Id user to have access to the vault, specifically the relevant KEY permissions.

## Project
Clone the repo and restore the project from nuget
Modify the appsettings.json to point to your Azure Key Vault and your Azure Sql DB instance (placeholders provided)
Modify the TestRunnerService InvokeTests and RunTests methods to run the specific tests with desired parameters.

## Data
If you don't want to run the tests yourself, existing data can be found at ./results/fullResults.csv
plotresults.py has the logic for creating the charts from the pointed to file. a requirements.txt file is included for ease of installation of python libraries. Be sure to create and activate your own virtual environment before installing these packages with a `python3 -m pip install -r requirements.txt`
