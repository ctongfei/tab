# Cloud support

`tab` supports directly reading tabular data files or directories from cloud storage:

```bash
tab view s3://$bucket/$path
tab view gs://$bucket/$path
tab view az://$container/$path
tab view abfss://$container@$account.dfs.core.windows.net/$path
```


## AWS S3

Authentication methods (in order):

1. **Environment variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
2. **Profile**: `AWS_PROFILE` or default — handles `~/.aws/credentials`, SSO, assume role, instance metadata

```bash
# Option 1: Set credentials directly
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# Option 2: Use a profile
aws configure              # static keys
aws sso login              # SSO authentication
export AWS_PROFILE=my-profile
```

## Azure Blob Storage

Authentication methods (in order):

1. **Connection string**: `AZURE_STORAGE_CONNECTION_STRING`
2. **Account key**: `AZURE_STORAGE_KEY`
3. **SAS token**: `AZURE_STORAGE_SAS_TOKEN`
4. **Azure AD / RBAC**: `DefaultAzureCredential`
5. **Azure CLI**: Key fetched via `az storage account keys list`

```bash
# Option 1: Connection string
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=..."

# Option 2: Account key
export AZURE_STORAGE_ACCOUNT=myaccount
export AZURE_STORAGE_KEY=...

# Option 3: SAS token
export AZURE_STORAGE_ACCOUNT=myaccount
export AZURE_STORAGE_SAS_TOKEN="?sv=2022-11-02&ss=..."

# Option 4: Azure AD (requires RBAC role: Storage Blob Data Reader)
az login

# Option 5: CLI fallback (requires ARM access)
az login
```

#### Interpretation of `az://` URLs
The interpretation of the `az://` URL authority (the part between `az://` and the first `/`) can be configured with the `--az-url-authority-is-account` flag.

Two interpretations are supported:

 - `az://$container/$path` - the authority is the container name (default adlfs behavior)
 - `az://$account/$container/$path` - the authority is the storage account name

The first form is consistent with `s3://` and `gs://` URLs, but requires the `AZURE_STORAGE_ACCOUNT` environment variable to be set.
The second form requires the `--az-url-authority-is-account` flag. 

## Google Cloud Storage

Authentication methods (in order):

1. **`GOOGLE_APPLICATION_CREDENTIALS`**: Path to service account JSON
2. **ADC file**: `~/.config/gcloud/application_default_credentials.json`
3. **gcloud CLI**: Token from `gcloud auth print-access-token`
4. **`google.auth.default()`**: Default credential resolution

```bash
# Option 1: Service account
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Option 2: User credentials (ADC)
gcloud auth application-default login

# Option 3: CLI login (fallback)
gcloud auth login
```
