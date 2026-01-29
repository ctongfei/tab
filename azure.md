#### Interpretation of `az://` URLs

The `az://` URL scheme is used to access Azure Blob Storage. The interpretation of the URL authority (the part between `az://` and the first `/`) can be configured with the `--az-url-authority-is-account` flag.

Two interpretations are supported:
 - `az://$account/$container/$path` - the authority is the storage account name
 - `az://$container/$path` - the authority is the container name (default `adlfs` behavior)

The first form requires the `--az-url-authority-is-account` flag. The second form is consistent with `s3://` and `gs://` URLs, but requires the `AZURE_STORAGE_ACCOUNT` environment variable to be set.

