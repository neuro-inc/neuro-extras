# Neu.ro Extras

A set of tools and commands to extend the functionality of [Neu.ro](https://neu.ro) platform CLI client.

# Usage
Check-out `neuro-extras` CLI reference [here](./docs/cli.md) for main commands syntax and use-cases.

## Dowloading and extracting archive from bucket
### To platform `storage:` or `disk:`
- From Google bucket storage:
    - `neuro-extras data cp -x -t -v secret:gcp-creds:/gcp-creds.txt -e GOOGLE_APPLICATION_CREDENTIALS=/gcp-creds.txt gs://BUCKET_NAME/dataset.tar.gz storage:/project/dataset`
    - `secret:gcp-creds` is a secret, containing authentication credentials file used by [config](https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable) `gsutil`

- From AWS-compatible object storage:
    - `neuro-extras data cp -x -t -v secret:s3-creds:/s3-creds.txt -e AWS_SHARED_CREDENTIALS_FILE=/s3-creds.txt s3://BUCKET_NAME/dataset.tar.gz disk:disk-name-or-id:/project/dataset`
    - `secret:s3-creds` is a secret, containing auth data [file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html#envvars-list) for `aws` utility.

- From Azure blob object storage:
    - `neuro-extras data cp -x -t -e AZURE_SAS_TOKEN=secret:azure-sas-token azure+https://BUCKET_NAME/dataset.tar.gz storage:/project/dataset`
    - `secret:azure-sas-token` is a secret, containing [SAS token](https://docs.microsoft.com/en-us/azure/cognitive-services/translator/document-translation/create-sas-tokens?tabs=Containers) for accessing needed blob.

- From HTTP/HTTPS server:
    - `neuro-extras data cp -x -t https://example.org/dataset.tar.gz disk:disk-name-or-id:/project/dataset`

### To local machine
- From GCP bucket storage:
    - `neuro-extras data cp -x gs://BUCKET_NAME/dataset.tar.gz /project/dataset`
    - `gsutil` utility should be installed on local machine and authenticated to read needed bucket
    - [Supported](https://cloud.google.com/storage/docs/gsutil_install#sdk-install) Python verions are 3 (3.5 to 3.8, 3.7 recommended) and 2 (2.7.9 or higher)

- From AWS-compatible object storage:
    - `neuro-extras data cp -x s3://BUCKET_NAME/dataset.tar.gz /project/dataset`
    - `aws` utility should be installed on local machine and authenticated to read needed bucket
    - If needed, install it with `pipx install awscli` not to conflict with `neuro-cli`

- From Azure blob object storage:
    - `AZURE_SAS_TOKEN=$TOKEN neuro-extras data cp -x azure+https://BUCKET_NAME/dataset.tar.gz storage:/project/dataset`
    - `rclone` should be installed on the local machine

- From HTTP/HTTPS server:
    - `neuro-extras data cp -x -t https://example.org/dataset.tar.gz /project/dataset`
    - `rclone` should be installed on the local machine
