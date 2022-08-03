# paasJobs/scripts

## Directory Structure

```
├── gamechanger-crawlers/paasJobs/scripts
│   ├── README.md
│   ├── crawl_then_upload.py
│   ├── constants.conf
│   ├── rebuild_images.sh
│   ├── rebuild_prod_crawlers_and_base.sh
│   ├── rebuild_prod_crawlers.sh
```

## [crawl_then_upload.py](crawl_then_upload.py)

### Notes for Local Usage/ Testing

- Create and activate the virtual environment described [here](../../README.md#how-to-setup-local-env-for-development).
- Configure environment variables with [crawler_downloader.conf.sh](../configs/crawler_downloader.conf.sh).
- Supply AWS credentials via command line arguments when running locally.
  1. Refresh your AWSAML token.
  2. Pass credentials as arguments when running the script:
     ```
     --aws-profile <AWS Profile>
     --aws-access-key <AWS Access Key>
     --aws-secret-key <AWS Secret Key>
     --aws-session-token <AWS Session Token>
     ```
