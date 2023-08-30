The two images in this docker directory are the main images that are
run in gamechanger-crawlers and its pipeline. They are as follows:

- ```core``` - docker image that installs all of the requirements in ```docker/core/minimal-requirements.txt```
  and installs the Google chrome browser. This image does not copy the
  repo's code into it, and is not ready to execute the pipeline's code.
  

- ```crawl_download_upload``` - docker image built off of ```core``` that
  copies the repo's code and sets up the environment variables to run
  crawls automatically. This image is setup with an ENTRYPOINT for
  automatic execution of ```docker/crawl_download_upload/run_job.sh```,
  and thus should be deployed in active pipelines.