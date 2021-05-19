# Gamechanger Scrapy Spiders

## Things to Know
- `GCSpider` inherits from `Scrapy.spider`
- `GCSelenium` Spider inherits from `GCSpider`
- Since we want to run each spider on its own, the settings for each of the above classes are pulled in from `runspider_settings.py` and applied directly, default project settings<span>.py</span> is not used

## Custom Middlewares (downloader_middlewares.py)
- Selenium middleware is only used with GCSelenium spider
	- `Spider.parse(response)` yields `SeleniumResponse` instead of a `Scrapy.Response`
- Ban Evasion middleware adds some things to help avoid getting banned on certain sites
	- `Spider.rotate_user_agent` is True by default
	- `Spider.randomly_delay_request` is False by default, can take True or an iterable of valid numbers for `time.sleep()`


## How-to deploy/run in Prod
1. Checkout appropriate repository/branch into deployment directory
2. Kill any running gc-crawler containers `docker rm --force <...>`
3. Rebuild base and crawler containers `paasJobs/scripts/rebuild_prod_crawlers_and_base.sh`
4. (if only running) Run job interactively with `paasJobs/scripts/run_prod_gc_crawler_downloader.sh`
5. (if deploying) Setup cron job to launch appropriate `run_prod_...` script on schedule

## How-to run in dev
- Build the crawler base image `dev/docker/gc_crawler/Dockerfile`, context is repo root
- Run in either of two ways
    - With the crawler base image, mount the repo dir into `/app` and run appropriate job script, e.g.
        ```shell script
        docker run --rm \
          -v 'output_dir:/var/tmp/dl:rw' \
          -v 'gamechanger:/app:ro' \
          -e 'APP_REPO_DIR=/app' \
          -e 'DLDIR=/var/tmp/dl' \
          gc_crawler:latest
        ```
    - With the image that contains the code already (build with appropriate Dockerfile) in the `/app` directory
        ```shell script
        docker run --rm \
          -v 'output_dir:/var/tmp/dl:rw' \
          -e 'DLDIR=/var/tmp/dl' \
          gc_crawler:latest
        ```
    - **Note: To preserve output, instead of mounting local directories, you can use docker volumes, e.g.
        ```shell script
        docker volume create crawler_output;
        docker run --rm \
          --mount source=crawler_output,destination=/var/tmp/dl \
          -e 'DLDIR=/var/tmp/dl' \
          gc_crawler:latest
        ``` 