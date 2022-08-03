# gamechanger-crawlers/paasJobs/src

This directory contains Python files that support scripts in the
[scripts](../scripts/) folder.

## Directory Structure

````
├── gamechanger-crawlers/paasJobs/src
│   ├── README.md
│   ├── __init__.py
│   ├── crawler_job.py                      CrawlerJob class. Main class that wraps multiple functionalities
│   ├── environment
│   │   ├── __init__.py
│   │   ├── environment.py                  Environment class. Manages environment variables
│   │   └── environment_type.py             EnvironmentType class. Enumerates deployment environments
│   ├── services
│   │   ├── __init__.py
│   │   └── s3_service.py                   S3Service class. Provides connection to S3 and supports data transfer operations
│   ├── utils
│   │   ├── __init__.py
│   │   ├── logger.py                       Logging utilities
│   │   ├── timer.py                        Timer class. Time functionalities
│   │   ├── timestamp.py                    Timestamp class. Apply timestamps to crawler jobs
│   │   ├── args
│   │   │   ├── __init__.py
│   │   │   ├── environment_argument.py     EnvironmentArgument class. Format env arguments for Docker
│   │   │   ├── user_argument.py            UserArgument class. Format user argument for Docker
│   │   │   └── volume_argument.py          VolumeArgument class. Format volume arguments for Docker
│   │   ├── paths
│   │   │   ├── __init__.py
│   │   │   ├── container_paths.py          ContainerPaths class. Paths relative to the Docker container
│   │   │   ├── host_paths.py               HostPaths class. Paths relative to the host machine
│   │   │   ├── s3_paths.py                 S3Paths class. Paths relative to S3
│   │   │   └── filenames.py                Filenames class. Stores reused file names
````
