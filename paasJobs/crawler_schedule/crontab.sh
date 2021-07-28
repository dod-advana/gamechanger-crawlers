## -- VARS --

parser_threads=68
max_ocr_threads=1
skip_neo4j="no"
schedule_dir="../crawler_schedule"

flockfile=/data/run/gamechanger-crawler-ingest.lock
cmd=/data/gamechanger/gamechanger-data/paasJobs/job_runner.sh
arg1=/data/gamechanger/gamechanger-data/paasJobs/jobs/configs/crawler_ingest.conf.sh

# min hour dayMonth month dayWeek

# Sunday
1 1 * * 0 flock -n $flockfile env LOCAL_SPIDER_LIST_FILE="${schedule_dir}/sunday.txt" JOB_NAME="Crawler-Sunday" MAX_OCR_THREADS_PER_FILE=$max_ocr_threads MAX_PARSER_THREADS=$parser_threads SKIP_NEO4J_UPDATE=$skip_neo4j $cmd $arg1

# Monday
1 1 * * 1 flock -n $flockfile env LOCAL_SPIDER_LIST_FILE="${schedule_dir}/monday.txt" JOB_NAME="Crawler-Monday" MAX_OCR_THREADS_PER_FILE=$max_ocr_threads MAX_PARSER_THREADS=$parser_threads SKIP_NEO4J_UPDATE=$skip_neo4j $cmd $arg1

# Tuesday
1 1 * * 2 flock -n $flockfile env LOCAL_SPIDER_LIST_FILE="${schedule_dir}/tuesday.txt" JOB_NAME="Crawler-Tuesday" MAX_OCR_THREADS_PER_FILE=$max_ocr_threads MAX_PARSER_THREADS=$parser_threads SKIP_NEO4J_UPDATE=$skip_neo4j $cmd $arg1

# Wednesday
1 1 * * 3 flock -n $flockfile env LOCAL_SPIDER_LIST_FILE="${schedule_dir}/wednesday.txt" JOB_NAME="Crawler-Wednesday" MAX_OCR_THREADS_PER_FILE=$max_ocr_threads MAX_PARSER_THREADS=$parser_threads SKIP_NEO4J_UPDATE=$skip_neo4j $cmd $arg1

# Thursday
1 1 * * 4 flock -n $flockfile env LOCAL_SPIDER_LIST_FILE="${schedule_dir}/thursday.txt" JOB_NAME="Crawler-Thursday" MAX_OCR_THREADS_PER_FILE=$max_ocr_threads MAX_PARSER_THREADS=$parser_threads SKIP_NEO4J_UPDATE=$skip_neo4j $cmd $arg1

# Friday
1 1 * * 5 flock -n $flockfile env LOCAL_SPIDER_LIST_FILE="${schedule_dir}/friday.txt" JOB_NAME="Crawler-Friday" MAX_OCR_THREADS_PER_FILE=$max_ocr_threads MAX_PARSER_THREADS=$parser_threads SKIP_NEO4J_UPDATE=$skip_neo4j $cmd $arg1

# Saturday
1 1 * * 6 flock -n $flockfile env LOCAL_SPIDER_LIST_FILE="${schedule_dir}/saturday.txt" JOB_NAME="Crawler-Saturday" MAX_OCR_THREADS_PER_FILE=$max_ocr_threads MAX_PARSER_THREADS=$parser_threads SKIP_NEO4J_UPDATE=$skip_neo4j $cmd $arg1








