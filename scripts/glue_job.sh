#!/bin/bash
aws glue create-job \
    --name movie-data-analysis-job \
    --role AWSGlueServiceRole \
    --command '{"Name":"glueetl","ScriptLocation":"s3://movie-data-analysis/scripts/glue_job_script.py","PythonVersion":"3"}' \
    --default-run-properties '{"--enable-metrics":""}'

aws glue start-job-run --job-name movie-data-analysis-job
