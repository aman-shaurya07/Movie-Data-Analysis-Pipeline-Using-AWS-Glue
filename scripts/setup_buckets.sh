#!/bin/bash
aws s3 mb s3://movie-data-analysis
aws s3api put-object --bucket movie-data-analysis --key raw-data/
aws s3api put-object --bucket movie-data-analysis --key processed-data/
