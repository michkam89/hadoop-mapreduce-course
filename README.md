# hadoop-mapreduce-course

Just another training repository

## Notes on running jobs in the cloud
To run jobs on AWS:
1. `pip install awscli`
2. For python also SDK `pip install boto3`
3. Configure credentials `aws configure`
4. Create S3 bucket, upload data
5. Run 05_taxi/03_most_popular_pickup_location.py on EMR cluster (only master node, cluster created only for this job and automatically terminated after job)

```python
# -r for 'runner'
python 03_most_popular_pickup_location.py -r emr s3://<bucket-name>/data/* --output-dir=s3://<bucket-name>/output/job1
```

6. Run on EMR cluster with 1 master and 4 workers
```python
# --num-core-instances
python 03_most_popular_pickup_location.py -r emr --num-core-instances 4  s3://<bucket-name>/data/* --output-dir=s3://<bucket-name>/output/job2
```
__IMPORTANT__ MRjob fails on EMR clusters created on-demand with default params when jobs with multiple steps are specified in the job. After first job is completed, cluster is terminating and the following steps cannot be executed. Solution is to run on existing cluster (provide cluster ID) or add `--add-steps-in-batch` parameter to the commands

__Bootstrap installation__
  - new EMR clusters use `pip3` instead of `pip`
  - #TODO fix bootstraping commands with ntlk library installation on cluster in `.mrjob.conf` file
