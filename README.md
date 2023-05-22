# costOptimizer

Script to analyze the customer environment for all the RDS instances and provide ways to optimize the cost

Script will run across all the RDS instances in customer account and provide the following 
1. Savings in migrating to graviton instances 
2. Downsizing of instance type based on the max cpu usage in the specified period 
3. Identify the instances with no application connections 
4. Savings in migrating from i01 to gp3 based on the allocated IOPS 

The output will be an csv file which will have all the instance information along with the cost savings options.

## How to run the script

Install python3.7 or above version
Install pip3
Clone the repository to the directory
Install required libraries

```

$ pip3 install -r requirements.txt  

$ python3 ./costoptimizer.py -a -r us-east-2
Working on the instance ams57-instance-1
Getting the details for db.t3.small aurora-mysql Single-AZ False
Getting the Graviton details for db.t3.small ams57-instance-1
ams57-instance-1 aurora-mysql db.t3.small 2 2 0.041 NA 0.041
Working on the instance ams8-instance-1
Getting the details for db.t3.medium aurora-mysql Single-AZ False
Getting the Graviton details for db.t3.medium ams8-instance-1
ams8-instance-1 aurora-mysql db.t3.medium 4 2 0.082 db.t4g.medium 0.073
Working on the instance ams8-instance-1-us-east-2c
Getting the details for db.t3.medium aurora-mysql Single-AZ False
Getting the Graviton details for db.t3.medium ams8-instance-1-us-east-2c
ams8-instance-1-us-east-2c aurora-mysql db.t3.medium 4 2 0.082 db.t4g.medium 0.073
```