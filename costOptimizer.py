#!/usr/bin/python3

import boto3
import argparse
import datetime
import sys
import traceback
import json
from operator import itemgetter
import csv

global args
global allInfo
global pricingList
global pricingClient

NoData = "No Data"

columns = ["DBInstanceIdentifier", "DBInstanceClass", "Engine", "EngineVersion", "MultiAZ", "StorageType", "Iops", "AllocatedStorage" ]
metrics = [ "CPUUtilization", "DatabaseConnections", "WriteIOPS", "ReadIOPS" ] 
newColumns = ['instanceType','vCPU','Memory','MonthlyCost','GravitonInstanceType','GravitonMonthlyCostSavings','ScaleDownInstanceType','ScaleDownCostSavings','CostSavings-io1-to-gp3','NoConnectionsInstances']

engineMapping = {
   "aurora-mysql": "Aurora MySQL",
   "aurora-postgresql": "Aurora PostgreSQL",
   "mariadb": "MariaDB",
   "mysql": "MySQL",
   "oracle": "Oracle",
   "postgres": "PostgreSQL",
   "sqlserver-se": "SQL Server",
   "sqlserver-ee": "SQL Server",
   "sqlserver-ex": "SQL Server",
   "db2-se": "Db2",
   "db2-ee": "Db2"
}

editionMapping = {
   "sqlserver-se": "Standard",
   "sqlserver-ex": "Web",
   "sqlserver-ee": "Enterprise",
   "oracle-se2": "Standard Two",
   "oracle-ee": "Enterprise"
}

instanceHierarcy = ["micro","small","medium","large","xlarge","2xlarge","4xlarge","8xlarge","12xlarge","16xlarge","24xlarge"]
tInstanceTypes = { "t4g": ["t2","t3"] }
mInstanceTypes = { "m6g" : ["m5","m6"] }
rInstanceTypes = { "r6g" : [ "r6"] }

#------------------------------------------------------------------------------

def get_cw_metrics(instanceName, start_date, end_date, period):

   metricQuery = []
   id = 0 
   for metricName in metrics:
      id = id + 1
      metricd = {
                 "Id": "id{}".format(id),
                 "MetricStat": {
                     "Metric": {
                        "Namespace": "AWS/RDS",
                        "MetricName": metricName,
                        "Dimensions": [ { "Name": "DBInstanceIdentifier", "Value": instanceName } ]
                      },
                   "Period": period,
                   "Stat": "Maximum"
                  },
                 "ReturnData": True
                }

      metricQuery.append(metricd)

   cloudwatch = boto3.client('cloudwatch',region_name=args.region)
   try:
      response = cloudwatch.get_metric_data(
                MetricDataQueries = metricQuery,
                StartTime=start_date,
                EndTime=end_date
               )

      #print(json.dumps(response,indent=2,default=str))
      result = {}
      for metric in response['MetricDataResults']:
         metricValue = None
         if len(metric['Values']) == 0:
            metricValue = NoData
         else: 
            metricValue = round(metric['Values'][0],2)
         result[metric['Label']] = metricValue
      return result

   except Exception as e:
      traceback.print_exception(*sys.exc_info())
      print(e)

#------------------------------------------------------------------------------

def get_pi_metrics(start_date, end_date):

   global args

   pi = boto3.client('pi',region_name=args.region)
   try:

      response = client.get_resource_metrics(
                ServiceType='RDS'|'DOCDB',
                Identifier='string',
                MetricQueries=[ { 'Metric': 'db.load.avg' } ],
                StartTime=start_date,
                EndTime=end_date,
                PeriodInSeconds=60
               )

      sortList = []     
      for item in response['Datapoints']:
          sortList.append(item)
          sortList = sorted(sortList, key=itemgetter('Timestamp'))

      return sortList

   except Exception as e:
      traceback.print_exception(*sys.exc_info())


#------------------------------------------------------------------------------

def get_metrics(instanceName):

   global args

   endnow= datetime.datetime.utcnow()
   startnow = datetime.datetime.utcnow() - datetime.timedelta(days=int(args.period))
   periodsec = (int(args.period)+10)*24*60*60
   start_date = startnow.replace(hour=0, minute=0, second=0, microsecond=0)
   end_date = endnow.replace(hour=23, minute=59, second=59, microsecond=0)
   cw_metrics = get_cw_metrics(instanceName, start_date,end_date,periodsec)
   return cw_metrics

   #if args.instance_info["PerformanceInsightsEnabled"] :
   if False:
      print("Performance insights enabled for the instance")
      newlist =[]
      for i in range(0,int(args.period)):
         now = datetime.datetime.utcnow() - datetime.timedelta(days=i)
         start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
         end_date = now.replace(hour=23, minute=59, second=59, microsecond=0)
         newlist = newlist + get_pi_metrics(start_date,end_date)
      output[metricName] = newlist
   else:
      print("Performance insights is not enabled for this instance")

   return output1 

#------------------------------------------------------------------------------

def get_instance_details(instanceName, region):

   try:
      rds = boto3.client("rds", region_name=region)
      response = rds.describe_db_instances(DBInstanceIdentifier=instanceName)
      instance_info = response['DBInstances'][0]
      return instance_info
   except:
      traceback.print_exc()
      print("Unable to gather instance information. Instance may not be present in the given region")
      sys.exit(1)

#------------------------------------------------------------------------------

def get_instance_cost(instanceType, engine, AZ, IOopt):

   global pricingClient

   vcpu = None
   memory = None
   cost = None

   print("Getting the details for {} {} {} {}".format(instanceType, engine, AZ, IOopt))

   filters = [ 
               { 'Type': 'TERM_MATCH', 'Field': 'regionCode', 'Value': args.region },
               { 'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': engineMapping[engine] },
               { 'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instanceType },
               { 'Type': 'TERM_MATCH', 'Field': 'deploymentOption', 'Value': AZ}
             ]

   data = pricingClient.get_products( ServiceCode='AmazonRDS', Filters=filters)
   
   price_list = data['PriceList']

   for line in price_list:
      instorig = json.loads(line)
      #print(json.dumps(instorig['product'],indent=2))
      if not IOopt:
         if instorig['product']['attributes']['storage'] == "Aurora IO Optimization Mode":
            continue
      memory, vcpu, cost, newInstanceType = cpu_memory_cost_details(instorig)

   #print(vcpu, memory, cost)
   return vcpu, memory, cost

#------------------------------------------------------------------------------

def get_param():

   global args
   parser = argparse.ArgumentParser()
   parser.add_argument("-n", "--name", action="store", default=None, help="Instance name")
   parser.add_argument("-a", "--all", action="store_true", default=False, help="All Instance in this account")
   parser.add_argument("-r", "--region", action="store", default=None, help="Region")
   parser.add_argument("-p", "--period", action="store", default=14, help="Period for calculation. Default 2 weeks")
   args = parser.parse_args()

   if args.name and args.all :
      print("Only instance name or all option can be provided and not both")
      sys.exit(1)
   if args.region is None:
      print("Invalid parameter passed")
      sys.exit(1)

   args.pricingFile = "{}-pricing.json".format(args.region)

#------------------------------------------------------------------------------

def merge_instance_info( instance_info, metric_data):
   global allInfo
   indInfo = {}
   for col in columns:
       indInfo[col] = instance_info.get(col,'')
   for keys in metric_data:
       indInfo['Max{}'.format(keys)] = metric_data[keys]

   return indInfo

#------------------------------------------------------------------------------

def write_to_csv():

   global allInfo

   fieldnames = []
   for keys in columns:
      fieldnames.append(keys)
   for keys in metrics:
      fieldnames.append('Max{}'.format(keys))
   for keys in newColumns:
      fieldnames.append(keys)

   with open('costOptimizer.csv', 'w', newline='') as f:
      writer = csv.DictWriter(f,fieldnames=fieldnames)
      writer.writeheader()
      for line in allInfo:
        #print(line)
        writer.writerow(line)

#------------------------------------------------------------------------------

def get_cost_info(instanceType, instanceInfo, AZ, auroraIOopt):

   global pricingList

   memory = None
   vcpu = None
   cost = None
   engine = None

   if instanceInfo['Engine']== "aurora":
      engine = "aurora-mysql"
   else:
      engine = instanceInfo['Engine']

   vcpu,memory,cost = get_instance_cost(instanceType, engine,AZ, auroraIOopt)


   return memory,vcpu,cost

#------------------------------------------------------------------------------

def mapGraviton(instanceType,indInfo, vcpu,memory, cost, AZ, auroraIOopt):

   newInstanceType = None
   newVcpu = None
   newMemory = None
   newCost = None

   print("Getting the Graviton details for {} {}".format(instanceType, indInfo['DBInstanceIdentifier']))

   filters = [ 
               { 'Type': 'TERM_MATCH', 'Field': 'regionCode', 'Value': args.region },
               { 'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': engineMapping[indInfo['Engine']] },
               { 'Type': 'TERM_MATCH', 'Field': 'deploymentOption', 'Value': AZ },
               { 'Type': 'TERM_MATCH', 'Field': 'vcpu', 'Value': vcpu },
               { 'Type': 'TERM_MATCH', 'Field': 'memory', 'Value': "{} GiB".format(memory) },
             ]

   data = pricingClient.get_products( ServiceCode='AmazonRDS', Filters=filters)

   price_list = data['PriceList']
   for line in price_list:
      instorig = json.loads(line)
      #print(json.dumps(instorig['product'],indent=2))
      if not auroraIOopt:
         if instorig['product']['attributes']['storage'] == "Aurora IO Optimization Mode":
            continue
      if 'AWS Graviton2' not in instorig['product']['attributes'].get('physicalProcessor','None') :
         continue

      newMemory, newVcpu, newCost, newInstanceType = cpu_memory_cost_details(instorig)

   if newInstanceType is None:
      return instanceType, vcpu, memory, cost

   
   return newInstanceType, newVcpu, newMemory, newCost

#------------------------------------------------------------------------------

def downsizeInstance(instanceType,indInfo, vcpu, memory, cost, AZ, auroraIOopt, factor):

   print("Getting the Downsize details for {} {} {} {} {}".format(instanceType, indInfo['DBInstanceIdentifier'],vcpu,memory, factor))

   newVcpu = int(int(vcpu)/int(factor))
   if newVcpu == 0 or newVcpu == 1:
      # Current Generation instances supports only min of 2 vcpus
      newVcpu = 2
   newMemory = int(int(memory)/int(factor))
   #print (newVcpu, newMemory)
   newCost = None
   newInstanceType = None

   filters = [ 
               { 'Type': 'TERM_MATCH', 'Field': 'regionCode', 'Value': args.region },
               { 'Type': 'TERM_MATCH', 'Field': 'databaseEngine', 'Value': engineMapping[indInfo['Engine']] },
               { 'Type': 'TERM_MATCH', 'Field': 'deploymentOption', 'Value': AZ },
               { 'Type': 'TERM_MATCH', 'Field': 'vcpu', 'Value': str(newVcpu) },
               { 'Type': 'TERM_MATCH', 'Field': 'memory', 'Value': "{} GiB".format(newMemory) },
               { 'Type': 'TERM_MATCH', 'Field': 'currentGeneration', 'Value': "yes"}
             ]

   if indInfo['Engine'] in editionMapping:
      filters.append({ 'Type': 'TERM_MATCH', 'Field': 'databaseEdition', 'Value': editionMapping[indInfo['Engine']]})
   
   data = pricingClient.get_products( ServiceCode='AmazonRDS', Filters=filters)

   price_list = data['PriceList']
   for line in price_list:
      instorig = json.loads(line)
      #print(instorig['product']['attributes']['instanceType'])
      if not auroraIOopt:
         if instorig['product']['attributes']['storage'] == "Aurora IO Optimization Mode":
            continue
      if instanceType.split(".")[0]+"."+instanceType.split(".")[1] == instorig['product']['attributes']['instanceType'].split(".")[0]+"."+instorig['product']['attributes']['instanceType'].split(".")[1]:
         newMemory, newVcpu, newCost, newInstanceType = cpu_memory_cost_details(instorig)
      #print(json.dumps(instorig['product'],indent=2))

   if newInstanceType is None:
      return instanceType, vcpu, memory, cost
   
   #print("Printing the value", newInstanceType, newVcpu, newMemory, newCost)
   return newInstanceType, newVcpu, newMemory, newCost

#------------------------------------------------------------------------------

def cpu_memory_cost_details(instorig):
   newMemory,newVcpu,newCost,newInstanceType = None,None,None,None
   inst = instorig
   inst = inst['terms']['OnDemand']
   inst = list(inst.values())[0]
   inst = list(inst["priceDimensions"].values())[0]
   inst = inst['pricePerUnit']
   currency = list(inst.keys())[0]
   newCost = float(inst[currency])
   attributes = instorig['product']['attributes']
   if instorig['product']["productFamily"] == 'Database Instance':
      newMemory =  attributes['memory'].split(" ")[0]
      newVcpu =  attributes['vcpu']
      newInstanceType =  attributes['instanceType']

   return newMemory, newVcpu, newCost, newInstanceType

#------------------------------------------------------------------------------

def cost_optimize(indInfo):

   IOPS_cost = 0.20

   instanceType = indInfo["DBInstanceClass"]
   if instanceType == "db.serverless" :
      indInfo['instanceType'] = "db.serverless"
      indInfo['vCPU'] = None
      indInfo['MonthlyCost'] = None
      indInfo['GravitonInstanceType'] = None
      indInfo['GravitonMonthlyCostSavings'] = None
      indInfo['ScaleDownInstanceType'] = None
      indInfo['ScaleDownCostSavings'] = None
      indInfo['CostSavings-io1-to-gp3'] = None
      indInfo['NoConnectionsInstances'] = None
      print("Current instance is serverless...Skipping")
      return indInfo

   AZ=None
   if indInfo['MultiAZ']:
      AZ="Multi-AZ"
   else:
      AZ="Single-AZ"

   auroraIOopt = False
   if indInfo['StorageType'] == "aurora-iopt1":
      auroraIOopt = True

   memory,vcpu,cost = get_cost_info(instanceType,indInfo, AZ, auroraIOopt)

   if memory is None or vcpu is None or cost is None:
      print("Unable to get the details for {}".format(indInfo['DBInstanceIdentifier']))
      return None

   engine = indInfo["Engine"]
   maxcpu = indInfo["MaxCPUUtilization"]
   #maxcpu = 10

   totalcost = str(float(cost) * 730)

   GinstanceType,Gvcpu,Gmemory,Gcost  = mapGraviton(instanceType,indInfo, vcpu,memory,cost, AZ, auroraIOopt)
   #print(GinstanceType, Gmemory,Gvcpu,Gcost)

   GtotalcostSavings = 0 
   if GinstanceType == instanceType : 
      GinstanceType = "NA"
   else:    
      GtotalcostSavings = str(float(totalcost) - float(Gcost) * 730)

   LinstanceType = instanceType
   Lvcpu, Lmemory, Lcost = None, None, None
   if maxcpu != NoData :
      if float(maxcpu) <= 20.0 :
         LinstanceType, Lvcpu, Lmemory, Lcost  = downsizeInstance(instanceType,indInfo, vcpu, memory, cost, AZ, auroraIOopt, 4)
      elif float(maxcpu) <= 40.0 :
         LinstanceType, Lvcpu, Lmemory, Lcost  = downsizeInstance(instanceType,indInfo, vcpu, memory, cost, AZ, auroraIOopt, 2)

   LtotalcostSavings = 0 
   if LinstanceType == instanceType : 
      LinstanceType = "NA"
   else:    
      LtotalcostSavings = str(float(totalcost) - float(Lcost) * 730)
   #print(LinstanceType, Lmemory,Lvcpu,Lcost)

   costSavingsio1 = ""
         
   if indInfo["StorageType"] == "io1" and indInfo["MaxReadIOPS"] != NoData and indInfo["MaxWriteIOPS"] != NoData :
      totalMaximumIOPS = float(indInfo["MaxReadIOPS"]) + float(indInfo["MaxWriteIOPS"])
      if float(indInfo["Iops"]) > totalMaximumIOPS*1.2 :
         costSavingsio1 = str(float(indInfo["Iops"]) * IOPS_cost)

   # Number of database connections
   noConnnectionsInstanceId = ""
   if indInfo["MaxDatabaseConnections"] == 0 :
      noConnnectionsInstanceId = indInfo["DBInstanceIdentifier"]

   indInfo['instanceType'] = instanceType
   indInfo['vCPU'] = vcpu 
   indInfo['Memory'] = memory
   indInfo['MonthlyCost'] = totalcost
   indInfo['GravitonInstanceType'] = GinstanceType
   indInfo['GravitonMonthlyCostSavings'] = GtotalcostSavings
   indInfo['ScaleDownInstanceType'] = LinstanceType
   indInfo['ScaleDownCostSavings'] = LtotalcostSavings
   indInfo['CostSavings-io1-to-gp3'] = costSavingsio1
   indInfo['NoConnectionsInstances'] = noConnnectionsInstanceId

   print (indInfo['DBInstanceIdentifier'],engine, instanceType, memory,vcpu,cost, GinstanceType, Gcost)
   return indInfo

#------------------------------------------------------------------------------

def main():

   global allInfo
   global pricingList
   global pricingClient

   allInfo = []
   get_param()

   pricingClient = boto3.client('pricing', region_name="us-east-1")

   try:
      rds = boto3.client("rds", region_name=args.region)
      if args.name :
         response = rds.describe_db_instances(DBInstanceIdentifier=args.name)
      else:
         response = rds.describe_db_instances()

      for instanceNames in response['DBInstances']:
         instanceName = instanceNames['DBInstanceIdentifier']
         print("Working on the instance {}".format(instanceName))
         instance_info = get_instance_details(instanceName, args.region)
         if instance_info['Engine'] == "docdb":
            continue
         metric_data= get_metrics(instanceName)
         indInfo = merge_instance_info (instance_info,metric_data)
         indInfo = cost_optimize(indInfo)
         if indInfo is None:
            print("Skipping the details for {}".format(instanceNames['DBInstanceIdentifier']))
         else:
            allInfo.append(indInfo)
      write_to_csv()

   except:
      traceback.print_exc()
      print("Process failed with error")
      sys.exit(1)

if __name__ == "__main__":
    main()
