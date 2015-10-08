## DataDog Collect YARN Metrics
Collects application metrics from YARN and publishes these to DataDog.  


### Metrics collected
	Metric name: yarn.apps.running
		Tags:
			apptype:TOTAL
			appscope:queue|apptype|all
			apptype:<applicationType> (eg MR, SPARK, TEZ)
			submittype:INTERACTIVE|BATCH
			queuename:<queue> (eg deafult, production)
			appmetric:allocatedGB (by system by queue and by apptype)
			appmetric:allocatedVCores (by system by queue and by apptype)
			appmetric:runningContainers (by system by queue and by apptype)
			appmetric:totmemsecs (by queue and by apptype)
			appmetric:totvcoresecs (by queue and by apptype)
			appmetric:maxmemoryseconds (by system)
			appmetric:maxvcoreseconds (by system)
			appmetric:maxelapsedtime (by system)
			appmetric:maxallocatedGB (by system)
			appmetric:maxallocatedVCores (by system)
			appmetric:maxcontainers (by system)
	
### Dependencies
* DataDog Agent 
* Python 2.7 +
