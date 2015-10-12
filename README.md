## DataDog Collect YARN Metrics
Collects application metrics from YARN and publishes these to DataDog.  

### Example DataDog YARN Metrics Dashboard

![datadog-screenshot](https://s3.amazonaws.com/avensolutions-images/Datadog_Dashboard.png)

### Metrics collected
	Datadog metrics:
		yarn.apps.queued								(Desc: COUNT of ALL queued applications, Tags:	None)
		yarn.apps.running								(Desc: COUNT of ALL running applications, Tags:	None)		
		yarn.apps.running.submittype					(Desc: COUNT by SubmitType, Tags: submittype:BATCH|INTERACTIVE)
		yarn.apps.running.allocatedGB					(Desc: SUM allocatedGB, Tags: None)		
		yarn.apps.running.allocatedVCores				(Desc: SUM allocatedVCores, Tags: None)		
		yarn.apps.running.runningContainers				(Desc: SUM runningContainers, Tags:	None)		
		yarn.apps.running.maxElapsedTime				(Desc: MAX ElapsedTime, Tags: None)		
		yarn.apps.running.maxAllocatedGB				(Desc: MAX allocatedGB, Tags: None)		
		yarn.apps.running.maxAllocatedVCores			(Desc: MAX allocatedVCores, Tags: None)		
		yarn.apps.running.maxRunningContainers			(Desc: MAX RunningContainers, Tags:	None)		
		yarn.apps.running.maxMemorySeconds				(Desc: MAX MemorySeconds, Tags:	None)		
		yarn.apps.running.maxVCoreSeconds				(Desc: MAX VCoreSeconds, Tags: None)		
		yarn.apps.running.apptype						(Desc: COUNT by AppType, Tags: apptype:{MR, TEZ, SPARK, etc})
		yarn.apps.running.queue							(Desc: COUNT by Queue, Tags: queue:{default, production, etc})
		yarn.apps.running.allocatedGB.byQueue			(Desc: SUM by Queue, Tags: queue:{default, production, etc})
		yarn.apps.running.allocatedGB.byAppType			(Desc: SUM by AppType, Tags: apptype:{MR, TEZ, SPARK, etc})
		yarn.apps.running.allocatedVCores.byQueue		(Desc: SUM by Queue, Tags: queue:{default, production, etc})
		yarn.apps.running.allocatedVCores.byAppType		(Desc: SUM by AppType, Tags: apptype:{MR, TEZ, SPARK, etc})
		yarn.apps.running.runningContainers.byQueue		(Desc: COUNT by Queue, Tags: queue:{default, production, etc})
		yarn.apps.running.runningContainers.byAppType	(Desc: COUNT by AppType, Tags: apptype:{MR, TEZ, SPARK, etc})
		yarn.apps.running.totalMemorySeconds.byQueue	(Desc: SUM by Queue, Tags: queue:{default, production, etc})
		yarn.apps.running.totalMemorySeconds.byAppType	(Desc: SUM by AppType, Tags: apptype:{MR, TEZ, SPARK, etc})
		yarn.apps.running.totalVCoreSeconds.byQueue		(Desc: SUM by Queue, Tags: queue:{default, production, etc})
		yarn.apps.running.totalVCoreSeconds.byAppType	(Desc: SUM by AppType, Tags: apptype:{MR, TEZ, SPARK, etc})
	
### Dependencies
* DataDog Agent 
* Python 2.7 +
