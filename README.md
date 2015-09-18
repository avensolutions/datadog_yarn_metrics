## DataDog Collect YARN Metrics
Collects application metrics from YARN and publishes these to DataDog.  


### Metrics collected
* yarn.apps.running.TOTAL :  Number of apps currently running  
* yarn.apps.running.MR : Number of MR apps currently running  
* yarn.apps.running.TEZ : Number of TEZ apps currently running  
* yarn.apps.running.SPARK : Number of SPARK apps currently running  
* yarn.apps.running.INTERACTIVE : Number of Interactive apps currently running  
* yarn.apps.running.BATCH : Number of Batch apps currently running  
* yarn.apps.running.queue.DEFAULT : Number of apps submitted in the default queue  
* yarn.apps.running.queue.PRODUCTION : Number of apps submitted in the production queue  
* yarn.apps.running.queue.PRODTACTICAL : Number of apps submitted in the productiontactical queue  
* yarn.apps.running.queue.DEVELOPMENT : Number of apps submitted in the development queue  
* yarn.apps.running.allocatedMB : Total allocatedMB for running jobs  
* yarn.apps.running.allocatedVCores : Total allocatedVCores for running jobs  
* yarn.apps.running.runningContainers : Total runningContainers for running jobs  


### Dependencies
* DataDog Agent 
