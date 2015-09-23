from checks import AgentCheck
from urllib2 import urlopen, URLError, HTTPError
import json, re, time, urllib2

class YARNMetrics(AgentCheck):
	"""Collect metrics on applications running in YARN via the RM REST API
	https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Applications_API
	User regex and queue names are specific to your environment and should be updated in the check method of this class
	"""
	
	event_type = 'yarn_metrics_collection'

	def check(self, instance):

		# update this for your environment to distinguish user submitted queries from batch queries, example u123456 may represent a human user
		user_pattern = "^[cd]\d{6}"
		
		resourcemanager_uri = instance.get('resourcemanager_uri', None)
		if resourcemanager_uri is None:
			raise Exception("resourcemanager_uri must be specified")
		user_pattern_regex = re.compile(user_pattern)

		try:
			apps_url = "http://" + resourcemanager_uri + "/ws/v1/cluster/apps?state=RUNNING"
			rmhost = resourcemanager_uri.split(":")[0]

			# get data
			apps_resp = urllib2.urlopen(apps_url)
			apps_json_obj = json.load(apps_resp)

			# initialize counters
			total_interactive_apps = 0
			total_batch_apps = 0
			total_default_queue = 0
			total_prod_queue = 0
			total_prodtact_queue = 0
			total_development_queue = 0
			total_mr_apps = 0
			total_tez_apps = 0
			total_spark_apps = 0
			total_allocatedMB = 0
			total_allocatedVCores = 0
			total_runningContainers = 0
			total_allocatedMB_prod = 0
			total_allocatedMB_prodtact = 0
			total_allocatedMB_dev = 0
			total_allocatedMB_def = 0

			# increment counters
			for i in apps_json_obj['apps']['app']:
				# Interactive vs batch applications
				user = i['user']
				if user_pattern_regex.match(user):
					total_interactive_apps += 1
				else:
					total_batch_apps += 1 
				#
				# Queues will vary by environment				
				#
				queue = i['queue']
				if queue == 'default':
					total_default_queue += 1
					total_allocatedMB_def += i['allocatedMB']
				elif queue == 'production':
					total_prod_queue += 1
					total_allocatedMB_prod += i['allocatedMB']
				elif queue == 'prodtactical':
					total_prodtact_queue += 1	
					total_allocatedMB_prodtact += i['allocatedMB']
				elif queue == 'development':
					total_development_queue += 1
					total_allocatedMB_dev += i['allocatedMB']
				#
				# Add additional YARN application types as necessary
				#				
				applicationType = i['applicationType']
				if applicationType == 'MAPREDUCE':
					total_mr_apps += 1
				elif applicationType == 'TEZ':
					total_tez_apps += 1
				elif applicationType == 'SPARK':	
					total_spark_apps += 1	
				# total_allocatedMB
				total_allocatedMB += i['allocatedMB']
				# total_allocatedVCores
				total_allocatedVCores += i['allocatedVCores']
				# total_runningContainers
				total_runningContainers += i['runningContainers']
			
			total_allocatedTB = (float(total_allocatedMB)/1024)/1024
			
			#
			# Post metrics
			#
			
			# yarn.apps.running.TOTAL
			total_apps = len(apps_json_obj['apps']['app'])
			self.gauge(
				metric='yarn.apps.running.TOTAL', 
				value=total_apps, 
				tags=[self.event_type, 'apptype:TOTAL'], 
				hostname=rmhost)
			
			# yarn.apps.running.INTERACTIVE, yarn.apps.running.BATCH	
			self.gauge(
				metric='yarn.apps.running', 
				value=total_interactive_apps, 
				tags=[self.event_type, 'apptype:INTERACTIVE', 'submittype:INTERACTIVE'], 
				hostname=rmhost)				
			self.gauge(
				metric='yarn.apps.running', 
				value=total_batch_apps, 
				tags=[self.event_type, 'apptype:BATCH', 'submittype:BATCH'], 
				hostname=rmhost)
					
			# yarn.apps.running.queue
			self.gauge(
				metric='yarn.apps.running.queue', 
				value=total_default_queue, 
				tags=[self.event_type, 'queuename:default'], 
				hostname=rmhost)			
			self.gauge(
				metric='yarn.apps.running.queue', 
				value=total_prod_queue, 
				tags=[self.event_type, 'queuename:production'], 
				hostname=rmhost)
			self.gauge(
				metric='yarn.apps.running.queue', 
				value=total_prodtact_queue, 
				tags=[self.event_type, 'queuename:prodtactical'], 
				hostname=rmhost)				
			self.gauge(
				metric='yarn.apps.running.queue', 
				value=total_development_queue, 
				tags=[self.event_type, 'queuename:development'], 
				hostname=rmhost)					

			# yarn.apps.running.MR
			self.gauge(
				metric='yarn.apps.running.MR', 
				value=total_mr_apps, 
				tags=[self.event_type, 'apptype:MR'], 
				hostname=rmhost)				

			# yarn.apps.running.TEZ
			self.gauge(
				metric='yarn.apps.running.TEZ', 
				value=total_tez_apps, 
				tags=[self.event_type, 'apptype:TEZ'], 
				hostname=rmhost)					

			# yarn.apps.running.SPARK
			self.gauge(
				metric='yarn.apps.running.SPARK', 
				value=total_spark_apps, 
				tags=[self.event_type, 'apptype:SPARK'], 
				hostname=rmhost)

			# yarn.apps.running.allocatedTB
			self.gauge(
				metric='yarn.apps.running.allocatedTB', 
				value=total_allocatedTB, 
				tags=[self.event_type, 'appmetric:allocatedTB'], 
				hostname=rmhost)

			# yarn.apps.running.allocatedGB.queue
			self.gauge(
				metric='yarn.apps.running.allocatedGB.queue', 
				value=float(total_allocatedMB_def)/1024, 
				tags=[self.event_type, 'queuename:default'], 
				hostname=rmhost)
			self.gauge(
				metric='yarn.apps.running.allocatedGB.queue', 
				value=float(total_allocatedMB_prod)/1024, 
				tags=[self.event_type, 'queuename:production'], 
				hostname=rmhost)
			self.gauge(
				metric='yarn.apps.running.allocatedGB.queue', 
				value=float(total_allocatedMB_prodtact)/1024, 
				tags=[self.event_type, 'queuename:prodtactical'], 
				hostname=rmhost)
			self.gauge(
				metric='yarn.apps.running.allocatedGB.queue', 
				value=float(total_allocatedMB_dev)/1024, 
				tags=[self.event_type, 'queuename:development'], 
				hostname=rmhost)

			# yarn.apps.running.allocatedVCores
			self.gauge(
				metric='yarn.apps.running.allocatedVCores', 
				value=total_allocatedVCores, 
				tags=[self.event_type, 'appmetric:allocatedVCores'], 
				hostname=rmhost)

			# yarn.apps.running.runningContainers
			self.gauge(
				metric='yarn.apps.running.runningContainers', 
				value=total_runningContainers, 
				tags=[self.event_type, 'appmetric:runningContainers'], 
				hostname=rmhost)

		except HTTPError, e:
			err_msg = 'HTTPError %s Returned From \'%s\'' % (e.code, rmhost)
			self.yarn_error_event('HTTPError', err_msg)
		except URLError, e:
			err_msg = 'URLError %s Returned From \'%s\'' % (e.reason, rmhost)
			self.yarn_error_event('URLError', err_msg)

	def yarn_error_event(self, title, err_msg):
		self.event({
          'timestamp': int(time.time()),
          'event_type': self.event_type,
          'alert_type': 'error',
          'msg_title': title,
          'msg_text': err_msg
        })

################ TEST HOOK ################
if __name__ == '__main__':

	check, instances = YARNMetrics.from_yaml('/etc/dd-agent/conf.d/collect_yarn_stats.yaml')
	for instance in instances:
		print "\nCollecting YARN metrics : resourcemanager_uri => %s" % (instance['resourcemanager_uri'])
		check.agentConfig = {
			'api_key': 'dummy_key'
		}
		check.check(instance)
		if check.has_events():
			print 'Events: %s' % (check.get_events())
		print 'Metrics: %s' % (check.get_metrics())
