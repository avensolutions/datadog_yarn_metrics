from checks import AgentCheck
from urllib2 import urlopen, URLError, HTTPError
import json, re, time, urllib2
from itertools import groupby

class YARNMetrics(AgentCheck):
	"""Collect metrics on applications running in YARN via the RM REST API
	https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Applications_API
	User regex and queue names are specific to your environment and should be updated in the check method of this class
	
	Datadog metrics:
		yarn.apps.queued								(Desc: COUNT of ALL queued applications, Tags:	None)
		yarn.apps.failed								(Desc: COUNT of ALL failed applications (evaluated hourly), Tags:	None)
		yarn.apps.failed.byQueue						(Desc: COUNT of failed applications by Queue (evaluated hourly), Tags:	queue:{default, production, etc})
		yarn.apps.failed.byAppType						(Desc: COUNT of failed applications by AppType (evaluated hourly), Tags: apptype:{MR, TEZ, SPARK, etc})
		yarn.apps.failed.byUser							(Desc: COUNT of failed applications by User (evaluated hourly), Tags: user:{u12345, etc}
		yarn.apps.succeeded								(Desc: COUNT of ALL succeeded applications (evaluated hourly), Tags:	None)
		yarn.apps.succeeded.byQueue						(Desc: COUNT of succeeded applications by Queue (evaluated hourly), Tags:	queue:{default, production, etc})
		yarn.apps.succeeded.byAppType					(Desc: COUNT of succeeded applications by AppType (evaluated hourly), Tags: apptype:{MR, TEZ, SPARK, etc})
		yarn.apps.succeeded.byUser						(Desc: COUNT of succeeded applications by User (evaluated hourly), Tags: user:{u12345, etc}
		yarn.apps.killed								(Desc: COUNT of ALL killed applications (evaluated hourly), Tags:	None)
		yarn.apps.killed.byQueue						(Desc: COUNT of killed applications by Queue (evaluated hourly), Tags:	queue:{default, production, etc})
		yarn.apps.killed.byAppType						(Desc: COUNT of killed applications by AppType (evaluated hourly), Tags: apptype:{MR, TEZ, SPARK, etc})
		yarn.apps.killed.byUser							(Desc: COUNT of killed applications by User (evaluated hourly), Tags: user:{u12345, etc}		
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

	"""
	
	event_type = 'yarn_metrics_collection'
	basetags = [event_type]
	
	def get_num_apps(self, rm_uri, state):
		num_apps_url = "http://" + rm_uri + "/ws/v1/cluster/apps?state=" + state
		num_apps_resp = urllib2.urlopen(num_apps_url)
		num_apps_json_obj = json.load(num_apps_resp)
		no_apps = 0
		if num_apps_json_obj['apps'] is not None:
			if num_apps_json_obj['apps']['app'] is not None:
				no_apps = len(num_apps_json_obj['apps']['app'])
		return no_apps

	def setmetric(self, metricname, metricvalue, metrictags, host):
		tags = list(self.basetags)
		tags.extend(metrictags)
		self.gauge(
			metric=metricname, 
			value=metricvalue, 
			tags=tags, 
			hostname=host)
		
	def metricsbycontext(self, context, context_sorted_list, host):
		if context == 'queue':
			metric_suffix = 'byQueue'
		elif context == 'apptype':
			metric_suffix = 'byAppType'
		for key, group in groupby(context_sorted_list, lambda x: x[0]):
			totmem = 0
			totcores = 0
			totcontainers = 0
			totmemsecs = 0
			totvcoresecs = 0
			count = 0
			for groupitm in group:
				count += 1
				totmem += groupitm[2]
				totcores += groupitm[3]
				totcontainers += groupitm[4]
				totmemsecs += groupitm[5]
				totvcoresecs += groupitm[6]
			# context, numrunningapps, totalmem, totalcores, totalcontainers, totmemsecs, totvcoresecs
			# context
			self.setmetric('yarn.apps.running.' + context, count, [context + ":" + key], host)
			# allocatedGB
			self.setmetric('yarn.apps.running.allocatedGB.' + metric_suffix, totmem/1000, [context + ":" + key], host)
			# allocatedVCores
			self.setmetric('yarn.apps.running.allocatedVCores.' + metric_suffix, totcores, [context + ":" + key], host)
			# runningContainers
			self.setmetric('yarn.apps.running.runningContainers.' + metric_suffix, totcontainers, [context + ":" + key], host)
			# totalmemoryseconds
			self.setmetric('yarn.apps.running.totalMemorySeconds.' + metric_suffix, totmemsecs, [context + ":" + key], host)
			# totalvcoreseconds
			self.setmetric('yarn.apps.running.totalVCoreSeconds.' + metric_suffix, totvcoresecs, [context + ":" + key], host)	

	#SUCCEEDED, FAILED, KILLED
	def comp_apps_count(self, rm, final_status, last_hour_ms_in):
		final_status_uc = final_status.upper() 
		rmhost = rm.split(":")[0]
		comp_apps_queues_list = []
		comp_apps_apptypes_list = []
		comp_apps_user_list = []
		comp_apps_url = "http://" + rm + "/ws/v1/cluster/apps?finalStatus=" + final_status_uc	 
		comp_apps_resp = urllib2.urlopen(comp_apps_url)
		comp_apps_json_obj = json.load(comp_apps_resp)
		if comp_apps_json_obj['apps'] is not None:
			if comp_apps_json_obj['apps']['app'] is not None:
				for i in comp_apps_json_obj['apps']['app']:
					finishedTime = i['finishedTime']
					if finishedTime >= last_hour_ms_in: 
						user = i['user']
						queue = i['queue']
						applicationType = i['applicationType']
						comp_apps_queues_list.append(queue)
						comp_apps_apptypes_list.append(applicationType)
						comp_apps_user_list.append(user)

				comp_apps_list = zip(comp_apps_queues_list, comp_apps_apptypes_list, comp_apps_user_list)

				no_comp_apps = len(comp_apps_list)
				#self.gauge('yarn.apps.' + final_status, no_comp_apps, timestamp=last_hour_ms_in, tags=None, hostname=rmhost)
				#self.gauge(metric='yarn.apps.' + final_status, value=no_comp_apps, tags=None, hostname=rmhost)
				self.setmetric('yarn.apps.' + final_status, no_comp_apps, [], rmhost)
				
				# yarn.apps.failed.byQueue
				comp_sorted_by_queue = sorted(comp_apps_list, key=lambda tup: tup[0])
				for key, group in groupby(comp_sorted_by_queue, lambda x: x[0]):
					count = 0
					for groupitm in group:
						count += 1	
					#self.gauge('yarn.apps.' + final_status + '.byQueue', count, timestamp=last_hour_ms_in, tags=["queue:" + key], hostname=rmhost)	
					#self.gauge(metric='yarn.apps.' + final_status + '.byQueue', value=count, tags=["queue:" + key], hostname=rmhost)	
					self.setmetric('yarn.apps.' + final_status, count, ["queue:" + key], rmhost)

				# yarn.apps.failed.byAppType
				comp_sorted_by_apptype = sorted(comp_apps_list, key=lambda tup: tup[1])
				for key, group in groupby(comp_sorted_by_apptype, lambda x: x[1]):
					count = 0
					for groupitm in group:
						count += 1				
					#self.gauge('yarn.apps.' + final_status + '.byAppType', count, timestamp=last_hour_ms_in, tags=["apptype:" + key], hostname=rmhost)		
					#self.gauge(metric='yarn.apps.' + final_status + '.byAppType', value=count, tags=["apptype:" + key], hostname=rmhost)		
					self.setmetric('yarn.apps.' + final_status, count, ["apptype:" + key], rmhost)
					
				# yarn.apps.failed.byUser	
				comp_sorted_by_user = sorted(comp_apps_list, key=lambda tup: tup[2])
				for key, group in groupby(comp_sorted_by_user, lambda x: x[2]):
					count = 0
					for groupitm in group:
						count += 1				
					#self.gauge('yarn.apps.' + final_status + '.byUser', count, timestamp=last_hour_ms_in, tags=["user:" + key], hostname=rmhost)
					#self.gauge(metric='yarn.apps.' + final_status + '.byUser', value=count, tags=["user:" + key], hostname=rmhost)
					self.setmetric('yarn.apps.' + final_status, count, ["user:" + key], rmhost)

	def check(self, instance):

		resourcemanager_uri = instance.get('resourcemanager_uri', None)
		
		if resourcemanager_uri is None:
			raise Exception("resourcemanager_uri must be specified")

		# update this for your environment to distinguish user submitted queries from batch queries, example u123456 may represent a human user
		user_pattern = self.init_config.get('user_pattern', '.*')
		user_pattern_regex = re.compile(user_pattern)
			
		try:
		
			# Get running apps		
			apps_url = "http://" + resourcemanager_uri + "/ws/v1/cluster/apps?state=RUNNING"
			rmhost = resourcemanager_uri.split(":")[0]
			apps_resp = urllib2.urlopen(apps_url)
			apps_json_obj = json.load(apps_resp)
			
			# Get queued apps
			queued_apps = self.get_num_apps(resourcemanager_uri, 'NEW') + self.get_num_apps(resourcemanager_uri, 'NEW_SAVING') + self.get_num_apps(resourcemanager_uri, 'SUBMITTED') + self.get_num_apps(resourcemanager_uri, 'ACCEPTED')
			self.setmetric('yarn.apps.queued', queued_apps, [], rmhost)
			
			# Get failed apps
			t0 = time.time()
			tm = time.localtime(t0)
			now = int(t0) * 1000
			last_hour_ms = (int(t0) * 1000) - 3600000 
			if (tm[4] == 00):
				self.comp_apps_count(resourcemanager_uri, 'failed', last_hour_ms)		
				self.comp_apps_count(resourcemanager_uri, 'succeeded', last_hour_ms)		
				self.comp_apps_count(resourcemanager_uri, 'killed', last_hour_ms)	
					
			# iterate through running apps
			total_interactive_apps = 0
			total_batch_apps = 0
			queues_list = []
			apptypes_list = []
			elapsedTime_list = []
			allocatedMB_list = []
			allocatedVCores_list = []
			runningContainers_list = []
			memorySeconds_list = []
			vcoreSeconds_list = []
			for i in apps_json_obj['apps']['app']:
				user = i['user']
				if user_pattern_regex.match(user):
					total_interactive_apps += 1
				else:
					total_batch_apps += 1
				applicationType = i['applicationType']
				queue = i['queue']
				if i['name'] == 'Spark shell':
					elapsedTime = 0
				else:	
					elapsedTime = i['elapsedTime']
				allocatedMB = i['allocatedMB']
				allocatedVCores = i['allocatedVCores']
				runningContainers = i['runningContainers']
				memorySeconds = i['memorySeconds']
				vcoreSeconds = i['vcoreSeconds']
				apptypes_list.append(applicationType)
				queues_list.append(queue)
				elapsedTime_list.append(elapsedTime)
				allocatedMB_list.append(allocatedMB)
				allocatedVCores_list.append(allocatedVCores)
				runningContainers_list.append(runningContainers)
				memorySeconds_list.append(memorySeconds)
				vcoreSeconds_list.append(vcoreSeconds)
			queues_zipped_list = zip(queues_list, elapsedTime_list, allocatedMB_list, allocatedVCores_list, runningContainers_list, memorySeconds_list, vcoreSeconds_list)
			apptypes_zipped_list = zip(apptypes_list, elapsedTime_list, allocatedMB_list, allocatedVCores_list, runningContainers_list, memorySeconds_list, vcoreSeconds_list)
			#
			# System wide metrics
			#
			
			# [yarn.apps.running] 
			total_apps = len(apps_json_obj['apps']['app'])
			self.setmetric('yarn.apps.running', total_apps, [], rmhost)

			# [yarn.apps.running.submittype]
			self.setmetric('yarn.apps.running.submittype', total_batch_apps, ["submittype:BATCH"], rmhost)
			self.setmetric('yarn.apps.running.submittype', total_interactive_apps, ["submittype:INTERACTIVE"], rmhost)

			# [yarn.apps.running.allocatedGB]
			allocatedGB = sum(l[2] for l in queues_zipped_list)/1000
			self.setmetric('yarn.apps.running.allocatedGB', allocatedGB, [], rmhost)

			# [yarn.apps.running.allocatedVCores]
			allocatedVCores = sum(l[3] for l in queues_zipped_list)
			self.setmetric('yarn.apps.running.allocatedVCores', allocatedVCores, [], rmhost)

			# [yarn.apps.running.runningContainers]
			runningContainers = sum(l[4] for l in queues_zipped_list)
			self.setmetric('yarn.apps.running.runningContainers', runningContainers, [], rmhost)

			# [yarn.apps.running.maxElapsedTime]
			maxelapsedtime = max(l[1] for l in queues_zipped_list)
			self.setmetric('yarn.apps.running.maxElapsedTime', maxelapsedtime, [], rmhost)

			# [yarn.apps.running.maxAllocatedGB]
			maxallocatedGB = max(l[2] for l in queues_zipped_list)/1000
			self.setmetric('yarn.apps.running.maxAllocatedGB', maxallocatedGB, [], rmhost)

			# [yarn.apps.running.maxAllocatedVCores]
			maxallocatedVCores = max(l[3] for l in queues_zipped_list)
			self.setmetric('yarn.apps.running.maxAllocatedVCores', maxallocatedVCores, [], rmhost)

			# [yarn.apps.running.maxRunningContainers]
			maxcontainers = max(l[4] for l in queues_zipped_list)
			self.setmetric('yarn.apps.running.maxRunningContainers', maxcontainers, [], rmhost)

			# [yarn.apps.running.maxMemorySeconds]
			maxmemoryseconds = max(l[5] for l in queues_zipped_list)
			self.setmetric('yarn.apps.running.maxMemorySeconds', maxmemoryseconds, [], rmhost)

			# [yarn.apps.running.maxVCoreSeconds]
			maxvcoreseconds = max(l[5] for l in queues_zipped_list)
			self.setmetric('yarn.apps.running.maxVCoreSeconds', maxvcoreseconds, [], rmhost)

			#
			# by queue
			#
			sorted_by_queue = sorted(queues_zipped_list, key=lambda tup: tup[0])
			self.metricsbycontext('queue', sorted_by_queue, rmhost)

			#
			# by apptype
			#

			sorted_by_apptype = sorted(apptypes_zipped_list, key=lambda tup: tup[0])
			self.metricsbycontext('apptype', sorted_by_apptype, rmhost)			

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
