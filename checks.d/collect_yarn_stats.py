from checks import AgentCheck

class YARNCheck(AgentCheck):
    event_type = 'yarn_check'

    def check(self, instance):
        default_timeout = self.init_config.get('default_timeout', 8)
        timeout = float(instance.get('timeout', default_timeout)) 

        default_query_type = self.init_config.get('default_query_type', 'A')
        query_type = instance.get('query_type', default_query_type)

        nameserver = instance.get('nameserver')
        query = instance.get('query')
        expected_answer = instance.get('answer') 
        
        try:
            resolv = dns.resolver.Resolver()
            resolv.timeout = timeout
            resolv.lifetime = timeout
            resolv.nameservers = [ nameserver ]

            start_tm = time.time()*1000
            answer = resolv.query(query, query_type)
            end_tm = time.time()*1000
            query_time = end_tm - start_tm
            self.gauge('dns.query_time', query_time, tags=['dns_check', 'nameserver:%s' % nameserver])
            actual_answer = answer[0].address 
            if actual_answer != expected_answer :
                err_msg = 'lookup of \'%s\' expected answer \'%s\', received \'%s\'' % (query, expected_answer, actual_answer)
                self.answer_mismatch_event(nameserver, err_msg, aggregation_key)
                        
        except dns.resolver.Timeout:
            err_msg = 'lookup for \'%s\' timed out in %s ms' % (query, timeout)
            self.timeout_event(nameserver, err_msg, aggregation_key)
        except dns.exception.DNSException as e:
            err_msg = 'lookup failed with exception %s' % (type(e).__name__)
            self.dns_fail_event(nameserver, err_msg, aggregation_key)

        end_tm = time.time()*1000
        query_time = end_tm - start_tm

    def timeout_event(self, nameserver, error, aggregation_key):
        self.event({
          'timestamp': int(time.time()),
          'event_type': self.event_type,
          'alert_type': 'error',
          'msg_title': 'DNS lookup timeout',
          'msg_text': 'using nameserver %s: %s' % (nameserver, error),
          'aggregation_key': aggregation_key
        })

    def dns_fail_event(self, nameserver, error, aggregation_key):
        self.event({
          'timestamp': int(time.time()),
          'event_type': self.event_type,
          'alert_type': 'error',
          'msg_title': 'DNS lookup error',
          'msg_text': 'using nameserver %s: %s' % (nameserver, error),
          'aggregation_key': aggregation_key
        })

    def answer_mismatch_event(self, nameserver, error, aggregation_key):
        self.event({
          'timestamp': int(time.time()),
          'event_type': self.event_type,
          'alert_type': 'warn',
          'msg_title': 'DNS answer mismatch error',
          'msg_text': 'using nameserver %s: %s' % (nameserver, error),
          'aggregation_key': aggregation_key
        })



################ TEST HOOK ################
if __name__ == '__main__':

    check, instances = DNSCheck.from_yaml('/etc/dd-agent/conf.d/dns_check.yaml')
    for instance in instances:
        print "\nRunning check: nameserver => %s, query => %s" % (instance['nameserver'], instance['query'])
        check.agentConfig = {
            'api_key': 'dummy_key'
        }
        check.check(instance)
        if check.has_events():
            print 'Events: %s' % (check.get_events())
        print 'Metrics: %s' % (check.get_metrics())
