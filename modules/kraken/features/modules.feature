# control tags available which need to be passed in as an environment
# variable - FEATURE_TAGS. For example
#   FEATURE_TAGS="appserver=jboss7as flavor=gemfire-cs"
#
# Required tags:
#   appserver=[tcserver_native | tcserver_appserver | jboss7as | jboss6]
#   flavor=[gemfire-cs | gemfire-p2p]
#
# Optional tags:
#   webapp_jar_mode=split
#       This option is relevant for all appserver definitions except tcserver_native.
#       When set, it will deploy the webapp with a minimal set of jars and place the
#       other, required, jars into the appserver's system level classpath. How that is
#       done is appserver dependent.
#
#   setup=[true | false] (default is true)
#       When set, performs initial feature-level startup actions.
#
#   teardown=[true | false] (default is true)
#       When set, performs feature-level teardown actions.
#
# All test combos would be achieved by the product of 'appserver * flavor * webapp_jar_mode'.

Feature: Testing GemFire Session Replication Modules

  @sanity
  Scenario: working webapp
      Given application server is started
        And "basic" webapp is deployed
       Then "basic" webapp test url works

  @wip
  Scenario: maxInactiveInterval is propagated
      Given application server is started
        And "basic" webapp is deployed
       Then setting maxInactiveInterval to "200" works

  @sanity
  Scenario: working JSON service
      Given application server is started
        And "basic" webapp is deployed
       Then json contains "gemfire_modules_sessions" at url path "/json/regions"

  @sanity
  Scenario: working caching
      Given application server is started
        And "basic" webapp is deployed
       Then basic caching works

  @sanity
  Scenario: basic cache failover
      Given application server is started
        And "basic" webapp is deployed
       Then basic cache failover works

  @redeploy
  Scenario: static webapp reloading works
      Given application server is set up for redeployment
        And application server is started
        And "basic" webapp is deployed
       Then webapp reloading works

  @redeploy
  Scenario: webapp reloading works with active sessions
      Given application server is set up for redeployment
        And application server is started
        And "basic" webapp is deployed
       Then webapp reloading works with active sessions

  Scenario: use custom region name
      Given gemfire configuration is using region path "sessions-app1" and xml template id "custom_region"
        And application server is started
        And "basic" webapp is deployed
       Then json contains "sessions-app1" at url path "/json/regions"
        And json should not contain "gemfire_modules_sessions" at url path "/json/regions"
        And basic caching works
        And restore customized region

  Scenario: enabling debug cache listener works
      Given debug cache listener is enabled
        And application server is started
        And "basic" webapp is deployed
       Then json contains "gemfire_modules_sessions" at url path "/json/regions"
        And json should not contain "sessions-app1" at url path "/json/regions"
        And basic caching works

  Scenario: use locator from client
      Given client is configured for locator using region path "gemfire_modules_sessions" and xml template id "custom_port"
        And cache server is restarted
        And application server is started
        And "basic" webapp is deployed
       Then basic caching works      
        And restore customized region

  Scenario: non-sticky sessions work
      Given client cache is configured for "PROXY"
        And cache server is restarted
        And application server is started
        And "basic" webapp is deployed
       Then non-sticky sessions work
