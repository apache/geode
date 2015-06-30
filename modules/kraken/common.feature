@flavor=gemfire-p2p @webapp_jar_mode=
Feature: Testing with GemFire peer-to-peer

  @wip
  Scenario: working webapp
    Given "basic" webapp is deployed
     Then "basic" webapp test url works

  Scenario: working JSON service
    Given "basic" webapp is deployed
     Then json contains "gemfire_modules_sessions" at url path "/json/regions"
     
  Scenario: working caching
    Given "basic" webapp is deployed
     Then basic caching works

  Scenario: basic cache failover
    Given "basic" webapp is deployed
     Then basic cache failover works

  Scenario: static webapp reloading works
    Given "basic" webapp is deployed
     Then webapp reloading works

  Scenario: webapp reloading works with active sessions
    Given "basic" webapp is deployed
     Then webapp reloading works with active sessions