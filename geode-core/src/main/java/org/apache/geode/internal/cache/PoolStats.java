/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache;

import org.apache.geode.*;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;

/**
 * GemFire statistics about a Pool 
 *
 *
 * @since GemFire 5.7
 */
public class PoolStats {

  private static final StatisticsType _type;

  ////////////////////  Statistic "Id" Fields  ////////////////////

  private static final String INITIAL_CONTACTS = "initialContactCount"; // gauge
  private static final String KNOWN_LOCATORS = LOCATORS; // gauge
  private static final String REQUESTS_TO_LOCATOR = "locatorRequests"; // counter
  private static final String RESPONSES_FROM_LOCATOR = "locatorResponses"; // counter
  private static final String ENDPOINTS_KNOWN = "servers"; // gauge
  private static final String SUBSCRIPTION_SERVERS = "subscriptionServers"; // gauge
  
  private static final int _INITIAL_CONTACTS;
  private static final int _KNOWN_LOCATORS;
  private static final int _REQUESTS_TO_LOCATOR;
  private static final int _RESPONSES_FROM_LOCATOR;
  private static final int _ENDPOINTS_KNOWN;
  private static final int _SUBSCRIPTION_SERVERS;
  private static final int _PREFILL_CONNECT;
  private static final int _LOAD_CONDITIONING_CHECK;
  private static final int _LOAD_CONDITIONING_EXTENSIONS;
  private static final int _IDLE_CHECK;
  private static final int _LOAD_CONDITIONING_CONNECT;
  private static final int _LOAD_CONDITIONING_DISCONNECT;
  private static final int _LOAD_CONDITIONING_REPLACE_TIMEOUT;
  private static final int _IDLE_EXPIRE;
  private static final int _CONNECTION_WAIT_IN_PROGRESS;
  private static final int _CONNECTION_WAITS;
  private static final int _CONNECTION_WAIT_TIME;
  private final static int connectionsId;
//   private final static int conCountId;
  private final static int poolConnectionsId;
  private final static int connectsId;
  private final static int disconnectsId;
  private final static int clientOpInProgressId;
  private final static int clientOpSendInProgressId;
  private final static int clientOpSendId;
  private final static int clientOpSendFailedId;
  private final static int clientOpSendDurationId;
  private final static int clientOpId;
  private final static int clientOpTimedOutId;
  private final static int clientOpFailedId;
  private final static int clientOpDurationId;

  static {
    String statName = "PoolStats";

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    _type = f.createType(statName, statName,
       new StatisticDescriptor[] {
            f.createIntGauge(INITIAL_CONTACTS, "Number of contacts initially by user", "contacts"),
           f.createIntGauge(KNOWN_LOCATORS, "Current number of locators discovered", LOCATORS),
            f.createIntGauge(ENDPOINTS_KNOWN, "Current number of servers discovered", "servers"),
            f.createIntGauge(SUBSCRIPTION_SERVERS, "Number of servers hosting this clients subscriptions", "servers"),
            f.createLongCounter(REQUESTS_TO_LOCATOR, "Number of requests from this connection pool to a locator", "requests"),
            f.createLongCounter(RESPONSES_FROM_LOCATOR, "Number of responses from the locator to this connection pool", "responses"),

            f.createIntGauge("connections", "Current number of connections", "connections"),
//             f.createIntGauge("conCount", "Current number of connections", "connections"),
            f.createIntGauge("poolConnections", "Current number of pool connections", "connections"),
            f.createIntCounter("connects", "Total number of times a connection has been created.", "connects"),
            f.createIntCounter("disconnects", "Total number of times a connection has been destroyed.", "disconnects"),
            f.createIntCounter("minPoolSizeConnects", "Total number of connects done to maintain minimum pool size.", "connects"),
            f.createIntCounter("loadConditioningConnects", "Total number of connects done due to load conditioning.", "connects"),
            f.createIntCounter("loadConditioningReplaceTimeouts", "Total number of times a load conditioning connect was done but was not used.", "timeouts"),
            f.createIntCounter("idleDisconnects", "Total number of disconnects done due to idle expiration.", "disconnects"),
            f.createIntCounter("loadConditioningDisconnects", "Total number of disconnects done due to load conditioning expiration.", "disconnects"),
            f.createIntCounter("idleChecks", "Total number of checks done for idle expiration.", "checks"),
            f.createIntCounter("loadConditioningChecks", "Total number of checks done for load conditioning expiration.", "checks"),
            f.createIntCounter("loadConditioningExtensions", "Total number of times a connection's load conditioning has been extended because the servers are still balanced.", "extensions"),
            f.createIntGauge("connectionWaitsInProgress", "Current number of threads waiting for a connection", "threads"),
            f.createIntCounter("connectionWaits", "Total number of times a thread completed waiting for a connection (by timing out or by getting a connection).", "waits"),
            f.createLongCounter("connectionWaitTime", "Total number of nanoseconds spent waiting for a connection.", "nanoseconds"),
            f.createIntGauge("clientOpsInProgress", "Current number of clientOps being executed", "clientOps"), 
            f.createIntGauge("clientOpSendsInProgress", "Current number of clientOp sends being executed", "sends"), 
            f.createIntCounter("clientOpSends", "Total number of clientOp sends that have completed successfully", "sends"), 
            f.createIntCounter("clientOpSendFailures", "Total number of clientOp sends that have failed", "sends"), 
            f.createIntCounter("clientOps", "Total number of clientOps completed successfully", "clientOps"), 
            f.createIntCounter("clientOpFailures", "Total number of clientOp attempts that have failed", "clientOps"), 
            f.createIntCounter("clientOpTimeouts", "Total number of clientOp attempts that have timed out", "clientOps"), 
            f.createLongCounter("clientOpSendTime", "Total amount of time, in nanoseconds spent doing clientOp sends", "nanoseconds"), 
            f.createLongCounter("clientOpTime", "Total amount of time, in nanoseconds spent doing clientOps", "nanoseconds"),
       });

    // Initialize id fields
    _INITIAL_CONTACTS = _type.nameToId(INITIAL_CONTACTS);
    _KNOWN_LOCATORS = _type.nameToId(KNOWN_LOCATORS);
    _REQUESTS_TO_LOCATOR = _type.nameToId(REQUESTS_TO_LOCATOR);
    _RESPONSES_FROM_LOCATOR = _type.nameToId(RESPONSES_FROM_LOCATOR);
    _ENDPOINTS_KNOWN = _type.nameToId(ENDPOINTS_KNOWN);
    _SUBSCRIPTION_SERVERS = _type.nameToId(SUBSCRIPTION_SERVERS);
    _PREFILL_CONNECT = _type.nameToId("minPoolSizeConnects");
    _LOAD_CONDITIONING_CHECK = _type.nameToId("loadConditioningChecks");
    _LOAD_CONDITIONING_EXTENSIONS = _type.nameToId("loadConditioningExtensions");
    _IDLE_CHECK = _type.nameToId("idleChecks");
    _LOAD_CONDITIONING_CONNECT = _type.nameToId("loadConditioningConnects");
    _LOAD_CONDITIONING_REPLACE_TIMEOUT = _type.nameToId("loadConditioningReplaceTimeouts");
    _LOAD_CONDITIONING_DISCONNECT = _type.nameToId("loadConditioningDisconnects");
    _IDLE_EXPIRE = _type.nameToId("idleDisconnects");
    _CONNECTION_WAIT_IN_PROGRESS = _type.nameToId("connectionWaitsInProgress");
    _CONNECTION_WAITS = _type.nameToId("connectionWaits");
    _CONNECTION_WAIT_TIME = _type.nameToId("connectionWaitTime");
    
    connectionsId = _type.nameToId("connections");
//     conCountId = _type.nameToId("conCount");
    poolConnectionsId = _type.nameToId("poolConnections");
    connectsId = _type.nameToId("connects");
    disconnectsId = _type.nameToId("disconnects");

    clientOpInProgressId = _type.nameToId("clientOpsInProgress");
    clientOpSendInProgressId = _type.nameToId("clientOpSendsInProgress");
    clientOpSendId = _type.nameToId("clientOpSends");
    clientOpSendFailedId = _type.nameToId("clientOpSendFailures");
    clientOpSendDurationId = _type.nameToId("clientOpSendTime");
    clientOpId = _type.nameToId("clientOps");
    clientOpTimedOutId = _type.nameToId("clientOpTimeouts");
    clientOpFailedId = _type.nameToId("clientOpFailures");
    clientOpDurationId = _type.nameToId("clientOpTime");
  }

  //////////////////////  Instance Fields  //////////////////////

  /** The Statistics object that we delegate most behavior to */
  private final Statistics _stats;

  ///////////////////////  Constructors  ///////////////////////

  public PoolStats(StatisticsFactory f, String name) {
    this._stats = f.createAtomicStatistics(_type, name);
  }

  /////////////////////  Instance Methods  /////////////////////

  public void close() {
    this._stats.close();
  }

  public long startTime()
  {
    return DistributionStats.getStatTime();
  }

  public final void setInitialContacts(int ic) {
    this._stats.setInt(_INITIAL_CONTACTS, ic);
  }

  public final void setServerCount(int sc) {
    this._stats.setInt(_ENDPOINTS_KNOWN, sc);
  }
  
  public final void setSubscriptionCount(int qc) {
    this._stats.setInt(_SUBSCRIPTION_SERVERS, qc);
  }

  public final void setLocatorCount(int lc) {
    this._stats.setInt(_KNOWN_LOCATORS, lc);
  }

  public final long getLocatorRequests() {
    return this._stats.getLong(_REQUESTS_TO_LOCATOR);
  }
  public final void incLocatorRequests() {
    this._stats.incLong(_REQUESTS_TO_LOCATOR, 1);
  }  
  
  public final void incLocatorResponses() {
    this._stats.incLong(_RESPONSES_FROM_LOCATOR, 1);
  }  
  
  public final void setLocatorRequests(long rl) {
    this._stats.setLong(_REQUESTS_TO_LOCATOR,rl);
  }
  
  public final void setLocatorResponses(long rl) {
    this._stats.setLong(_RESPONSES_FROM_LOCATOR,rl);
  }

//   public void incConCount(int delta) {
//     this._stats.incInt(conCountId, delta);
//   }
  public void incConnections(int delta) {
    this._stats.incInt(connectionsId, delta);
    if (delta > 0) {
      this._stats.incInt(connectsId, delta);
    } else if (delta < 0) {
      this._stats.incInt(disconnectsId, -delta);
    }
  }
  public void incPoolConnections(int delta) {
    this._stats.incInt(poolConnectionsId, delta);
  }
  public int getPoolConnections() {
    return this._stats.getInt(poolConnectionsId);
  }
  public int getConnects() {
    return this._stats.getInt(connectsId);
  }
  public int getDisConnects() {
    return this._stats.getInt(disconnectsId);
  }
  private static long getStatTime() {
    return DistributionStats.getStatTime();
  }
  public void incPrefillConnect() {
    this._stats.incInt(_PREFILL_CONNECT, 1);
  }
  public int getLoadConditioningCheck() {
    return this._stats.getInt(_LOAD_CONDITIONING_CHECK);
  }
  public void incLoadConditioningCheck() {
    this._stats.incInt(_LOAD_CONDITIONING_CHECK, 1);
  }
  public int getLoadConditioningExtensions() {
    return this._stats.getInt(_LOAD_CONDITIONING_EXTENSIONS);
  }
  public void incLoadConditioningExtensions() {
    this._stats.incInt(_LOAD_CONDITIONING_EXTENSIONS, 1);
  }
  public void incIdleCheck() {
    this._stats.incInt(_IDLE_CHECK, 1);
  }
  public int getLoadConditioningConnect() {
    return this._stats.getInt(_LOAD_CONDITIONING_CONNECT);
  }
  public void incLoadConditioningConnect() {
    this._stats.incInt(_LOAD_CONDITIONING_CONNECT, 1);
  }
  public int getLoadConditioningReplaceTimeouts() {
    return this._stats.getInt(_LOAD_CONDITIONING_REPLACE_TIMEOUT);
  }
  public void incLoadConditioningReplaceTimeouts() {
    this._stats.incInt(_LOAD_CONDITIONING_REPLACE_TIMEOUT, 1);
  }
  public int getLoadConditioningDisconnect() {
    return this._stats.getInt(_LOAD_CONDITIONING_DISCONNECT);
  }
  public void incLoadConditioningDisconnect() {
    this._stats.incInt(_LOAD_CONDITIONING_DISCONNECT, 1);
  }
  public int getIdleExpire() {
    return this._stats.getInt(_IDLE_EXPIRE);
  }
  public void incIdleExpire(int delta) {
    this._stats.incInt(_IDLE_EXPIRE, delta);
  }
  public long beginConnectionWait() {
    this._stats.incInt(_CONNECTION_WAIT_IN_PROGRESS, 1);
    return getStatTime();
  }
  public void endConnectionWait(long start) {
    long duration = getStatTime() - start;
    this._stats.incInt(_CONNECTION_WAIT_IN_PROGRESS, -1);
    this._stats.incInt(_CONNECTION_WAITS, 1);
    this._stats.incLong(_CONNECTION_WAIT_TIME, duration);
  }
  public void startClientOp() {
    this._stats.incInt(clientOpInProgressId, 1);
    this._stats.incInt(clientOpSendInProgressId, 1);
  }
  public void endClientOpSend(long duration, boolean failed) {
    this._stats.incInt(clientOpSendInProgressId, -1);
    int endClientOpSendId;
    if (failed) {
      endClientOpSendId = clientOpSendFailedId;
    } else {
      endClientOpSendId = clientOpSendId;
    }
    this._stats.incInt(endClientOpSendId, 1);
    this._stats.incLong(clientOpSendDurationId, duration);
  }
  public void endClientOp(long duration, boolean timedOut, boolean failed) {
    this._stats.incInt(clientOpInProgressId, -1);
    int endClientOpId;
    if (timedOut) {
      endClientOpId = clientOpTimedOutId;
    } else if (failed) {
      endClientOpId = clientOpFailedId;
    } else {
      endClientOpId = clientOpId;
    }
    this._stats.incInt(endClientOpId, 1);
    this._stats.incLong(clientOpDurationId, duration);
  }
}
