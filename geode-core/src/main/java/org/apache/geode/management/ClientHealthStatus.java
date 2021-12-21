/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management;

import java.util.HashMap;
import java.util.Map;


/**
 * Composite data type used to distribute statistics which can be used to determine the health of a
 * cache client.
 *
 * @since GemFire 7.0
 */
public class ClientHealthStatus {

  /**
   * Id of the client
   */
  private String clientId;

  /**
   * Name of the client
   */
  private String name;

  /**
   * Host Name of the client
   */
  private String hostName;

  /**
   * "numOfGets", IntCounter, "The total number of times a successful get has been done on this
   * cache." Java: CachePerfStats.gets Native: Not yet Defined
   */
  private int numOfGets;

  /**
   * "numOfPuts", IntCounter, "The total number of times an entry is added or replaced in this cache
   * as a result of a local operation (put(), create(), or get() which results in load, netsearch,
   * or netloading a value). Note that this only counts puts done explicitly on this cache. It does
   * not count updates pushed from other caches." Java: CachePerfStats.puts Native: Not yet Defined
   */
  private int numOfPuts;

  /**
   * Represents number of cache misses in this client. IntCounter, "Total number of times a get on
   * the cache did not find a value already in local memory." Java: CachePerfStats.misses
   */
  private int numOfMisses;

  /**
   * Represents number of cache listners calls completed. IntCounter, "Total number of times a cache
   * listener call has completed." Java: CachePerfStats.cacheListenerCallsCompleted
   */
  private int numOfCacheListenerCalls;

  /**
   * Represents total number of active threads in the client VM. IntCounter, "Current number of live
   * threads (both daemon and non-daemon) in this VM." Java: VMStats.threads
   **/
  private int numOfThreads;

  /**
   * Represents the CPU time used by the process (in nanoseconds). LongCounter, "CPU timed used by
   * the process in nanoseconds." Java: VMStats.processCpuTime
   **/
  private long processCpuTime;

  /**
   * Represents the number of cpus available to the java VM on its machine. IntCounter, "Number of
   * cpus available to the java VM on its machine." Java: VMStats.cpus
   **/
  private int cpus;

  /**
   * client queue sizes
   */
  private int queueSize;

  /**
   * Represents time when this snapshot of the client statistics was taken.
   **/
  private long upTime;

  /**
   * Whether a durable client is connected or not to the server.
   **/
  private boolean connected;

  /**
   * Number of CQS getting executed by the client
   */
  private int clientCQCount;

  /**
   * Whether the client has subscriptions enabled true or not..
   **/
  private boolean subscriptionEnabled;


  /**
   * Represents stats for a poolName .
   **/
  private Map<String, String> clientPoolStatsMap = new HashMap<String, String>();

  /**
   * Returns the number of times a successful get operation has occurred.
   */
  public int getNumOfGets() {
    return numOfGets;
  }

  /**
   * Returns the number of times an entry was added or replaced in this cache as a result of a local
   * operation. Operations counted include puts, creates, and gets which result in loading, net
   * searching, or net loading a value. The count only includes operations done explicitly on this
   * cache, not those that are pushed from other caches.
   */
  public int getNumOfPuts() {
    return numOfPuts;
  }

  /**
   * Returns the number of times a cache miss has occurred.
   */
  public int getNumOfMisses() {
    return numOfMisses;
  }

  /**
   * Returns the number of times a cache listener call has completed.
   */
  public int getNumOfCacheListenerCalls() {
    return numOfCacheListenerCalls;
  }

  /**
   * Returns the number of threads currently in use.
   */
  public int getNumOfThreads() {
    return numOfThreads;
  }

  /**
   * Returns the amount of time (in nanoseconds) used by the client process.
   */
  public long getProcessCpuTime() {
    return processCpuTime;
  }

  /**
   * Returns the number of CPUs available to the client.
   */
  public int getCpus() {
    return cpus;
  }

  /**
   * Returns the amount of time (in seconds) that the client has been running.
   */
  public long getUpTime() {
    return upTime;
  }

  /**
   * Returns the ID of the client.
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * Sets the number of times a successful get operation has occurred.
   */
  public void setNumOfGets(int numOfGets) {
    this.numOfGets = numOfGets;
  }

  /**
   * Set the number of times an entry was added or replaced in this cache as a result of a local
   * operation.
   */
  public void setNumOfPuts(int numOfPuts) {
    this.numOfPuts = numOfPuts;
  }

  /**
   * Sets the number of times a cache miss has occurred.
   */
  public void setNumOfMisses(int numOfMisses) {
    this.numOfMisses = numOfMisses;
  }

  /**
   * Sets the number of times a cache listener call has completed.
   */
  public void setNumOfCacheListenerCalls(int numOfCacheListenerCalls) {
    this.numOfCacheListenerCalls = numOfCacheListenerCalls;
  }

  /**
   * Sets the number of threads currently in use.
   */
  public void setNumOfThreads(int numOfThreads) {
    this.numOfThreads = numOfThreads;
  }

  /**
   * Sets the amount of time (in nanoseconds) used by the client process.
   */
  public void setProcessCpuTime(long processCpuTime) {
    this.processCpuTime = processCpuTime;
  }

  /**
   * Sets the number of CPUs available to the client.
   */
  public void setCpus(int cpus) {
    this.cpus = cpus;
  }

  /**
   * Sets the amount of time (in seconds) that the client has been running.
   */
  public void setUpTime(long upTime) {
    this.upTime = upTime;
  }

  /**
   * Sets the client queue size.
   */
  public void setQueueSize(int queueSize) {
    this.queueSize = queueSize;
  }

  /**
   * Sets the ID of the client.
   */
  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  /**
   * Returns the client queue size.
   */
  public int getQueueSize() {
    return queueSize;
  }

  /**
   * Returns the name of the client.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the name of the host on which the client is running.
   */
  public String getHostName() {
    return hostName;
  }

  /**
   * Sets the name of the client.
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Sets the name of the host on which the client is running.
   */
  public void setHostName(String hostName) {
    this.hostName = hostName;
  }


  public Map<String, String> getPoolStats() {
    return clientPoolStatsMap;
  }

  public void setPoolStats(Map<String, String> map) {
    clientPoolStatsMap = map;
  }

  public boolean isConnected() {
    return connected;
  }

  public void setConnected(boolean connected) {
    this.connected = connected;
  }


  public boolean getSubscriptionEnabled() {
    return subscriptionEnabled;
  }

  public int getClientCQCount() {
    return clientCQCount;
  }

  public void setSubscriptionEnabled(boolean subscriptionEnabled) {
    this.subscriptionEnabled = subscriptionEnabled;
  }

  public void setClientCQCount(int clientCQCount) {
    this.clientCQCount = clientCQCount;
  }


  @Override
  public String toString() {
    return "ClientHealthStatus [clientId=" + clientId + ", name=" + name + ", hostName=" + hostName
        + ", numOfGets=" + numOfGets + ", numOfPuts=" + numOfPuts + ", numOfMisses=" + numOfMisses
        + ", numOfCacheListenerCalls=" + numOfCacheListenerCalls + ", numOfThreads=" + numOfThreads
        + ", processCpuTime=" + processCpuTime + ", cpus=" + cpus + ", queueSize=" + queueSize
        + ", upTime=" + upTime + ", clientPoolStatsMap=" + clientPoolStatsMap + ", connected="
        + connected + " clientCQCount = " + clientCQCount + " subscriptionEnabled = "
        + subscriptionEnabled + "]";
  }

}
