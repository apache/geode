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
package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Bean class act as container for client stats
 *
 */

public class ClientHealthStats implements DataSerializableFixedID, Serializable {
  private static final long serialVersionUID = 4229401714870332766L;

  /**
   * "numOfGets", IntCounter, "The total number of times a successful get has been done on this
   * cache." Java: CachePerfStats.gets Native: Not yet Defined
   */
  protected long numOfGets;

  /**
   * "numOfPuts", IntCounter, "The total number of times an entry is added or replaced in this cache
   * as a result of a local operation (put(), create(), or get() which results in load, netsearch,
   * or netloading a value). Note that this only counts puts done explicitly on this cache. It does
   * not count updates pushed from other caches." Java: CachePerfStats.puts Native: Not yet Defined
   */
  protected long numOfPuts;

  /**
   * Represents number of cache misses in this client. IntCounter, "Total number of times a get on
   * the cache did not find a value already in local memory." Java: CachePerfStats.misses
   */
  protected long numOfMisses;

  /**
   * Represents number of cache listners calls completed. IntCounter, "Total number of times a cache
   * listener call has completed." Java: CachePerfStats.cacheListenerCallsCompleted
   */
  protected int numOfCacheListenerCalls;

  /**
   * Represents total number of active threads in the client VM. IntCounter, "Current number of live
   * threads (both daemon and non-daemon) in this VM." Java: VMStats.threads
   **/
  protected int numOfThreads;

  /**
   * Represents the CPU time used by the process (in nanoseconds). LongCounter, "CPU timed used by
   * the process in nanoseconds." Java: VMStats.processCpuTime
   **/
  protected long processCpuTime;

  /**
   * Represents the number of cpus available to the java VM on its machine. IntCounter, "Number of
   * cpus available to the java VM on its machine." Java: VMStats.cpus
   **/
  protected int cpus;


  /**
   * Represents time when this snapshot of the client statistics was taken.
   **/
  protected Date updateTime;

  /**
   * Represents stats for a poolName .
   **/
  private HashMap<String, String> poolStats = new HashMap<String, String>();

  /** The versions in which this message was modified */
  @Immutable
  private static final KnownVersion[] dsfidVersions =
      new KnownVersion[] {KnownVersion.GEODE_1_9_0};

  public ClientHealthStats() {}

  /**
   * This method returns total number of successful get requests completed.
   *
   * @return total number of get requests completed successfully.
   */
  public long getNumOfGets() {
    return numOfGets;
  }

  /**
   * This method sets the total number of successful get requests completed.
   *
   * @param numOfGets Total number of get requests to be set.
   */
  public void setNumOfGets(long numOfGets) {
    this.numOfGets = numOfGets;
  }

  /**
   * This method returns the total number of successful put requests completed.
   *
   * @return Total number of put requests completed.
   */
  public long getNumOfPuts() {
    return numOfPuts;
  }

  /**
   * This method sets the total number of successful put requests completed.
   *
   * @param numOfPuts Total number of put requests to be set.
   */
  public void setNumOfPuts(long numOfPuts) {
    this.numOfPuts = numOfPuts;
  }

  /**
   * This method returns total number of cache misses in this client.
   *
   * @return total number of cache misses.
   */
  public long getNumOfMisses() {
    return numOfMisses;
  }

  /**
   * This method sets total number of cache misses in this client.
   *
   * @param numOfMisses total number of cache misses.
   */
  public void setNumOfMisses(long numOfMisses) {
    this.numOfMisses = numOfMisses;
  }

  /**
   * This method returns total number of cache listener calls completed.
   *
   * @return total number of cache listener calls completed.
   */
  public int getNumOfCacheListenerCalls() {
    return numOfCacheListenerCalls;
  }

  /**
   * This method sets total number of cache listener calls compeleted.
   *
   * @param numOfCacheListenerCalls total number of cache listener calls completed.
   */
  public void setNumOfCacheListenerCalls(int numOfCacheListenerCalls) {
    this.numOfCacheListenerCalls = numOfCacheListenerCalls;
  }

  /**
   * This method returns total number of threads in the client VM.
   *
   * @return total number of threads in the client VM
   */
  public int getNumOfThreads() {
    return numOfThreads;
  }

  /**
   * This method sets the total number of threads in the client VM.
   *
   * @param numOfThreads total number of threads in the client VM
   */
  public void setNumOfThreads(int numOfThreads) {
    this.numOfThreads = numOfThreads;
  }

  /**
   * This method returns the CPU time used by the process (in nanoseconds)
   *
   * @return CPU time used by the process (in nanoseconds)
   */
  public long getProcessCpuTime() {
    return processCpuTime;
  }

  /**
   * This method sets the CPU time used by the process (in nanoseconds).
   *
   * @param processCpuTime CPU time used by the process (in nanoseconds)
   */
  public void setProcessCpuTime(long processCpuTime) {
    this.processCpuTime = processCpuTime;
  }

  public int getCpus() {
    return cpus;
  }

  public void setCpus(int cpus) {
    this.cpus = cpus;
  }

  public Date getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(Date updateTime) {
    this.updateTime = updateTime;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writePrimitiveLong(numOfGets, out);
    DataSerializer.writePrimitiveLong(numOfPuts, out);
    DataSerializer.writePrimitiveLong(numOfMisses, out);
    DataSerializer.writePrimitiveInt(numOfCacheListenerCalls, out);
    DataSerializer.writePrimitiveInt(numOfThreads, out);
    DataSerializer.writePrimitiveInt(cpus, out);
    DataSerializer.writePrimitiveLong(processCpuTime, out);
    DataSerializer.writeDate(updateTime, out);
    DataSerializer.writeHashMap((poolStats), out);
  }

  public void toDataPre_GEODE_1_9_0_0(DataOutput out, SerializationContext context)
      throws IOException {
    DataSerializer.writePrimitiveInt((int) numOfGets, out);
    DataSerializer.writePrimitiveInt((int) numOfPuts, out);
    DataSerializer.writePrimitiveInt((int) numOfMisses, out);
    DataSerializer.writePrimitiveInt(numOfCacheListenerCalls, out);
    DataSerializer.writePrimitiveInt(numOfThreads, out);
    DataSerializer.writePrimitiveInt(cpus, out);
    DataSerializer.writePrimitiveLong(processCpuTime, out);
    DataSerializer.writeDate(updateTime, out);
    DataSerializer.writeHashMap((poolStats), out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    this.numOfGets = DataSerializer.readPrimitiveLong(in);
    this.numOfPuts = DataSerializer.readPrimitiveLong(in);
    this.numOfMisses = DataSerializer.readPrimitiveLong(in);
    this.numOfCacheListenerCalls = DataSerializer.readPrimitiveInt(in);
    this.numOfThreads = DataSerializer.readPrimitiveInt(in);
    this.cpus = DataSerializer.readPrimitiveInt(in);
    this.processCpuTime = DataSerializer.readPrimitiveLong(in);
    this.updateTime = DataSerializer.readDate(in);
    this.poolStats = DataSerializer.readHashMap(in);
  }

  public void fromDataPre_GEODE_1_9_0_0(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    this.numOfGets = DataSerializer.readPrimitiveInt(in);
    this.numOfPuts = DataSerializer.readPrimitiveInt(in);
    this.numOfMisses = DataSerializer.readPrimitiveInt(in);
    this.numOfCacheListenerCalls = DataSerializer.readPrimitiveInt(in);
    this.numOfThreads = DataSerializer.readPrimitiveInt(in);
    this.cpus = DataSerializer.readPrimitiveInt(in);
    this.processCpuTime = DataSerializer.readPrimitiveLong(in);
    this.updateTime = DataSerializer.readDate(in);
    this.poolStats = DataSerializer.readHashMap(in);
  }

  @Override
  public String toString() {

    StringBuffer buf = new StringBuffer();
    buf.append("ClientHealthStats [");
    buf.append("\n numOfGets=" + this.numOfGets);
    buf.append("\n numOfPuts=" + this.numOfPuts);
    buf.append("\n numOfMisses=" + this.numOfMisses);
    buf.append("\n numOfCacheListenerCalls=" + this.numOfCacheListenerCalls);
    buf.append("\n numOfThreads=" + this.numOfThreads);
    buf.append("\n cpus=" + this.cpus);
    buf.append("\n processCpuTime=" + this.processCpuTime);
    buf.append("\n updateTime=" + this.updateTime);
    Iterator<Entry<String, String>> it = this.poolStats.entrySet().iterator();
    StringBuffer tempBuffer = new StringBuffer();
    while (it.hasNext()) {
      Entry<String, String> entry = it.next();
      tempBuffer.append(entry.getKey() + " = " + entry.getValue());
    }
    buf.append("\n poolStats " + tempBuffer);
    buf.append("\n]");

    return buf.toString();
  }

  @Override
  public int getDSFID() {
    return CLIENT_HEALTH_STATS;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return dsfidVersions;
  }

  public HashMap<String, String> getPoolStats() {
    return this.poolStats;
  }

  public void setPoolStats(HashMap<String, String> statsMap) {
    this.poolStats = statsMap;
  }
}
