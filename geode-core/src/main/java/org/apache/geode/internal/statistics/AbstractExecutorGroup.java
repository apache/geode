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
package org.apache.geode.internal.statistics;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.ThreadMonitoringUtils;
import org.apache.geode.internal.logging.LogService;

public abstract class AbstractExecutorGroup {

  private static final int THREAD_DUMP_DEPTH = 40;
  private static final Logger logger = LogService.getLogger();
  private long threadID;
  private String grpName;
  private short numIterationsStuck;
  private long startTime;

  private static final Properties nonDefault = new Properties();
  private static final DistributionConfigImpl distributionConfigImpl =
      new DistributionConfigImpl(nonDefault);

  public AbstractExecutorGroup() {
    this.startTime = System.currentTimeMillis();
    this.numIterationsStuck = 0;
    this.threadID = Thread.currentThread().getId();
  }

  public void handleExpiry(long stuckTime) {
    this.incNumIterationsStuck();
    logger.warn(handleLogMessage(stuckTime));
    if (distributionConfigImpl.getThreadMonitorAutoEnabled()
        && this.numIterationsStuck > distributionConfigImpl.getThreadMonitorAutoLimit())
      handleInterrupt();
  }

  @SuppressWarnings("deprecation")
  private void handleInterrupt() {
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getId() == this.threadID) {
        logger.error("Thread: <{}> is stuck for <{}> iterations - Executing Interrupt\n",
            this.threadID, this.numIterationsStuck);

        t.stop();
        ThreadMonitoringUtils.getThreadMonitorObj().getMonitorMap().remove(this.threadID);

        break;
      }
    }
  }

  private String handleLogMessage(long stuckTime) {

    DateFormat dateFormat = new SimpleDateFormat("dd MMM yyyy HH:mm:ss zzz");

    ThreadInfo thread =
        ManagementFactory.getThreadMXBean().getThreadInfo(this.threadID, THREAD_DUMP_DEPTH);

    StringBuilder strb = new StringBuilder();

    strb.append("Thread < ").append(this.threadID).append(" > that was executed at < ")
        .append(dateFormat.format(this.getStartTime())).append(" > is stuck for <")
        .append((float) stuckTime / 1000)
        .append(" seconds> and number of thread monitor iteration < ")
        .append(this.numIterationsStuck).append(" > ").append(System.lineSeparator())
        .append("Thread Name < ").append(thread.getThreadName()).append(" > ")
        .append(System.lineSeparator()).append("Thread state < ").append(thread.getThreadState())
        .append(" > ").append(System.lineSeparator());

    if (thread.getLockName() != null)
      strb.append("Waiting on < ").append(thread.getLockName()).append(" > ")
          .append(System.lineSeparator());

    if (thread.getLockOwnerName() != null)
      strb.append("Owned By < ").append(thread.getLockOwnerName()).append(" > and ID < ")
          .append(thread.getLockOwnerId()).append(" > ").append(System.lineSeparator());

    strb.append("Executor Group < ").append(grpName).append(" > ").append(System.lineSeparator())
        .append("Monitored metric < ResourceManagerStats.numThreadsStuck >")
        .append(System.lineSeparator()).append("Thread Stack:").append(System.lineSeparator());

    for (int i = 0; i < thread.getStackTrace().length; i++) {
      String row = thread.getStackTrace()[i].toString();
      strb.append(row).append(System.lineSeparator());
    }

    return strb.toString();
  }

  public long getStartTime() {
    return this.startTime;
  }

  public void setStartTime(long newTime) {
    this.startTime = newTime;
  }

  public short getNumIterationsStuck() {
    return this.numIterationsStuck;
  }

  public void incNumIterationsStuck() {
    this.numIterationsStuck++;
  }

  public String getGrpName() {
    return this.grpName;
  }

  public void setGrpName(String grpName) {
    this.grpName = grpName;
  }

  public long getThreadID() {
    return this.threadID;
  }

}
