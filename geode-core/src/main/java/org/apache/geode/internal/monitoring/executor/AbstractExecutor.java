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
package org.apache.geode.internal.monitoring.executor;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;

public abstract class AbstractExecutor {

  private static final int THREAD_DUMP_DEPTH = 40;
  private static final Logger logger = LogService.getLogger();
  private long threadID;
  private String groupName;
  private short numIterationsStuck;
  private long startTime;

  public AbstractExecutor(ThreadsMonitoring tMonitoring) {
    this.startTime = System.currentTimeMillis();
    this.numIterationsStuck = 0;
    this.threadID = Thread.currentThread().getId();
  }

  public AbstractExecutor(ThreadsMonitoring tMonitoring, long threadID) {
    this.startTime = System.currentTimeMillis();
    this.numIterationsStuck = 0;
    this.threadID = threadID;
  }

  public void handleExpiry(long stuckTime) {
    this.incNumIterationsStuck();
    logger.warn(handleLogMessage(stuckTime));
  }

  private String handleLogMessage(long stuckTime) {

    DateFormat dateFormat = new SimpleDateFormat("dd MMM yyyy HH:mm:ss zzz");

    ThreadInfo thread =
        ManagementFactory.getThreadMXBean().getThreadInfo(this.threadID, THREAD_DUMP_DEPTH);
    boolean logThreadDetails = (thread != null);

    StringBuilder strb = new StringBuilder();

    strb.append("Thread <").append(this.threadID).append("> that was executed at <")
        .append(dateFormat.format(this.getStartTime())).append("> has been stuck for <")
        .append((float) stuckTime / 1000)
        .append(" seconds> and number of thread monitor iteration <")
        .append(this.numIterationsStuck).append("> ").append(System.lineSeparator());
    if (logThreadDetails) {
      strb.append("Thread Name <").append(thread.getThreadName()).append(">")
          .append(System.lineSeparator()).append("Thread state <").append(thread.getThreadState())
          .append(">").append(System.lineSeparator());

      if (thread.getLockName() != null)
        strb.append("Waiting on <").append(thread.getLockName()).append(">")
            .append(System.lineSeparator());

      if (thread.getLockOwnerName() != null)
        strb.append("Owned By <").append(thread.getLockOwnerName()).append("> and ID <")
            .append(thread.getLockOwnerId()).append(">").append(System.lineSeparator());
    }


    strb.append("Executor Group <").append(groupName).append(">").append(System.lineSeparator())
        .append("Monitored metric <ResourceManagerStats.numThreadsStuck>")
        .append(System.lineSeparator());

    if (logThreadDetails) {
      strb.append("Thread Stack:").append(System.lineSeparator());
      for (int i = 0; i < thread.getStackTrace().length; i++) {
        String row = thread.getStackTrace()[i].toString();
        strb.append(row).append(System.lineSeparator());
      }
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

  public String getGroupName() {
    return this.groupName;
  }

  public void setGroupName(String groupName) {
    this.groupName = groupName;
  }

  public long getThreadID() {
    return this.threadID;
  }

}
