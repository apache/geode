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

import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.logging.internal.log4j.api.LogService;

public abstract class AbstractExecutor {

  private static final int THREAD_DUMP_DEPTH = 40;
  private static final Logger logger = LogService.getLogger();
  public static final String LOCK_OWNER_THREAD_STACK = "Lock owner thread stack";
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
    logger.warn(createThreadReport(stuckTime));
  }

  String createThreadReport(long stuckTime) {

    DateFormat dateFormat = new SimpleDateFormat("dd MMM yyyy HH:mm:ss zzz");

    ThreadInfo thread =
        ManagementFactory.getThreadMXBean().getThreadInfo(this.threadID, THREAD_DUMP_DEPTH);
    boolean logThreadDetails = (thread != null);

    StringBuilder stringBuilder = new StringBuilder();
    final String lineSeparator = System.lineSeparator();

    stringBuilder.append("Thread <").append(this.threadID).append("> (0x")
        .append(Long.toHexString(this.threadID)).append(") that was executed at <")
        .append(dateFormat.format(this.getStartTime())).append("> has been stuck for <")
        .append((float) stuckTime / 1000)
        .append(" seconds> and number of thread monitor iteration <")
        .append(this.numIterationsStuck).append("> ").append(lineSeparator);
    if (logThreadDetails) {
      stringBuilder.append("Thread Name <").append(thread.getThreadName()).append(">")
          .append(" state <").append(thread.getThreadState())
          .append(">").append(lineSeparator);

      if (thread.getLockName() != null) {
        stringBuilder.append("Waiting on <").append(thread.getLockName()).append(">")
            .append(lineSeparator);
      }

      if (thread.getLockOwnerName() != null) {
        stringBuilder.append("Owned By <").append(thread.getLockOwnerName()).append("> with ID <")
            .append(thread.getLockOwnerId()).append(">").append(lineSeparator);
      }
    }


    stringBuilder.append("Executor Group <").append(groupName).append(">").append(
        lineSeparator)
        .append("Monitored metric <ResourceManagerStats.numThreadsStuck>")
        .append(lineSeparator);

    if (logThreadDetails) {
      writeThreadStack(thread, "Thread stack:", stringBuilder);
    }

    if (logThreadDetails && thread.getLockOwnerName() != null) {
      ThreadInfo lockOwnerThread = ManagementFactory.getThreadMXBean()
          .getThreadInfo(thread.getLockOwnerId(), THREAD_DUMP_DEPTH);
      if (lockOwnerThread != null) {
        writeThreadStack(lockOwnerThread, LOCK_OWNER_THREAD_STACK, stringBuilder);
      }
    }

    return stringBuilder.toString();
  }

  private void writeThreadStack(ThreadInfo thread, String header, StringBuilder strb) {
    final String lineSeparator = System.lineSeparator();
    strb.append(header).append(lineSeparator);
    for (int i = 0; i < thread.getStackTrace().length; i++) {
      String row = thread.getStackTrace()[i].toString();
      strb.append(row).append(lineSeparator);
    }
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
