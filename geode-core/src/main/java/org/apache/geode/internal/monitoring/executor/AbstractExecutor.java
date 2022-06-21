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

import static java.lang.Integer.min;

import java.lang.management.LockInfo;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.lang.SystemProperty;
import org.apache.geode.logging.internal.log4j.api.LogService;

public abstract class AbstractExecutor {

  private static final int THREAD_DUMP_DEPTH = 40;
  private static final Logger logger = LogService.getLogger();
  public static final String LOCK_OWNER_THREAD_STACK = "Lock owner thread stack";
  private final long threadID;
  private final String groupName;
  private short numIterationsStuck;
  private volatile long startTime;
  private final int maxThreadStuckTime = SystemProperty.getProductIntegerProperty(
      "max-thread-stuck-time-minutes").orElse(0) * 1000 * 60;

  private volatile boolean stuckForGood = false;

  public AbstractExecutor(String groupName) {
    this(groupName, Thread.currentThread().getId());
  }

  protected AbstractExecutor(String groupName, long threadID) {
    this.groupName = groupName;
    startTime = 0;
    numIterationsStuck = 0;
    this.threadID = threadID;
  }

  public void handleExpiry(long stuckTime, Map<Long, ThreadInfo> threadInfoMap) {
    incNumIterationsStuck();
    sendAlertForThreadStuckForLong(stuckTime, threadInfoMap);
    logger.warn(createThreadReport(stuckTime, threadInfoMap));
  }

  private void sendAlertForThreadStuckForLong(long stuckTime, Map<Long, ThreadInfo> threadInfoMap) {
    if (maxThreadStuckTime <= 0) {
      return;
    }
    if (threadInfoMap.get(threadID) == null) {
      return;
    }
    if (stuckForGood) {
      return;
    }
    if (stuckTime > maxThreadStuckTime) {
      stuckForGood = true;
      logger.fatal(createThreadReport(stuckTime, threadInfoMap));
    }
  }

  String createThreadReport(long stuckTime, Map<Long, ThreadInfo> threadInfoMap) {

    final DateFormat dateFormat = new SimpleDateFormat("dd MMM yyyy HH:mm:ss zzz");

    final ThreadInfo thread = threadInfoMap.get(threadID);
    final boolean logThreadDetails = (thread != null);

    final StringBuilder stringBuilder = new StringBuilder();
    final String lineSeparator = System.lineSeparator();

    stringBuilder.append("Thread <").append(threadID).append("> (0x")
        .append(Long.toHexString(threadID)).append(") that was executed at <")
        .append(dateFormat.format(getStartTime())).append("> has been stuck for <")
        .append((float) stuckTime / 1000)
        .append(" seconds> and number of thread monitor iteration <")
        .append(numIterationsStuck).append("> ").append(lineSeparator);
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
      writeThreadStack(thread, "Thread stack", stringBuilder);
    }

    if (logThreadDetails && thread.getLockOwnerName() != null) {
      final ThreadInfo lockOwnerThread = threadInfoMap.get(thread.getLockOwnerId());
      if (lockOwnerThread != null) {
        writeThreadStack(lockOwnerThread, LOCK_OWNER_THREAD_STACK, stringBuilder);
      }
    }

    return stringBuilder.toString();
  }

  @Immutable
  private static final String INDENT = "  ";
  @Immutable
  private static final String lineSeparator = System.lineSeparator();

  private void writeThreadStack(ThreadInfo thread, String header, StringBuilder strb) {
    strb.append(header).append(" for \"")
        .append(thread.getThreadName())
        .append("\" (0x").append(Long.toHexString(thread.getThreadId()))
        .append("):").append(lineSeparator);
    strb.append("java.lang.ThreadState: ").append(thread.getThreadState());
    if (thread.isSuspended()) {
      strb.append(" (suspended)");
    }
    if (thread.isInNative()) {
      strb.append(" (in native)");
    }
    strb.append(lineSeparator);
    MonitorInfo[] lockedMonitors = thread.getLockedMonitors();
    for (int i = 0; i < min(thread.getStackTrace().length, THREAD_DUMP_DEPTH); i++) {
      String row = thread.getStackTrace()[i].toString();
      strb.append(INDENT).append("at ").append(row).append(lineSeparator);
      appendLockedMonitor(strb, i, lockedMonitors);
    }
    strb.append("Locked ownable synchronizers:").append(lineSeparator);
    LockInfo[] lockedSynchronizers = thread.getLockedSynchronizers();
    if (lockedSynchronizers.length == 0) {
      strb.append(INDENT).append("- None").append(lineSeparator);
    } else {
      for (LockInfo lockInfo : lockedSynchronizers) {
        strb.append(INDENT).append("- ").append(lockInfo).append(lineSeparator);
      }
    }
  }

  private void appendLockedMonitor(StringBuilder strb, int stackDepth,
      MonitorInfo[] lockedMonitors) {
    for (MonitorInfo monitorInfo : lockedMonitors) {
      if (stackDepth == monitorInfo.getLockedStackDepth()) {
        strb.append(INDENT).append("  - locked " + monitorInfo).append(lineSeparator);
      }
    }
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long newTime) {
    startTime = newTime;
  }

  public short getNumIterationsStuck() {
    return numIterationsStuck;
  }

  public void incNumIterationsStuck() {
    numIterationsStuck++;
  }

  public String getGroupName() {
    return groupName;
  }

  public long getThreadID() {
    return threadID;
  }

  public void suspendMonitoring() {}

  public void resumeMonitoring() {}

  public void reportProgress() {
    setStartTime(System.currentTimeMillis());
  }

  public boolean isMonitoringSuspended() {
    return false;
  }

}
