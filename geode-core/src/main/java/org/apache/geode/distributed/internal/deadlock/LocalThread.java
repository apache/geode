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
package org.apache.geode.distributed.internal.deadlock;

import java.io.Serializable;
import java.lang.management.LockInfo;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;

/**
 * This class is serializable version of the java 1.6 ThreadInfo class. It also holds a locality
 * field to identify the VM where the thread exists.
 *
 *
 */
public class LocalThread implements Serializable, ThreadReference {
  private static final long serialVersionUID = 1L;

  private final Serializable locality;
  private final String threadName;
  private final long threadId;
  private final String threadStack;

  public LocalThread(Serializable locatility, ThreadInfo info) {
    locality = locatility;
    threadName = info.getThreadName();
    threadStack = generateThreadStack(info);
    threadId = info.getThreadId();
  }

  private String generateThreadStack(ThreadInfo info) {
    // This is annoying, but the to string method on info sucks.
    StringBuilder result = new StringBuilder();
    result.append(info.getThreadName()).append(" ID=0x")
        .append(Long.toHexString(info.getThreadId())).append("(").append(info.getThreadId())
        .append(") state=").append(info.getThreadState());


    if (info.getLockInfo() != null) {
      result.append("\n\twaiting to lock <" + info.getLockInfo() + ">");
    }
    for (StackTraceElement element : info.getStackTrace()) {
      result.append("\n\tat " + element);
      for (MonitorInfo monitor : info.getLockedMonitors()) {
        if (element.equals(monitor.getLockedStackFrame())) {
          result.append("\n\tlocked <" + monitor + ">");
        }
      }
    }

    if (info.getLockedSynchronizers().length > 0) {
      result.append("\nLocked synchronizers:");
      for (LockInfo sync : info.getLockedSynchronizers()) {
        result.append(
            "\n" + sync.getClassName() + "@" + Integer.toHexString(sync.getIdentityHashCode()));

      }
    }

    return result.toString();
  }

  public Serializable getLocatility() {
    return locality;
  }

  public String getThreadName() {
    return threadName;
  }

  public long getThreadId() {
    return threadId;
  }

  public String getThreadStack() {
    return threadStack;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (threadId ^ (threadId >>> 32));
    result = prime * result + ((locality == null) ? 0 : locality.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof LocalThread)) {
      return false;
    }
    LocalThread other = (LocalThread) obj;
    if (threadId != other.threadId) {
      return false;
    }
    if (locality == null) {
      return other.locality == null;
    } else
      return locality.equals(other.locality);
  }

  @Override
  public String toString() {
    return locality + ":" + threadName;
  }
}
