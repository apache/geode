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
package org.apache.geode.test.dunit.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

class StackTraceInfo implements Serializable {

  private final String message;
  private final StackTraceElement[] stackTraceElements;

  static StackTraceInfo create(int vmId, long threadId) {
    long[] threadIds = new long[] {threadId};

    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds, true, true);

    assertThat(threadInfos).hasSize(1);

    String message = "Stack trace for vm-" + vmId + " thread-" + threadId;
    return new StackTraceInfo(message, threadInfos[0].getStackTrace());
  }

  StackTraceInfo(String message, StackTraceElement[] stackTraceElements) {
    this.message = message;
    this.stackTraceElements = stackTraceElements;
  }

  String getMessage() {
    return message;
  }

  StackTraceElement[] getStackTraceElements() {
    return stackTraceElements;
  }
}
