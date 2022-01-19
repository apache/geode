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
package org.apache.geode.internal.offheap;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;


@SuppressWarnings("serial")
/*
 * Used by MemoryAllocatorImpl to debug off-heap memory leaks.
 */
public class RefCountChangeInfo extends Throwable {
  private final String threadName;
  private final int rc;
  private final Object owner;
  private int useCount;

  public RefCountChangeInfo(boolean decRefCount, int rc, Object owner) {
    super(decRefCount ? "FREE" : "USED");
    threadName = Thread.currentThread().getName();
    this.rc = rc;
    this.owner = owner;
  }

  public Object getOwner() {
    return owner;
  }

  public int getUseCount() {
    return useCount;
  }

  public int incUseCount() {
    useCount++;
    return useCount;
  }

  public int decUseCount() {
    useCount--;
    return useCount;
  }

  @Override
  public String toString() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(64 * 1024);
    PrintStream ps = new PrintStream(baos);
    ps.print(getMessage());
    ps.print(" rc=");
    ps.print(rc);
    if (useCount > 0) {
      ps.print(" useCount=");
      ps.print(useCount);
    }
    ps.print(" by ");
    ps.print(threadName);
    if (owner != null) {
      ps.print(" owner=");
      ps.print(owner.getClass().getName());
      ps.print("@");
      ps.print(System.identityHashCode(owner));
    }

    ps.println(": ");
    cleanStackTrace(ps);
    ps.flush();

    return baos.toString();
  }

  public boolean isSameCaller(RefCountChangeInfo other) {
    if (!getMessage().equals(other.getMessage())) {
      return false;
    }
    Object trace = getStackTraceString();
    Object traceOther = other.getStackTraceString();
    if (trace.hashCode() != traceOther.hashCode()) {
      return false;
    }
    return trace.equals(traceOther);
  }

  private Object stackTraceString;

  Object getStackTraceString() {
    Object result = stackTraceString;
    if (result == null) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(64 * 1024);
      PrintStream spr = new PrintStream(baos);
      cleanStackTrace(spr);
      result = baos.toString();
      stackTraceString = result;
    }
    return result;
  }

  void setStackTraceString(Object sts) {
    stackTraceString = sts;
  }

  private void cleanStackTrace(PrintStream ps) {
    StackTraceElement[] trace = getStackTrace();
    // skip the initial elements from the offheap package
    int skip = 0;
    for (int i = 0; i < trace.length; i++) {
      if (!(trace[i].toString().contains("org.apache.geode.internal.offheap"))) {
        skip = i;
        break;
      }
    }
    for (int i = skip; i < trace.length; i++) {
      ps.println("\tat " + trace[i]);
    }
  }


}
