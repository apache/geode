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
package com.gemstone.gemfire.internal.offheap;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import com.gemstone.gemfire.internal.shared.StringPrintWriter;

@SuppressWarnings("serial")
/**
 * Used by SimpleMemoryAllocatorImpl to debug off-heap memory leaks.
 */
public class RefCountChangeInfo extends Throwable {
  private final String threadName;
  private final int rc;
  private final Object owner;
  private int dupCount;
  
  public RefCountChangeInfo(boolean decRefCount, int rc, Object owner) {
    super(decRefCount ? "FREE" : "USED");
    this.threadName = Thread.currentThread().getName();
    this.rc = rc;
    this.owner = owner;
  }
  
  public Object getOwner() {
    return this.owner;
  }
  
  public int getDupCount() {
    return this.dupCount;
  }
  public void decDupCount() {
    this.dupCount--;
  }

  @Override
  public String toString() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(64*1024);
    PrintStream ps = new PrintStream(baos);
    ps.print(this.getMessage());
    ps.print(" rc=");
    ps.print(this.rc);
    if (this.dupCount > 0) {
      ps.print(" dupCount=");
      ps.print(this.dupCount);
    }
    ps.print(" by ");
    ps.print(this.threadName);
    if (this.owner != null) {
      ps.print(" owner=");
      ps.print(this.owner.getClass().getName());
      ps.print("@");
      ps.print(System.identityHashCode(this.owner));
    }
    
    ps.println(": ");
    cleanStackTrace(ps); 
    ps.flush();
    
    return baos.toString();
  }
  
  public boolean isDuplicate(RefCountChangeInfo other) {
    if (!getMessage().equals(other.getMessage())) return false;
    String trace = getStackTraceString();
    String traceOther = other.getStackTraceString();
    if (trace.hashCode() != traceOther.hashCode()) return false;
    if (trace.equals(traceOther)) {
      this.dupCount++;
      return true;
    } else {
      return false;
    }
  }

  private String stackTraceString;
  String getStackTraceString() {
    String result = this.stackTraceString;
    if (result == null) {
	ByteArrayOutputStream baos = new ByteArrayOutputStream(64*1024);
	PrintStream spr = new PrintStream(baos);

	cleanStackTrace(spr);
	result = baos.toString();
	this.stackTraceString = result;
    }
    return result;
  }
  
  private void cleanStackTrace(PrintStream ps) {
      StackTraceElement[] trace = getStackTrace();
      // skip the initial elements from the offheap package
      int skip=0;
      for (int i=0; i < trace.length; i++) {
	if (!trace[i].getClassName().contains("com.gemstone.gemfire.internal.offheap")) {
          skip = i;
          break;
        }
      }
      for (int i=skip; i < trace.length; i++) {
        ps.println("\tat " + trace[i]);
      }   
  }

}