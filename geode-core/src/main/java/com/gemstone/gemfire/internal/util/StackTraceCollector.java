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
package com.gemstone.gemfire.internal.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Collects unique stack traces for later examination.  This works best and is most useful when traces are added at the same point of origin.
 * This will collect all unique code paths while the collector is turned on.  This class will affect timing and performance so its use is suspect
 * while diagnosing concurrency issues.
 * 
 * Example: 
 * <code>
 * public void doSomeWork() {
 *      // This will collect all unique code paths to this method.
 *      this.traceCollector.add(new Exception())
 * }
 * </code> 
 * 
 *
 * @since Geode 1.0
 */
public class StackTraceCollector {
  /**
   * Maximum stack trace size.
   */
  private static int MAX_TRACE_SIZE = 128 * 1024;
  
  /**
   * Contains unique stack traces.
   */
  private Set<String> stackTraceSet = new ConcurrentSkipListSet<String>();
  
  /**
   * The {@link StackTraceCollector#add(Throwable)} method will do no work when this is false.
   */
  private volatile boolean on = true;

  /**
   * Contains converted stack traces (Throwable -> String).
   */
  private ByteArrayOutputStream ostream = new ByteArrayOutputStream(MAX_TRACE_SIZE);
  
  /**
   * Used to convert stack traces (Throwable -> String).
   */
  private PrintWriter writer = new PrintWriter(this.ostream);
  
  /**
   * Returns true if this collector is on.
   */
  public boolean isOn() {
    return this.on;
  }

  /**
   * Returns true if this collector is off.
   */
  public boolean isOff() {
    return !isOn();
  }
  
  /**
   * Turns this collector on.
   */
  public void on() {
    this.on = true;
  }
  
  /**
   * Turns this collector off.
   */
  public void off() {
    this.on = false;
  }
  
  /**
   * Adds a stack trace to this collector if it is unique.  
   * @param throwable used to generate the stack trace.
   * @return true if the stack trace was not already present in this collector.  This
   * will always return false if the collector is turned off.
   */
  public boolean add(final Throwable throwable) {
    return add(this.stackTraceSet,throwable);
  }
  
  /**
   * Adds a stack trace to a collection Set if it is unique.  
   * @param throwable used to generate the stack trace.
   * @return true if the stack trace was not already present in this collector.  This
   * will always return false if the collector is turned off.
   */
  protected synchronized boolean add(Set<String> stackTraceSet, final Throwable throwable) {
    if(this.on) {
      String trace = convert(throwable);
      return stackTraceSet.add(trace);
    }
    
    return false;    
  }
  
  /**
   * Clears this collector of all stack traces.
   * @return true if there were stack traces to clear.
   */
  public boolean clear() {
    boolean value = (this.stackTraceSet.size() > 0);
    this.stackTraceSet.clear();
    
    return value;
  }

  /**
   * Returns true if the throwable parameter is contained by 
   * this collector. 
   * @param throwable a stack trace to check.
   */
  public boolean contains(final Throwable throwable) {
    return contains(convert(throwable));
  }
  
  /**
   * Returns true if the stackTrace parameter is contained
   * by this collector.
   * @param stackTrace a stack trace.
   */
  public boolean contains(final String stackTrace) {
    return this.stackTraceSet.contains(stackTrace);
  }
  
  /**
   * Returns the stack trace associated with a Throwable as a String.
   * @param throwable contains a stack trace to be extracted and returned.
   */
  public synchronized String convert(final Throwable throwable) {
    throwable.printStackTrace(this.writer);
    this.writer.flush();
    String stackTrace = this.ostream.toString();
    this.ostream.reset();
    
    return stackTrace;
  }
  
  /**
   * Returns an unmodifiable Set of the unique stack traces contained by this collector.
   */
  public Set<String> view() {
    return Collections.unmodifiableSet(this.stackTraceSet);
  }
}
