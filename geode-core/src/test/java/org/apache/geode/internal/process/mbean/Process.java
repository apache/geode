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
package org.apache.geode.internal.process.mbean;

/**
 * Extracted from LocalProcessControllerDUnitTest.
 * 
 */
public class Process implements ProcessMBean {
  
  private final Object object = new Object();
  private final int pid;
  private final boolean process;
  private volatile boolean stop;
  
  public Process(int pid, boolean process) {
    this.pid = pid;
    this.process = process;
  }
  
  @Override
  public int getPid() {
    return this.pid;
  }
  
  public boolean isProcess() {
    return this.process;
  }
  
  @Override
  public void stop() {
    synchronized (this.object) {
      this.stop = true;
      this.object.notifyAll();
    }
  }
  
  public void waitUntilStopped() throws InterruptedException {
    synchronized (this.object) {
      while (!this.stop) {
        this.object.wait();
      }
    }
  }
}
