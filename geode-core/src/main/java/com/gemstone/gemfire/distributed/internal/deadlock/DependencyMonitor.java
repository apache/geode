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
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.io.Serializable;
import java.util.Set;

/**
 * This interface defines the contract between the {@link DependencyMonitorManager} class
 * and classes that monitor a particular type of dependency. Classes implementing
 * this interface should register themselves with the dependency monitor using
 * the {@link DependencyMonitorManager#addMonitor(DependencyMonitor)} method.
 * 
 */
public interface DependencyMonitor {
  /**
   * Return a map of resource identifiers to the threads that are blocked
   * waiting for those resources.
   */
  public Set<Dependency<Thread, Serializable>> getBlockedThreads(
      Thread[] allThreads);

  /**
   * Return a map of resource indentifiers to the threads that hold that
   * particular resource.
   */
  public Set<Dependency<Serializable, Thread>> getHeldResources(
      Thread[] allThreads);

}
