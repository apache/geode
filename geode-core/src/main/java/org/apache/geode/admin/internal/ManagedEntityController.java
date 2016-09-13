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
package org.apache.geode.admin.internal;

import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.admin.ManagedEntity;
import org.apache.geode.admin.ManagedEntityConfig;

/**
 * Defines the actual administration (starting, stopping, etc.) of
 * GemFire {@link ManagedEntity}s.
 * 
 */
interface ManagedEntityController {
  /**
   * Starts a managed entity.
   */
  public void start(final InternalManagedEntity entity);

  /**
   * Stops a managed entity.
   */
  public void stop(final InternalManagedEntity entity);

  /**
   * Returns whether or not a managed entity is running
   */
  public boolean isRunning(InternalManagedEntity entity);
  
  /**
   * Returns the contents of a locator's log file.  Other APIs are
   * used to get the log file of managed entities that are also system
   * members.
   */
  public String getLog(DistributionLocatorImpl locator);
  
  /**
   * Returns the full path to the executable in
   * <code>$GEMFIRE/bin</code> taking into account the {@linkplain
   * ManagedEntityConfig#getProductDirectory product directory} and the
   * platform's file separator.
   *
   * <P>
   *
   * Note: we should probably do a better job of determine whether or
   * not the machine on which the entity runs is Windows or Linux.
   *
   * @param executable
   *        The name of the executable that resides in
   *        <code>$GEMFIRE/bin</code>.
   */
  public String getProductExecutable(InternalManagedEntity entity, String executable);
  
  /**
   * Builds optional SSL command-line arguments.  Returns null if SSL is not
   * enabled for the distributed system.
   */
  public String buildSSLArguments(DistributedSystemConfig config);
}
