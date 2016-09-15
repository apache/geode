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
package org.apache.geode.admin;

/**
 * A entity that can be managed with the GemFire administration API.
 *
 * @see ManagedEntityConfig
 *
 * @since GemFire 4.0
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code> package instead
 */
public interface ManagedEntity {

  /**
   * Starts this managed entity.  Note that this method may return
   * before the managed entity is actually started.
   *
   * @throws AdminException
   *         If a problem is encountered while starting this managed
   *         entity. 
   * @throws IllegalStateException
   *         If this managed entity resides on a remote machine and a
   *         <code>null</code> or empty (<code>""</code>) {@linkplain
   *         ManagedEntityConfig#getRemoteCommand remote command} has
   *         been specified.
   *
   * @see #waitToStart
   */
  public void start() throws AdminException;

  /**
   * Stops this managed entity.  Note that this method may return
   * before the managed entity is actually stopped.
   *
   * @throws AdminException
   *         If a problem is encountered while stopping this managed
   *         entity. 
   * @throws IllegalStateException
   *         If this managed entity resides on a remote machine and a
   *         <code>null</code> or empty (<code>""</code>) {@linkplain
   *         ManagedEntityConfig#getRemoteCommand remote command} has
   *         been specified.
   *
   * @see #waitToStop
   */
  public void stop() throws AdminException;

  /**
   * Waits for up to a given number of milliseconds for this managed
   * entity to {@linkplain #start start}.
   *
   * @param timeout
   *        The number of milliseconds to wait for this managed entity
   *        to start.
   *
   * @return Whether or not the entity has started.
   *         <code>false</code>, if the method times out.
   *
   * @throws InterruptedException
   *         If the thread invoking this method is interrupted while
   *         waiting. 
   */
  public boolean waitToStart(long timeout)
    throws InterruptedException;

  /**
   * Waits for up to a given number of milliseconds for this managed
   * entity to {@linkplain #stop stop}.
   *
   * @param timeout
   *        The number of milliseconds to wait for this managed entity
   *        to stop.
   *
   * @return Whether or not the entity has stopped.
   *         <code>false</code>, if the method times out.
   *
   * @throws InterruptedException
   *         If the thread invoking this method is interrupted while
   *         waiting. 
   */
  public boolean waitToStop(long timeout)
    throws InterruptedException;

  /**
   * Returns whether or not this managed entity is running.  Note that
   * this operation may attempt to contact the managed entity.
   *
   * @throws IllegalStateException
   *         If this managed entity resides on a remote machine and a
   *         <code>null</code> or empty (<code>""</code>) {@linkplain
   *         ManagedEntityConfig#getRemoteCommand remote command} has
   *         been specified.
   */
  public boolean isRunning();

  /**
   * Returns the tail of this manage entity's log file.  Note that not
   * all managed entities implement this functionality.
   *
   * @throws AdminException
   *         If a problem is encountered while getting the log of this
   *         managed entity.
   * @throws UnsupportedOperationException
   *         If this managed entity does not support retrieving its
   *         log.
   */
  public String getLog() throws AdminException;

}
