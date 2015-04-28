/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

/**
 * A entity that can be managed with the GemFire administration API.
 *
 * @see ManagedEntityConfig
 *
 * @author David Whitlock
 * @since 4.0
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
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
