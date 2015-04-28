/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management;

import javax.management.JMException;

import com.gemstone.gemfire.management.internal.Manager;


/**
 * MBean that provides access to information and management functionality for a
 * {@link Manager}.
 * 
 * @author rishim
 * @since 7.0
 * 
 */
public interface ManagerMXBean {

  /**
   * Returns whether the manager service is running on this member.
   * 
   * @return True of the manager service is running, false otherwise.
   */
  public boolean isRunning();

  /**
   * Starts the manager service.
   * 
   * @return True if the manager service was successfully started, false otherwise.
   */
  public boolean start() throws JMException;

  /**
   * Stops the manager service.
   * 
   * @return True if the manager service was successfully stopped, false otherwise.
   */
  public boolean stop() throws JMException;

  /**
   * Returns the URL for connecting to the Pulse application.
   */
  public String getPulseURL();
  
  /**
   * Sets the URL for the Pulse application.
   * 
   * @param pulseURL
   *          The URL for the Pulse application.
   */
  public void setPulseURL(String pulseURL);

  /**
   * Returns the last set status message. Generally, a subcomponent will call
   * setStatusMessage to save the result of its execution.  For example, if
   * the embedded HTTP server failed to start, the reason for that failure will
   * be saved here.
   */
  public String getStatusMessage();

  /**
   * Sets the status message.
   * 
   * @param message
   *          The status message.
   */
  public void setStatusMessage(String message);
}
