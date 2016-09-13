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
package com.gemstone.gemfire.management;

import javax.management.JMException;

import com.gemstone.gemfire.management.internal.Manager;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * MBean that provides access to information and management functionality for a
 * {@link Manager}.
 * 
 * @since GemFire 7.0
 * 
 */
@ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
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
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  public boolean start() throws JMException;

  /**
   * Stops the manager service.
   * 
   * @return True if the manager service was successfully stopped, false otherwise.
   */
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
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
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.WRITE)
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
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.WRITE)
  public void setStatusMessage(String message);
}
