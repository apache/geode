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
package com.gemstone.gemfire.admin;

/**
 * Common configuration for all entities that can be managed using the
 * GemFire administration API.  Note that once a managed entity has
 * been {@linkplain ManagedEntity#start started}, attempts to modify
 * its configuration will cause an {@link IllegalStateException} to be
 * thrown.
 *
 * @see ManagedEntity
 *
 * @since GemFire 4.0
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/com/gemstone/gemfire/management/package-summary.html">management</a></code> package instead
 */
public interface ManagedEntityConfig extends Cloneable {

  /**
   * Returns the name of the host on which the managed entity runs or
   * will run.
   */
  public String getHost();

  /**
   * Sets the name of the host on which the managed entity will run.
   */
  public void setHost(String host);

  /**
   * Returns the name of the working directory in which the managed
   * entity runs or will run.
   */
  public String getWorkingDirectory();

  /**
   * Sets the name of the working directory in which the managed
   * entity will run.
   */
  public void setWorkingDirectory(String dir);

  /**
   * Returns the name of the GemFire product directory to use when
   * administering the managed entity.
   */
  public String getProductDirectory();

  /**
   * Sets the name of the GemFire product directory to use when
   * administering the managed entity.
   */
  public void setProductDirectory(String dir);

  /**
   * Returns the command prefix used to administer a managed entity
   * that is hosted on a remote machine.  If the remote command is
   * <code>null</code> (the default value), then the remote command
   * associated with the {@linkplain
   * AdminDistributedSystem#getRemoteCommand() distributed system}
   * will be used.
   */
  public String getRemoteCommand();

  /**
   * Sets the command prefix used to administer a managed entity that
   * is hosted on a remote machine.
   */
  public void setRemoteCommand(String remoteCommand);

  /**
   * Validates this configuration.
   *
   * @throws IllegalStateException
   *         If a managed entity cannot be administered using this
   *         configuration 
   */
  public void validate();

  /**
   * Returns a new <code>ManagedEntityConfig</code> with the same
   * configuration as this <code>ManagedEntityConfig</code>.
   */
  public Object clone() throws CloneNotSupportedException; 

}
