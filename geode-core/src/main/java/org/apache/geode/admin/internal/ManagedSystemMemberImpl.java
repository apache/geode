/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.admin.internal;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;

import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.ConfigurationParameter;
import org.apache.geode.admin.ManagedEntityConfig;
import org.apache.geode.internal.admin.GemFireVM;

/**
 * A <code>SystemMember</code> that is also managed (or manageable) by the admin API.
 *
 * This class must be public so that its methods can be invoked reflectively (for MBean operations)
 * on instances of its subclasses.
 *
 * @since GemFire 4.0
 */
public abstract class ManagedSystemMemberImpl extends SystemMemberImpl
    implements InternalManagedEntity {

  /** Controller for starting and stopping local or remote managers */
  protected ManagedEntityController controller;

  /** The state of this managed entity (see bug 32455) */
  private int state = UNKNOWN;

  /** A lock that is obtained while this entity's state changes */
  private final Object stateChange = new Object();

  ////////////////////// Constructors //////////////////////

  /**
   * Creates a new <code>ManagedSystemMemberImpl</code> that represents an existing member of an
   * <code>AdminDistributedSystem</code>.
   */
  protected ManagedSystemMemberImpl(AdminDistributedSystemImpl system, GemFireVM vm)
      throws AdminException {

    super(system, vm);
    this.controller = system.getEntityController();
  }

  /**
   * Creates a new <code>ManagedSystemMemberImpl</code> that represents a non-existing member with
   * the given <code>ManagedEntityConfig</code> that has not yet been started.
   */
  protected ManagedSystemMemberImpl(AdminDistributedSystemImpl system, ManagedEntityConfig config)
      throws AdminException {

    super(system);
    this.internalId = null;
    this.id = getNewId();
    this.host = config.getHost();
    this.name = this.id;
    this.controller = system.getEntityController();
  }

  ////////////////////// Instance Methods //////////////////////

  public String getWorkingDirectory() {
    return this.getEntityConfig().getWorkingDirectory();
  }

  public void setWorkingDirectory(String workingDirectory) {
    this.getEntityConfig().setWorkingDirectory(workingDirectory);
  }

  public String getProductDirectory() {
    return this.getEntityConfig().getProductDirectory();
  }

  public void setProductDirectory(String productDirectory) {
    this.getEntityConfig().setProductDirectory(productDirectory);
  }

  @Override
  public String getHost() {
    return this.getEntityConfig().getHost();
  }

  @Override
  public int setState(int state) {

    synchronized (this.stateChange) {
      int oldState = this.state;
      this.state = state;

      this.stateChange.notifyAll();

      return oldState;
    }

  }

  /**
   * Returns whether or not this managed system member needs to be stopped. If this member is
   * stopped or is stopping, then it does not need to be stopped. Otherwise, it will atomically
   * place this member in the {@link #STOPPING} state. See bug 32455.
   */
  protected boolean needToStop() {
    synchronized (this.stateChange) {
      if (this.state == STOPPED || this.state == STOPPING) {
        return false;

      } else {
        setState(STOPPING);
        return true;
      }
    }
  }

  /**
   * Returns whether or not this managed system member needs to be started. If this member is
   * started or is starting, then it does not need to be started. Otherwise, it will atomically
   * place this member in the {@link #STARTING} state. See bug 32455.
   */
  protected boolean needToStart() {
    synchronized (this.stateChange) {
      if (this.state == RUNNING || this.state == STARTING) {
        return false;

      } else {
        setState(STARTING);
        return true;
      }
    }
  }

  /**
   * Sets the state of this managed system member depending on whether or not <code>vm</code> is
   * <code>null</code>.
   */
  @Override
  void setGemFireVM(GemFireVM vm) throws AdminException {
    super.setGemFireVM(vm);
    if (vm != null) {
      this.setState(RUNNING);

    } else {
      this.setState(STOPPED);
    }
  }

  /**
   * Waits until this system member's "state" is {@link #RUNNING}.
   */
  @Override
  public boolean waitToStart(long timeout) throws InterruptedException {

    if (Thread.interrupted())
      throw new InterruptedException();

    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      synchronized (this.stateChange) {
        if (this.state == RUNNING) {
          break;

        } else {
          this.stateChange.wait(System.currentTimeMillis() - start);
        }
      }
    }

    synchronized (this.stateChange) {
      return this.state == RUNNING;
    }
  }

  /**
   * Waits until this system member's "state" is {@link #STOPPED}.
   */
  @Override
  public boolean waitToStop(long timeout) throws InterruptedException {

    if (Thread.interrupted())
      throw new InterruptedException();
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      synchronized (this.stateChange) {
        if (this.state == STOPPED) {
          break;

        } else {
          this.stateChange.wait(System.currentTimeMillis() - start);
        }
      }
    }

    synchronized (this.stateChange) {
      return this.state == STOPPED;
    }
  }

  /**
   * Appends configuration information to a <code>StringBuffer</code> that contains a command line.
   * Handles certain configuration parameters specially.
   */
  protected void appendConfiguration(StringBuffer sb) {
    ConfigurationParameter[] params = this.getConfiguration();
    for (int i = 0; i < params.length; i++) {
      ConfigurationParameter param = params[i];

      if (!param.isModifiable()) {
        continue;
      }

      String name = param.getName();
      String value = param.getValueAsString();

      if (value != null && !value.equals("")) {
        if (name.equals(LOCATORS)) {
          // Use the new locator syntax so that is plays nicely with
          // rsh. See bug 32306.
          String locator = value;
          int firstBracket = locator.indexOf('[');
          int lastBracket = locator.indexOf(']');

          if (firstBracket > -1 && lastBracket > -1) {
            String host = locator.substring(0, firstBracket);
            String port = locator.substring(firstBracket + 1, lastBracket);
            locator = host + ":" + port;
          }

          sb.append(" ");
          sb.append(name);
          sb.append("=");
          sb.append(locator);

        } else {
          sb.append(" ");
          sb.append(name);
          sb.append("=");
          sb.append(value);
        }
      }
    }
  }

}
