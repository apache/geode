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
@Deprecated
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
    controller = system.getEntityController();
  }

  /**
   * Creates a new <code>ManagedSystemMemberImpl</code> that represents a non-existing member with
   * the given <code>ManagedEntityConfig</code> that has not yet been started.
   */
  protected ManagedSystemMemberImpl(AdminDistributedSystemImpl system, ManagedEntityConfig config)
      throws AdminException {

    super(system);
    internalId = null;
    id = getNewId();
    host = config.getHost();
    name = id;
    controller = system.getEntityController();
  }

  ////////////////////// Instance Methods //////////////////////

  public String getWorkingDirectory() {
    return getEntityConfig().getWorkingDirectory();
  }

  public void setWorkingDirectory(String workingDirectory) {
    getEntityConfig().setWorkingDirectory(workingDirectory);
  }

  public String getProductDirectory() {
    return getEntityConfig().getProductDirectory();
  }

  public void setProductDirectory(String productDirectory) {
    getEntityConfig().setProductDirectory(productDirectory);
  }

  @Override
  public String getHost() {
    return getEntityConfig().getHost();
  }

  @Override
  public int setState(int state) {

    synchronized (stateChange) {
      int oldState = this.state;
      this.state = state;

      stateChange.notifyAll();

      return oldState;
    }

  }

  /**
   * Returns whether or not this managed system member needs to be stopped. If this member is
   * stopped or is stopping, then it does not need to be stopped. Otherwise, it will atomically
   * place this member in the {@link #STOPPING} state. See bug 32455.
   */
  protected boolean needToStop() {
    synchronized (stateChange) {
      if (state == STOPPED || state == STOPPING) {
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
    synchronized (stateChange) {
      if (state == RUNNING || state == STARTING) {
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
      setState(RUNNING);

    } else {
      setState(STOPPED);
    }
  }

  /**
   * Waits until this system member's "state" is {@link #RUNNING}.
   */
  @Override
  public boolean waitToStart(long timeout) throws InterruptedException {

    if (Thread.interrupted()) {
      throw new InterruptedException();
    }

    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      synchronized (stateChange) {
        if (state == RUNNING) {
          break;

        } else {
          stateChange.wait(System.currentTimeMillis() - start);
        }
      }
    }

    synchronized (stateChange) {
      return state == RUNNING;
    }
  }

  /**
   * Waits until this system member's "state" is {@link #STOPPED}.
   */
  @Override
  public boolean waitToStop(long timeout) throws InterruptedException {

    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      synchronized (stateChange) {
        if (state == STOPPED) {
          break;

        } else {
          stateChange.wait(System.currentTimeMillis() - start);
        }
      }
    }

    synchronized (stateChange) {
      return state == STOPPED;
    }
  }

  /**
   * Appends configuration information to a <code>StringBuilder</code> that contains a command line.
   * Handles certain configuration parameters specially.
   */
  protected void appendConfiguration(StringBuilder sb) {
    ConfigurationParameter[] params = getConfiguration();
    for (ConfigurationParameter param : params) {
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
