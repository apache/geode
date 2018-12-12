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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.DistributionLocator;
import org.apache.geode.admin.DistributionLocatorConfig;
import org.apache.geode.admin.ManagedEntityConfig;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.logging.LogService;

/**
 * Default administrative implementation of a DistributionLocator.
 *
 * @since GemFire 3.5
 */
public class DistributionLocatorImpl implements DistributionLocator, InternalManagedEntity {

  private static final Logger logger = LogService.getLogger();

  /**
   * How many new <code>DistributionLocator</code>s have been created?
   */
  private static int newLocators = 0;

  //////////////////// Instance Fields ////////////////////

  /**
   * The configuration object for this locator
   */
  private final DistributionLocatorConfigImpl config;

  /**
   * The id of this distribution locator
   */
  private final String id;

  /**
   * Used to control the actual DistributionLocator service
   */
  private ManagedEntityController controller;

  /**
   * The system that this locator is a part of
   */
  private AdminDistributedSystemImpl system;

  // -------------------------------------------------------------------------
  // constructor(s)...
  // -------------------------------------------------------------------------

  /**
   * Constructs new instance of <code>DistributionLocatorImpl</code> that is a member of the given
   * distributed system.
   */
  public DistributionLocatorImpl(DistributionLocatorConfig config,
      AdminDistributedSystemImpl system) {
    this.config = (DistributionLocatorConfigImpl) config;
    this.config.validate();
    this.config.setManagedEntity(this);
    this.id = getNewId();
    this.controller = system.getEntityController();
    this.system = system;
  }

  // -------------------------------------------------------------------------
  // Attribute accessors/mutators...
  // -------------------------------------------------------------------------

  public String getId() {
    return this.id;
  }

  public String getNewId() {
    synchronized (DistributionLocatorImpl.class) {
      return "Locator" + (++newLocators);
    }
  }

  /**
   * Returns the configuration object for this locator.
   *
   * @since GemFire 4.0
   */
  public DistributionLocatorConfig getConfig() {
    return this.config;
  }

  public AdminDistributedSystem getDistributedSystem() {
    return this.system;
  }

  /**
   * Unfortunately, it doesn't make much sense to maintain the state of a locator. The admin API
   * does not receive notification when the locator actually starts and stops. If we try to guess,
   * we'll just end up with race conditions galore. So, we can't fix bug 32455 for locators.
   */
  public int setState(int state) {
    throw new UnsupportedOperationException(
        "Can not set the state of a locator.");
  }

  // -------------------------------------------------------------------------
  // Operations...
  // -------------------------------------------------------------------------

  /**
   * Polls to determine whether or not this managed entity has started.
   */
  public boolean waitToStart(long timeout) throws InterruptedException {

    if (Thread.interrupted())
      throw new InterruptedException();

    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      if (this.isRunning()) {
        return true;

      } else {
        Thread.sleep(100);
      }
    }

    logger.info("Done waiting for locator");
    return this.isRunning();
  }

  /**
   * Polls to determine whether or not this managed entity has stopped.
   */
  public boolean waitToStop(long timeout) throws InterruptedException {

    if (Thread.interrupted())
      throw new InterruptedException();

    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      if (!this.isRunning()) {
        return true;

      } else {
        Thread.sleep(100);
      }
    }

    return !this.isRunning();
  }

  public boolean isRunning() {
    DistributionManager dm =
        ((AdminDistributedSystemImpl) getDistributedSystem()).getDistributionManager();
    if (dm == null) {
      try {
        return this.controller.isRunning(this);
      } catch (IllegalStateException e) {
        return false;
      }
    }

    String host = getConfig().getHost();
    int port = getConfig().getPort();
    String bindAddress = getConfig().getBindAddress();

    boolean found = false;
    Map<InternalDistributedMember, Collection<String>> hostedLocators = dm.getAllHostedLocators();
    for (Iterator<InternalDistributedMember> memberIter =
        hostedLocators.keySet().iterator(); memberIter.hasNext();) {
      for (Iterator<String> locatorIter =
          hostedLocators.get(memberIter.next()).iterator(); locatorIter.hasNext();) {
        DistributionLocatorId locator = new DistributionLocatorId(locatorIter.next());
        found = found || locator.getHostName().equals(host);
        if (!found && !host.contains(".")) {
          try {
            InetAddress inetAddr = InetAddress.getByName(host);
            found = locator.getHost().getHostName().equals(inetAddr.getHostName());
            if (!found) {
              found =
                  locator.getHost().getAddress().getHostAddress().equals(inetAddr.getHostAddress());
            }
          } catch (UnknownHostException e) {
            // try config host as if it is an IP address instead of host name
          }
        }
        if (locator.getBindAddress() != null && !locator.getBindAddress().isEmpty()
            && bindAddress != null && !bindAddress.isEmpty()) {
          found = found && locator.getBindAddress().equals(bindAddress);
        }
        found = found && locator.getPort() == port;
        if (found) {
          return true;
        }
      }
    }
    return found;
  }

  public void start() {
    this.config.validate();
    this.controller.start(this);
    this.config.setLocator(this);
    this.system.updateLocatorsString();
  }

  public void stop() {
    this.controller.stop(this);
    this.config.setLocator(null);
  }

  public String getLog() {
    return this.controller.getLog(this);
  }

  /**
   * Returns a string representation of the object.
   *
   * @return a string representation of the object
   */
  @Override
  public String toString() {
    return "DistributionLocator " + getId();
  }

  //////////////////////// Command execution ////////////////////////

  public ManagedEntityConfig getEntityConfig() {
    return this.getConfig();
  }

  public String getEntityType() {
    return "Locator";
  }

  public String getStartCommand() {
    StringBuffer sb = new StringBuffer();
    sb.append(this.controller.getProductExecutable(this, "gemfire"));
    sb.append(" start-locator -q -dir=");
    sb.append(this.getConfig().getWorkingDirectory());
    sb.append(" -port=");
    sb.append(this.getConfig().getPort());
    Properties props = config.getDistributedSystemProperties();
    Enumeration en = props.propertyNames();
    while (en.hasMoreElements()) {
      String pn = (String) en.nextElement();
      sb.append(" -D" + DistributionConfig.GEMFIRE_PREFIX + "" + pn + "=" + props.getProperty(pn));
    }

    String bindAddress = this.getConfig().getBindAddress();
    if (bindAddress != null && bindAddress.length() > 0) {
      sb.append(" -address=");
      sb.append(this.getConfig().getBindAddress());
    }
    sb.append(" ");

    String sslArgs = this.controller.buildSSLArguments(this.system.getConfig());
    if (sslArgs != null) {
      sb.append(sslArgs);
    }

    return sb.toString().trim();
  }

  public String getStopCommand() {
    StringBuffer sb = new StringBuffer();
    sb.append(this.controller.getProductExecutable(this, "gemfire"));
    sb.append(" stop-locator -q -dir=");
    sb.append(this.getConfig().getWorkingDirectory());
    sb.append(" -port=");
    sb.append(this.getConfig().getPort());

    String bindAddress = this.getConfig().getBindAddress();
    if (bindAddress != null && bindAddress.length() > 0) {
      sb.append(" -address=");
      sb.append(this.getConfig().getBindAddress());
    }
    sb.append(" ");

    String sslArgs = this.controller.buildSSLArguments(this.system.getConfig());
    if (sslArgs != null) {
      sb.append(sslArgs);
    }

    return sb.toString().trim();
  }

  public String getIsRunningCommand() {
    StringBuffer sb = new StringBuffer();
    sb.append(this.controller.getProductExecutable(this, "gemfire"));
    sb.append(" status-locator -dir=");
    sb.append(this.getConfig().getWorkingDirectory());

    return sb.toString().trim();
  }

  public String getLogCommand() {
    StringBuffer sb = new StringBuffer();
    sb.append(this.controller.getProductExecutable(this, "gemfire"));
    sb.append(" tail-locator-log -dir=");
    sb.append(this.getConfig().getWorkingDirectory());

    return sb.toString().trim();
  }

}
