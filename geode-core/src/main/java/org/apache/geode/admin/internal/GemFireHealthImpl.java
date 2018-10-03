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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.geode.CancelException;
import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.DistributedSystemHealthConfig;
import org.apache.geode.admin.GemFireHealth;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.admin.GfManagerAgent;
import org.apache.geode.internal.admin.HealthListener;
import org.apache.geode.internal.admin.JoinLeaveListener;

/**
 * Provides the implementation of the <code>GemFireHealth</code> administration API. This class is
 * responsible for {@linkplain GemFireVM#addHealthListener sending} the {@link GemFireHealthConfig}s
 * to the remote member VM in which the health is calcualted.
 *
 *
 * @since GemFire 3.5
 */
public class GemFireHealthImpl implements GemFireHealth, JoinLeaveListener, HealthListener {

  /** The distributed system whose health is being monitored */
  private final GfManagerAgent agent;

  /** The default configuration for checking GemFire health */
  protected GemFireHealthConfig defaultConfig;

  /**
   * Maps the name of a host to its <code>GemFireHealthConfig</code>. Note that the mappings are
   * created lazily.
   */
  private final Map hostConfigs;

  /**
   * Maps the name of a host to all of the members (<code>GemFireVM</code>s) that run on that host.
   */
  private final Map hostMembers;

  /** The members that are known to be in {@link #OKAY_HEALTH}. */
  private Collection okayHealth;

  /** The members that are known to be in {@link #POOR_HEALTH}. */
  private Collection poorHealth;

  /** The overall health of GemFire */
  private GemFireHealth.Health overallHealth;

  /** Is this GemFireHealthImpl closed? */
  private boolean isClosed;

  /**
   * The configuration specifying how the health of the distributed system should be computed.
   */
  protected volatile DistributedSystemHealthConfig dsHealthConfig;

  /** Monitors the health of the entire distributed system */
  private DistributedSystemHealthMonitor dsHealthMonitor = null;

  /**
   * The distributed system whose health is monitored by this <Code>GemFireHealth</code>.
   */
  private final AdminDistributedSystem system;


  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>GemFireHealthImpl</code> that monitors the health of member of the given
   * distributed system.
   */
  protected GemFireHealthImpl(GfManagerAgent agent, AdminDistributedSystem system) {
    this.agent = agent;
    this.system = system;

    this.hostConfigs = new HashMap();
    this.hostMembers = new HashMap();
    this.okayHealth = new HashSet();
    this.poorHealth = new HashSet();
    this.overallHealth = GOOD_HEALTH;
    this.isClosed = false;

    GemFireVM[] apps = this.agent.listApplications();
    for (int i = 0; i < apps.length; i++) {
      GemFireVM member = apps[i];
      this.noteNewMember(member);
    }

    agent.addJoinLeaveListener(this);
    setDefaultGemFireHealthConfig(createGemFireHealthConfig(null));
    setDistributedSystemHealthConfig(createDistributedSystemHealthConfig());
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("closed=" + isClosed);
    sb.append("; hostMembers=" + hostMembers);
    sb.append("; okayHealth=" + okayHealth);
    sb.append("; poorHealth=" + poorHealth);
    sb.append("; overallHealth=" + overallHealth);
    sb.append("; diagnosis=" + getDiagnosis());
    return sb.toString();
  }
  ////////////////////// Instance Methods //////////////////////

  /**
   * Returns the <code>DistributedSystem</code> whose health this <code>GemFireHealth</code>
   * monitors.
   */
  public AdminDistributedSystem getDistributedSystem() {
    return this.system;
  }

  /**
   * A "template factory" method for creating a <code>DistributedSystemHealthConfig</code>. It can
   * be overridden by subclasses to produce instances of different
   * <code>DistributedSystemHealthConfig</code> implementations.
   */
  protected DistributedSystemHealthConfig createDistributedSystemHealthConfig() {

    return new DistributedSystemHealthConfigImpl();
  }

  /**
   * A "template factory" method for creating a <code>GemFireHealthConfig</code>. It can be
   * overridden by subclasses to produce instances of different <code>GemFireHealthConfig</code>
   * implementations.
   *
   * @param hostName The host whose health we are configuring
   */
  protected GemFireHealthConfig createGemFireHealthConfig(String hostName) {

    return new GemFireHealthConfigImpl(hostName);
  }

  /**
   * Throws an {@link IllegalStateException} if this <code>GemFireHealthImpl</code> is closed.
   */
  private void checkClosed() {
    if (this.isClosed) {
      throw new IllegalStateException(
          "Cannot access a closed GemFireHealth instance.");
    }
  }

  /**
   * Returns the overall health of GemFire. Note that this method does not contact any of the member
   * VMs. Instead, it relies on the members to alert it of changes in its health via a
   * {@link HealthListener}.
   */
  public GemFireHealth.Health getHealth() {
    checkClosed();
    return this.overallHealth;
  }

  /**
   * Resets the overall health to be {@link #GOOD_HEALTH}. It also resets the health in the member
   * VMs.
   *
   * @see GemFireVM#resetHealthStatus
   */
  public void resetHealth() {
    checkClosed();

    this.overallHealth = GOOD_HEALTH;
    this.okayHealth.clear();
    this.poorHealth.clear();

    synchronized (this) {
      for (Iterator iter = hostMembers.values().iterator(); iter.hasNext();) {
        List members = (List) iter.next();
        for (Iterator iter2 = members.iterator(); iter2.hasNext();) {
          GemFireVM member = (GemFireVM) iter2.next();
          member.resetHealthStatus();
        }
      }
    }
  }

  /**
   * Aggregates the diagnoses from all members of the distributed system.
   */
  public String getDiagnosis() {
    checkClosed();

    StringBuffer sb = new StringBuffer();

    synchronized (this) {
      for (Iterator iter = hostMembers.values().iterator(); iter.hasNext();) {
        List members = (List) iter.next();
        for (Iterator iter2 = members.iterator(); iter2.hasNext();) {
          GemFireVM member = (GemFireVM) iter2.next();
          String[] diagnoses = member.getHealthDiagnosis(this.overallHealth);
          for (int i = 0; i < diagnoses.length; i++) {
            sb.append(diagnoses[i]).append("\n");;
          }
        }
      }
    }

    return sb.toString();
  }

  /**
   * Starts a new {@link DistributedSystemHealthMonitor}
   */
  public void setDistributedSystemHealthConfig(DistributedSystemHealthConfig config) {
    synchronized (this.hostConfigs) {
      // If too many threads are changing the health config, then we
      // will might get an OutOfMemoryError trying to start a new
      // health monitor thread.

      if (this.dsHealthMonitor != null) {
        this.dsHealthMonitor.stop();
      }

      this.dsHealthConfig = config;

      DistributedSystemHealthEvaluator eval =
          new DistributedSystemHealthEvaluator(config, this.agent.getDM());
      int interval = this.getDefaultGemFireHealthConfig().getHealthEvaluationInterval();
      this.dsHealthMonitor = new DistributedSystemHealthMonitor(eval, this, interval);
      this.dsHealthMonitor.start();
    }
  }

  public DistributedSystemHealthConfig getDistributedSystemHealthConfig() {

    checkClosed();
    return this.dsHealthConfig;
  }

  public GemFireHealthConfig getDefaultGemFireHealthConfig() {
    checkClosed();
    return this.defaultConfig;
  }

  public void setDefaultGemFireHealthConfig(GemFireHealthConfig config) {
    checkClosed();

    if (config.getHostName() != null) {
      throw new IllegalArgumentException(
          String.format("The GemFireHealthConfig for %s cannot serve as the default health config.",
              config.getHostName()));
    }

    this.defaultConfig = config;

    synchronized (this) {
      for (Iterator iter = this.hostMembers.entrySet().iterator(); iter.hasNext();) {
        Map.Entry entry = (Map.Entry) iter.next();
        InetAddress hostIpAddress = (InetAddress) entry.getKey();
        List members = (List) entry.getValue();

        GemFireHealthConfig hostConfig = (GemFireHealthConfig) hostConfigs.get(hostIpAddress);
        if (hostConfig == null) {
          hostConfig = config;
        }

        for (Iterator iter2 = members.iterator(); iter2.hasNext();) {
          GemFireVM member = (GemFireVM) iter2.next();
          Assert.assertTrue(member.getHost().equals(hostIpAddress));
          member.addHealthListener(this, hostConfig);
        }
      }
    }

    // We only need to do this if the health monitoring interval has
    // change. This is probably not the most efficient way of doing
    // things.
    if (this.dsHealthConfig != null) {
      setDistributedSystemHealthConfig(this.dsHealthConfig);
    }
  }

  /**
   * Returns the GemFireHealthConfig object for the given host name.
   *
   * @param hostName host name for which the GemFire Health Config is needed
   *
   * @throws IllegalArgumentException if host with given name could not be found
   */
  public synchronized GemFireHealthConfig getGemFireHealthConfig(String hostName) {

    checkClosed();

    InetAddress hostIpAddress = null;
    try {
      hostIpAddress = InetAddress.getByName(hostName);
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(
          String.format("Could not find a host with name %s.",
              hostName),
          e);
    }

    GemFireHealthConfig config = (GemFireHealthConfig) this.hostConfigs.get(hostIpAddress);
    if (config == null) {
      config = createGemFireHealthConfig(hostName);
      this.hostConfigs.put(hostIpAddress, config);
    }

    return config;
  }

  /**
   * Sets the GemFireHealthConfig object for the given host name.
   *
   * @param hostName host name for which the GemFire Health Config is needed
   * @param config GemFireHealthConfig object to set
   *
   * @throws IllegalArgumentException if (1) given host name & the host name in the given config do
   *         not match OR (2) host with given name could not be found OR (3) there are no GemFire
   *         components running on the given host
   */
  public void setGemFireHealthConfig(String hostName, GemFireHealthConfig config) {
    checkClosed();

    synchronized (this) {
      String configHost = config.getHostName();
      if (configHost == null || !configHost.equals(hostName)) {
        StringBuffer sb = new StringBuffer();
        sb.append("The GemFireHealthConfig configures ");
        if (configHost == null) {
          sb.append("the default host ");

        } else {
          sb.append("host \"");
          sb.append(config.getHostName());
          sb.append("\" ");
        }
        sb.append("not \"" + hostName + "\"");
        throw new IllegalArgumentException(sb.toString());
      }
      InetAddress hostIpAddress = null;
      try {
        hostIpAddress = InetAddress.getByName(hostName);
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException(
            String.format("Could not find a host with name %s.",
                hostName),
            e);
      }

      List members = (List) this.hostMembers.get(hostIpAddress);
      if (members == null || members.isEmpty()) {
        throw new IllegalArgumentException(
            String.format("There are no GemFire components on host %s.",
                hostName));
      }

      for (Iterator iter = members.iterator(); iter.hasNext();) {
        GemFireVM member = (GemFireVM) iter.next();
        member.addHealthListener(this, config);
      }
    }
  }

  /**
   * Tells the members of the distributed system that we are no longer interested in monitoring
   * their health.
   *
   * @see GemFireVM#removeHealthListener
   */
  public void close() {
    this.agent.removeJoinLeaveListener(this);

    synchronized (this) {
      if (this.isClosed) {
        return;
      }

      this.isClosed = true;

      if (this.dsHealthMonitor != null) {
        this.dsHealthMonitor.stop();
        this.dsHealthMonitor = null;
      }

      try {
        for (Iterator iter = hostMembers.values().iterator(); iter.hasNext();) {
          List members = (List) iter.next();
          for (Iterator iter2 = members.iterator(); iter2.hasNext();) {
            GemFireVM member = (GemFireVM) iter2.next();
            member.removeHealthListener();
          }
        }
      } catch (CancelException e) {
        // if the DS is disconnected, stop trying to distribute to other members
      }

      hostConfigs.clear();
      hostMembers.clear();
      okayHealth.clear();
      poorHealth.clear();
    }
  }

  public boolean isClosed() {
    return this.isClosed;
  }

  /**
   * Makes note of the newly-joined member
   */
  private void noteNewMember(GemFireVM member) {
    InetAddress hostIpAddress = member.getHost();
    List members = (List) this.hostMembers.get(hostIpAddress);
    if (members == null) {
      members = new ArrayList();
      this.hostMembers.put(hostIpAddress, members);
    }
    members.add(member);

  }

  public synchronized void nodeJoined(GfManagerAgent source, GemFireVM joined) {
    noteNewMember(joined);

    InetAddress hostIpAddress = joined.getHost();

    GemFireHealthConfig config = (GemFireHealthConfig) this.hostConfigs.get(hostIpAddress);
    if (config == null) {
      config = this.getDefaultGemFireHealthConfig();
    }
    joined.addHealthListener(this, config);
  }

  /**
   * Makes note of the newly-left member
   */
  public synchronized void nodeLeft(GfManagerAgent source, GemFireVM left) {
    InetAddress hostIpAddress = left.getHost();
    List members = (List) this.hostMembers.get(hostIpAddress);
    if (members != null) {
      members.remove(left);
      if (members.isEmpty()) {
        // No more members on the host
        this.hostConfigs.remove(hostIpAddress);
        this.hostMembers.remove(hostIpAddress);
      }
    }

    this.okayHealth.remove(left);
    this.poorHealth.remove(left);

    reevaluateHealth();
  }

  /**
   * Does the same thing as {@link #nodeLeft}
   */
  public void nodeCrashed(GfManagerAgent source, GemFireVM crashed) {
    nodeLeft(source, crashed);
  }

  /**
   * Re-evaluates the overall health of GemFire
   */
  private void reevaluateHealth() {
    if (!this.poorHealth.isEmpty()) {
      this.overallHealth = POOR_HEALTH;

    } else if (!this.okayHealth.isEmpty()) {
      this.overallHealth = OKAY_HEALTH;

    } else {
      this.overallHealth = GOOD_HEALTH;
    }
  }

  public void healthChanged(GemFireVM member, GemFireHealth.Health status) {
    if (status == GOOD_HEALTH) {
      this.okayHealth.remove(member);
      this.poorHealth.remove(member);

    } else if (status == OKAY_HEALTH) {
      this.okayHealth.add(member);
      this.poorHealth.remove(member);

    } else if (status == POOR_HEALTH) {
      this.okayHealth.remove(member);
      this.poorHealth.add(member);

    } else {
      Assert.assertTrue(false, "Unknown health code: " + status);
    }

    reevaluateHealth();
  }

}
