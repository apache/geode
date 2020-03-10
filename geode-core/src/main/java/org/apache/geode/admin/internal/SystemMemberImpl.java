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

import static org.apache.geode.admin.internal.InetAddressUtils.toHostString;
import static org.apache.geode.admin.internal.InetAddressUtilsWithLogging.toInetAddress;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.CacheDoesNotExistException;
import org.apache.geode.admin.ConfigurationParameter;
import org.apache.geode.admin.RuntimeAdminException;
import org.apache.geode.admin.StatisticResource;
import org.apache.geode.admin.SystemMemberCache;
import org.apache.geode.admin.SystemMemberType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Config;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.admin.StatResource;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Member of a GemFire system.
 *
 * @since GemFire 3.5
 */
public class SystemMemberImpl implements org.apache.geode.admin.SystemMember,
    org.apache.geode.admin.internal.ConfigurationParameterListener {

  private static final Logger logger = LogService.getLogger();

  /**
   * Identifying name of this member. Note that by default this is the string form of internalId but
   * the ManagedSystemMemberImpl subclass resets it to getNewId()
   */
  protected String id;

  /** Unique internal id that the system impl identifies this member with */
  protected InternalDistributedMember internalId;

  /** The name of this system member */
  protected String name;

  /** Host name of the machine this member resides on */
  protected String host;

  /**
   * The configuration parameters for this member. Maps the name of the ConfigurationParameter to
   * the ConfigurationParameter.
   */
  protected Map parms = new HashMap();

  /** The {@link AdminDistributedSystem} this is a member of */
  protected AdminDistributedSystem system;

  /** Internal GemFire vm to delegate to */
  private GemFireVM vm;

  // -------------------------------------------------------------------------
  // Constructor(s)
  // -------------------------------------------------------------------------

  /**
   * Constructs new <code>SystemMemberImpl</code> for a <code>ManagedEntity</code> that has yet to
   * be started.
   *
   * @param system the distributed system this member belongs to
   */
  protected SystemMemberImpl(AdminDistributedSystem system) throws AdminException {

    this.system = system;
    refreshConfig(getDefaultConfig());
  }

  /**
   * Constructs new <code>SystemMemberImpl</code> from the given <code>GemFireVM</code>. This
   * constructor is invoked when we discover a new member of the distributed system.
   *
   * @param system the distributed system this member belongs to
   * @param vm internal GemFire vm to delegate to
   */
  public SystemMemberImpl(AdminDistributedSystem system, GemFireVM vm) throws AdminException {

    this(system);
    setGemFireVM(vm);
  }

  /**
   * Constructs the instance of SystemMember using the corresponding InternalDistributedMember
   * instance of a DS member for the given AdminDistributedSystem.
   *
   * @param system Current AdminDistributedSystem instance
   * @param member InternalDistributedMember instance for which a SystemMember instance is to be
   *        constructed.
   * @throws AdminException if construction of SystemMember fails
   *
   * @since GemFire 6.5
   */
  protected SystemMemberImpl(AdminDistributedSystem system, InternalDistributedMember member)
      throws AdminException {
    this(system);
    updateByInternalDistributedMember(member);
  }

  // -------------------------------------------------------------------------
  // Attribute accessors and mutators
  // -------------------------------------------------------------------------

  /**
   * Returns a <code>Config</code> object with the appropriate default values for a newly-created
   * system member.
   */
  protected Config getDefaultConfig() {
    Properties props = new Properties();
    return new DistributionConfigImpl(props);
  }

  @Override
  public AdminDistributedSystem getDistributedSystem() {
    return this.system;
  }

  public InternalDistributedMember getInternalId() {
    return internalId;
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public String getHost() {
    return this.host;
  }

  @Override
  public InetAddress getHostAddress() {
    return toInetAddress(this.getHost());
  }

  // -------------------------------------------------------------------------
  // Operations
  // -------------------------------------------------------------------------

  @Override
  public String getLog() {
    String childTail = null;
    String mainTail = null;
    GemFireVM vm = getGemFireVM();
    if (vm != null) {
      String[] log = vm.getSystemLogs();
      if (log != null && log.length > 0)
        mainTail = log[0];
      if (log != null && log.length > 1)
        childTail = log[1];
    }

    if (childTail == null && mainTail == null) {
      return "No log file configured, log messages will be directed to stdout.";
    } else {
      StringBuffer result = new StringBuffer();
      if (mainTail != null) {
        result.append(mainTail);
      }
      if (childTail != null) {
        result.append(
            "\n" + "-------------------- tail of child log --------------------" + "\n");
        result.append(childTail);
      }
      return result.toString();
    }
  }

  @Override
  public java.util.Properties getLicense() {
    GemFireVM vm = getGemFireVM();
    if (vm == null)
      return null;
    return new Properties();
  }

  @Override
  public String getVersion() {
    GemFireVM vm = getGemFireVM();
    if (vm == null)
      return null;
    return vm.getVersionInfo();
  }

  @Override
  public StatisticResource[] getStat(String statisticsTypeName)
      throws org.apache.geode.admin.AdminException {
    StatisticResource[] res = new StatisticResource[0];
    if (this.vm != null) {
      res = getStatsImpl(this.vm.getStats(statisticsTypeName));
    }
    return res.length == 0 ? null : res;
  }

  @Override
  public StatisticResource[] getStats() throws org.apache.geode.admin.AdminException {
    StatisticResource[] statsImpl = new StatisticResource[0];
    if (this.vm != null) {
      statsImpl = getStatsImpl(this.vm.getStats(null));
    }
    return statsImpl;
  }

  @Override
  public boolean hasCache() {
    GemFireVM member = getGemFireVM();
    if (member == null) {
      return false;

    } else {
      return member.getCacheInfo() != null;
    }
  }

  @Override
  public SystemMemberCache getCache() throws org.apache.geode.admin.AdminException {
    GemFireVM vm = getGemFireVM(); // fix for bug 33505
    if (vm == null)
      return null;
    try {
      return createSystemMemberCache(vm);

    } catch (CancelException ex) {
      return null;

    } catch (CacheDoesNotExistException ex) {
      return null;
    }
  }

  @Override
  public void refreshConfig() throws org.apache.geode.admin.AdminException {
    GemFireVM vm = getGemFireVM();
    if (vm == null)
      return;
    refreshConfig(vm.getConfig());
  }

  /**
   * Sets the value of this system member's distribution-related configuration based on the given
   * <code>Config</code> object.
   */
  public void refreshConfig(Config config) throws org.apache.geode.admin.AdminException {
    if (config == null) {
      throw new AdminException(
          String.format("Failed to refresh configuration parameters for: %s",
              new Object[] {getId()}));
    }

    String[] names = config.getAttributeNames();
    if (names == null || names.length < 1) {
      throw new AdminException(
          String.format("Failed to refresh configuration parameters for: %s",
              new Object[] {getId()}));
    }

    for (int i = 0; i < names.length; i++) {
      String name = names[i];
      Object value = config.getAttributeObject(name);
      if (value != null) {
        ConfigurationParameter parm = createConfigurationParameter(name, // name
            config.getAttributeDescription(name), // description
            value, // value
            config.getAttributeType(name), // valueType
            config.isAttributeModifiable(name)); // isModifiable
        ((ConfigurationParameterImpl) parm).addConfigurationParameterListener(this);
        this.parms.put(name, parm);
      }
    }
  }

  @Override
  public ConfigurationParameter[] getConfiguration() {
    ConfigurationParameter[] array = new ConfigurationParameter[this.parms.size()];
    this.parms.values().toArray(array);
    return array;
  }

  @Override
  public ConfigurationParameter[] setConfiguration(ConfigurationParameter[] parms)
      throws AdminException {

    for (int i = 0; i < parms.length; i++) {
      ConfigurationParameter parm = parms[i];
      this.parms.put(parm.getName(), parm);
    }

    GemFireVM vm = getGemFireVM();
    if (vm != null) {
      // update internal vm's config...
      Config config = vm.getConfig();
      for (int i = 0; i < parms.length; i++) {
        config.setAttributeObject(parms[i].getName(), parms[i].getValue(), ConfigSource.runtime());
      }
      vm.setConfig(config);
    }

    return this.getConfiguration();
  }

  @Override
  public SystemMemberType getType() {
    return SystemMemberType.APPLICATION;
  }

  // -------------------------------------------------------------------------
  // Listener callbacks
  // -------------------------------------------------------------------------

  // -- org.apache.geode.admin.internal.ConfigurationParameterListener ---
  @Override
  public void configurationParameterValueChanged(ConfigurationParameter parm) {
    try {
      setConfiguration(new ConfigurationParameter[] {parm});
    } catch (org.apache.geode.admin.AdminException e) {
      // this shouldn't occur since this is a config listener method...
      logger.warn(e.getMessage(), e);
      throw new RuntimeAdminException(e);
    } catch (java.lang.Exception e) {
      logger.warn(e.getMessage(), e);
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (java.lang.Error e) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e;
    }
  }

  // -------------------------------------------------------------------------
  // Overridden method(s) from java.lang.Object
  // -------------------------------------------------------------------------

  @Override
  public String toString() {
    return getName();
  }

  // -------------------------------------------------------------------------
  // Template methods with default behavior impl'ed. Override if needed.
  // -------------------------------------------------------------------------

  /**
   * Returns the <code>GemFireVM</code> that underlies this <code>SystemMember</code>.
   */
  protected GemFireVM getGemFireVM() {
    return this.vm;
  }

  /**
   * Sets the <code>GemFireVM</code> that underlies this <code>SystemMember</code>. This method is
   * used when a member, such as a cache server, is started by the admin API.
   */
  void setGemFireVM(GemFireVM vm) throws AdminException {
    this.vm = vm;
    if (vm != null) {
      this.internalId = vm.getId();
      this.id = this.internalId.toString();
      this.name = vm.getName();
      this.host = toHostString(vm.getHost());
    } else {
      this.internalId = null;
      this.id = null;
      // leave this.name set to what it is (how come?)
      this.host = this.getHost();
    }

    if (DistributionConfig.DEFAULT_NAME.equals(this.name)) {
      // Fix bug 32877
      this.name = this.id;
    }

    if (vm != null) {
      this.refreshConfig();
    }
  }

  /**
   * Updates this SystemMember instance using the corresponding InternalDistributedMember
   *
   * @param member InternalDistributedMember instance to update this SystemMember
   *
   * @since GemFire 6.5
   */
  private void updateByInternalDistributedMember(InternalDistributedMember member) {
    if (member != null) {
      this.internalId = member;
      this.id = this.internalId.toString();
      this.host = this.internalId.getHost();
      this.name = this.internalId.getName();
      if (this.name == null || DistributionConfig.DEFAULT_NAME.equals(this.name)) {
        /*
         * name could be null & referring to description of a fix for 32877
         */
        this.name = this.id;
      }
    }
  }

  /**
   * Template method for creating {@link StatisticResource}.
   *
   * @param stat the internal stat resource to wrap with {@link StatisticResource}
   * @return new impl instance of {@link StatisticResource}
   */
  protected StatisticResource createStatisticResource(StatResource stat)
      throws org.apache.geode.admin.AdminException {
    return new StatisticResourceImpl(stat, this);
  }

  /**
   * Template method for creating {@link ConfigurationParameter}.
   *
   * @param name the name of this parameter which cannot change
   * @param description full description to use
   * @param value the value of this parameter
   * @param type the class type of the value
   * @param userModifiable true if this is modifiable; false if read-only
   * @return new impl instance of {@link ConfigurationParameter}
   */
  protected ConfigurationParameter createConfigurationParameter(String name, String description,
      Object value, Class type, boolean userModifiable) {
    return new ConfigurationParameterImpl(name, description, value, type, userModifiable);
  }

  /**
   * Template method for creating {@link SystemMemberCache}.
   *
   * @param vm the GemFire vm to retrieve cache info from
   * @return new impl instance of {@link SystemMemberCache}
   */
  protected SystemMemberCache createSystemMemberCache(GemFireVM vm)
      throws org.apache.geode.admin.AdminException {
    return new SystemMemberCacheImpl(vm);
  }

  /** Wrap the internal stats with impls of {@link StatisticResource} */
  protected StatisticResource[] getStatsImpl(StatResource[] stats)
      throws org.apache.geode.admin.AdminException {
    List<StatisticResource> statList = new ArrayList<>();
    for (int i = 0; i < stats.length; i++) {
      statList.add(createStatisticResource(stats[i]));
    }
    return statList.toArray(new StatisticResource[0]);
  }

  @Override
  public String[] getRoles() {
    Set roles = this.internalId.getRoles();
    String[] roleNames = new String[roles.size()];
    Iterator iter = roles.iterator();
    for (int i = 0; i < roleNames.length; i++) {
      Role role = (Role) iter.next();
      roleNames[i] = role.getName();
    }
    return roleNames;
  }

  @Override
  public DistributedMember getDistributedMember() {
    return this.internalId;
  }
}
