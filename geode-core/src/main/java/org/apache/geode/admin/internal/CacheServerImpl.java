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

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;

import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.CacheServer;
import org.apache.geode.admin.CacheServerConfig;
import org.apache.geode.admin.CacheVm;
import org.apache.geode.admin.CacheVmConfig;
import org.apache.geode.admin.ManagedEntityConfig;
import org.apache.geode.admin.SystemMemberType;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.admin.remote.RemoteApplicationVM;

/**
 * Implements the administrative interface to a cache server.
 *
 * @since GemFire 3.5
 */
public class CacheServerImpl extends ManagedSystemMemberImpl implements CacheVm, CacheServer {

  /** How many new <code>CacheServer</code>s have been created? */
  @MakeNotStatic
  private static int newCacheServers = 0;

  /////////////////////// Instance Fields ///////////////////////

  /** The configuration object for this cache server */
  private final CacheServerConfigImpl config;

  ///////////////////////// Constructors ////////////////////////

  /**
   * Creates a new <code>CacheServerImpl</code> that represents a non-existsing (unstarted) cache
   * server in a given distributed system.
   */
  public CacheServerImpl(AdminDistributedSystemImpl system, CacheVmConfig config)
      throws AdminException {

    super(system, config);

    this.config = (CacheServerConfigImpl) config;
    this.config.setManagedEntity(this);
  }

  /**
   * Creates a new <code>CacheServerImpl</code> that represents an existing dedicated cache server
   * in a given distributed system.
   */
  public CacheServerImpl(AdminDistributedSystemImpl system, GemFireVM vm) throws AdminException {

    super(system, vm);
    this.config = new CacheServerConfigImpl(vm);
  }

  ////////////////////// Instance Methods //////////////////////

  @Override
  public SystemMemberType getType() {
    return SystemMemberType.CACHE_VM;
  }

  @Override
  public String getNewId() {
    synchronized (CacheServerImpl.class) {
      return "CacheVm" + (++newCacheServers);
    }
  }

  @Override
  public void start() throws AdminException {
    if (!needToStart()) {
      return;
    }

    this.config.validate();
    this.controller.start(this);
    this.config.setManagedEntity(this);
  }

  @Override
  public void stop() {
    if (!needToStop()) {
      return;
    }

    this.controller.stop(this);
    // NOTE: DistributedSystem nodeLeft will then set this.manager to null
    this.config.setManagedEntity(null);
  }

  @Override
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
    return dm.getDistributionManagerIdsIncludingAdmin()
        .contains(getDistributedMember());
  }

  @Override
  public CacheServerConfig getConfig() {
    return this.config;
  }

  @Override
  public CacheVmConfig getVmConfig() {
    return this.config;
  }

  //////////////////////// Command execution ////////////////////////

  @Override
  public ManagedEntityConfig getEntityConfig() {
    return this.getConfig();
  }

  @Override
  public String getEntityType() {
    // Fix bug 32564
    return "Cache Vm";
  }

  @Override
  public String getStartCommand() {
    StringBuffer sb = new StringBuffer();
    sb.append(this.controller.getProductExecutable(this, "cacheserver"));
    sb.append(" start -dir=");
    sb.append(this.getConfig().getWorkingDirectory());

    String file = this.getConfig().getCacheXMLFile();
    if (file != null && file.length() > 0) {
      sb.append(" ");
      sb.append(CACHE_XML_FILE);
      sb.append("=");
      sb.append(file);
    }

    String classpath = this.getConfig().getClassPath();
    if (classpath != null && classpath.length() > 0) {
      sb.append(" -classpath=");
      sb.append(classpath);
    }

    appendConfiguration(sb);

    return sb.toString().trim();
  }

  @Override
  public String getStopCommand() {
    StringBuffer sb = new StringBuffer();
    sb.append(this.controller.getProductExecutable(this, "cacheserver"));
    sb.append(" stop -dir=");
    sb.append(this.getConfig().getWorkingDirectory());

    return sb.toString().trim();
  }

  @Override
  public String getIsRunningCommand() {
    StringBuffer sb = new StringBuffer();
    sb.append(this.controller.getProductExecutable(this, "cacheserver"));
    sb.append(" status -dir=");
    sb.append(this.getConfig().getWorkingDirectory());

    return sb.toString().trim();
  }

  /**
   * Find whether this server is primary for given client (durableClientId)
   *
   * @param durableClientId - durable-id of the client
   * @return true if the server is primary for given client
   *
   * @since GemFire 5.6
   */
  @Override
  public boolean isPrimaryForDurableClient(String durableClientId) {
    RemoteApplicationVM vm = (RemoteApplicationVM) this.getGemFireVM();
    boolean isPrimary = false;
    if (vm != null) {
      isPrimary = vm.isPrimaryForDurableClient(durableClientId);
    }
    return isPrimary;
  }

}
