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
package com.gemstone.gemfire.modules.session.catalina;

import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.modules.session.catalina.internal.DeltaSessionStatistics;
import com.gemstone.gemfire.modules.util.RegionConfiguration;
import com.gemstone.gemfire.modules.util.SessionCustomExpiry;
import org.apache.catalina.Session;

import javax.servlet.http.HttpSession;

public abstract class AbstractSessionCache implements SessionCache {

  protected SessionManager sessionManager;

  /**
   * The sessionRegion is the <code>Region</code> that actually stores and replicates the <code>Session</code>s.
   */
  protected Region<String, HttpSession> sessionRegion;

  /**
   * The operatingRegion is the <code>Region</code> used to do HTTP operations. if local cache is enabled, then this
   * will be the local <code>Region</code>; otherwise, it will be the session <code>Region</code>.
   */
  protected Region<String, HttpSession> operatingRegion;

  protected DeltaSessionStatistics statistics;

  public AbstractSessionCache(SessionManager sessionManager) {
    this.sessionManager = sessionManager;
  }

  @Override
  public String getSessionRegionName() {
    return getSessionRegion().getFullPath();
  }

  @Override
  public String getOperatingRegionName() {
    return getOperatingRegion().getFullPath();
  }

  @Override
  public void putSession(Session session) {
    getOperatingRegion().put(session.getId(), (HttpSession) session);
  }

  @Override
  public HttpSession getSession(String sessionId) {
    return getOperatingRegion().get(sessionId);
  }

  @Override
  public void destroySession(String sessionId) {
    try {
      getOperatingRegion().destroy(sessionId);
    } catch (EntryNotFoundException enex) {
      // Ignored
    }
  }

  @Override
  public DeltaSessionStatistics getStatistics() {
    return this.statistics;
  }

  protected SessionManager getSessionManager() {
    return this.sessionManager;
  }

  public Region<String, HttpSession> getSessionRegion() {
    return this.sessionRegion;
  }

  public Region<String, HttpSession> getOperatingRegion() {
    return this.operatingRegion;
  }

  protected void createStatistics() {
    this.statistics = new DeltaSessionStatistics(getCache().getDistributedSystem(),
        getSessionManager().getStatisticsName());
  }

  protected RegionConfiguration createRegionConfiguration() {
    RegionConfiguration configuration = new RegionConfiguration();
    configuration.setRegionName(getSessionManager().getRegionName());
    configuration.setRegionAttributesId(getSessionManager().getRegionAttributesId());
    if (getSessionManager().getMaxInactiveInterval() != RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL) {
      configuration.setMaxInactiveInterval(getSessionManager().getMaxInactiveInterval());
      configuration.setCustomExpiry(new SessionCustomExpiry());
    }
    configuration.setEnableGatewayDeltaReplication(getSessionManager().getEnableGatewayDeltaReplication());
    configuration.setEnableGatewayReplication(getSessionManager().getEnableGatewayReplication());
    configuration.setEnableDebugListener(getSessionManager().getEnableDebugListener());
    return configuration;
  }
}
