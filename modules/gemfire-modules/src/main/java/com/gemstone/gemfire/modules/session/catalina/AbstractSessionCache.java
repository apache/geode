/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.catalina;

import com.gemstone.gemfire.cache.EntryNotFoundException;
import org.apache.catalina.Session;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.modules.session.catalina.internal.DeltaSessionStatistics;
import com.gemstone.gemfire.modules.util.RegionConfiguration;
import com.gemstone.gemfire.modules.util.SessionCustomExpiry;
import javax.servlet.http.HttpSession;

public abstract class AbstractSessionCache implements SessionCache {
  
  protected SessionManager sessionManager;
  
  /**
   * The sessionRegion is the <code>Region</code> that actually stores and
   * replicates the <code>Session</code>s.
   */
  protected Region<String,HttpSession> sessionRegion;

  /**
   * The operatingRegion is the <code>Region</code> used to do HTTP operations.
   * if local cache is enabled, then this will be the local <code>Region</code>;
   * otherwise, it will be the session <code>Region</code>.
   */
  protected Region<String,HttpSession> operatingRegion;

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
    getOperatingRegion().put(session.getId(), (HttpSession)session);
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

  public Region<String,HttpSession> getSessionRegion() {
    return this.sessionRegion;
  }

  public Region<String,HttpSession> getOperatingRegion() {
    return this.operatingRegion;
  }

  protected void createStatistics() {
    this.statistics = new DeltaSessionStatistics(getCache().getDistributedSystem(), getSessionManager().getStatisticsName());
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
