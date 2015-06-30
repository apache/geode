/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.catalina;

import java.util.Set;

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.modules.session.catalina.internal.DeltaSessionStatistics;
import javax.servlet.http.HttpSession;
import org.apache.catalina.Session;

public interface SessionCache {
  
  public void initialize();
  
  public String getDefaultRegionAttributesId();

  public boolean getDefaultEnableLocalCache();
  
  public String getSessionRegionName();
  
  public String getOperatingRegionName();

  public void putSession(Session session);
  
  public HttpSession getSession(String sessionId);
  
  public void destroySession(String sessionId);
  
  public void touchSessions(Set<String> sessionIds);
  
  public DeltaSessionStatistics getStatistics();
  
  public GemFireCache getCache();
  
  public Region<String,HttpSession> getSessionRegion();
  
  public Region<String,HttpSession> getOperatingRegion();
  
  public boolean isPeerToPeer();
  
  public boolean isClientServer();
  
  public Set<String> keySet();
  
  public int size();

  public boolean isBackingCacheAvailable();
}
