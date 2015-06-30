/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.catalina;

import org.apache.juli.logging.Log;

public interface SessionManager {

  public String getRegionName();
  
  public String getRegionAttributesId();
  
  public int getMaxInactiveInterval();
  
  public boolean getEnableGatewayReplication();
  
  public boolean getEnableGatewayDeltaReplication();
  
  public boolean getEnableDebugListener();

  public boolean getEnableLocalCache();

  public boolean isCommitValveEnabled();

  public boolean isCommitValveFailfastEnabled();

  public boolean isBackingCacheAvailable();

  public boolean getPreferDeserializedForm();

  public String getStatisticsName();
  
  public Log getLogger();
}
