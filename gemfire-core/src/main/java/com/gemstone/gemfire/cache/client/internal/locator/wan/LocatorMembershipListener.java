/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal.locator.wan;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;

/**
 * A listener to handle membership when new locator is added to remote locator
 * metadata. This listener is expected to inform all other locators in remote
 * locator metadata about the new locator so that they can update their remote
 * locator metadata.
 * 
 * @author kbachhav
 * 
 */
public interface LocatorMembershipListener {

  public Object handleRequest(Object request);
  
  public void setPort(int port);
  public void setConfig(DistributionConfig config);
  
  /**
   * When the new locator is added to remote locator metadata, inform all other
   * locators in remote locator metadata about the new locator so that they can
   * update their remote locator metadata.
   * 
   * @param locator
   */
  public void locatorJoined(int distributedSystemId, DistributionLocatorId locator, DistributionLocatorId sourceLocator);
  
  public Set<String> getRemoteLocatorInfo(int dsId);

  public ConcurrentMap<Integer,Set<DistributionLocatorId>> getAllLocatorsInfo();
  
  public ConcurrentMap<Integer,Set<String>> getAllServerLocatorsInfo();
  
  public void clearLocatorInfo();
}
