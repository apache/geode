/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.ha;

import java.util.Map;

import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;

/**
 * @author ashetkar
 * @since 5.7
 */
public interface HAContainerWrapper extends Map {

  public void cleanUp();

  public Object getEntry(Object key);

  public Object getKey(Object key);

  public String getName();

  public ClientProxyMembershipID getProxyID(String haRegionName);

  public Object putProxy(String haRegionName, CacheClientProxy proxy);

  public Object removeProxy(String haRegionName);
  
  public CacheClientProxy getProxy(String haRegionName);
  
}
