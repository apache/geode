/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

/**
 * A dedicated cache server VM that is managed by the administration
 * API.
 * <p>Note that this may not represent an instance of
 * {@link com.gemstone.gemfire.cache.server.CacheServer}. It is possible for
 * a cache VM to be started but for it not to listen for client connections
 * in which case it is not a 
 * {@link com.gemstone.gemfire.cache.server.CacheServer}
 * but is an instance of this interface.
 *
 * @author darrel
 * @since 5.7
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface CacheVm extends SystemMember, ManagedEntity {
  /**
   * Returns the configuration of this cache vm
   */
  public CacheVmConfig getVmConfig();
}
