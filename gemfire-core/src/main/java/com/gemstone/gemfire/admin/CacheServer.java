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
 *
 * @author David Whitlock
 * @since 4.0
 * @deprecated as of 5.7 use {@link CacheVm} instead.
 */
@Deprecated
public interface CacheServer extends SystemMember, ManagedEntity {
  /**
   * Returns the configuration of this cache vm
   * @deprecated as of 5.7 use {@link CacheVm#getVmConfig} instead.
   */
  @Deprecated
  public CacheServerConfig getConfig();
  /**
   * Find whether this server is primary for given client (durableClientId)
   * 
   * @param durableClientId -
   *                durable-id of the client
   * @return true if the server is primary for given client
   * 
   * @since 5.6
   */
  public boolean isPrimaryForDurableClient(String durableClientId);

}
