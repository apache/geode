/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.datasource;

/**
 * This interface outlines the behavior of a Connection provider.
 * 
 * @author tnegi
 */
public interface ConnectionProvider {

  /**
   * Provides a PooledConnection from the connection pool. Default user and
   * password are used.
   * 
   * @return a PooledConnection object to the user.
   */
  public Object borrowConnection() throws PoolException;

  /**
   * Returns a PooledConnection object to the pool.
   * 
   * @param connectionObject to be returned to the pool
   */
  public void returnConnection(Object connectionObject);

  /**
   * Closes a PooledConnection object .
   */
  public void returnAndExpireConnection(Object connectionObject);

  /**
   * Clean up the resources before restart of Cache
   */
  public void clearUp();
}
