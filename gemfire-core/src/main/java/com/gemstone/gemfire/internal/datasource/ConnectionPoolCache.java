/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.datasource;

/**
 * This interface outlines the behavior of a connection pool.
 * 
 * @author negi
 */
public interface ConnectionPoolCache {

  /**
   * This method is used to get the connection from the pool. The default
   * username and password are used for making the conections.
   * 
   * @return Object - connection from the pool.
   * @throws PoolException
   */
  public Object getPooledConnectionFromPool() throws PoolException;

  /**
   * This method will return the Pooled connection object back to the pool.
   * 
   * @param connectionObject - Connection object returned to the pool.
   */
  public void returnPooledConnectionToPool(Object connectionObject);

  /**
   * This method is used to set the time out for active connection so that it
   * can be collected by the cleaner thread Modified by Asif
   */
  public void expirePooledConnection(Object connectionObject);

  /**
   * Clean up the resources before restart of Cache
   */
  public void clearUp();
}
