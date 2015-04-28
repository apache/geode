/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * DataSourceResources.java
 *
 * Created on February 24, 2005, 12:42 PM
 */
package com.gemstone.gemfire.internal.datasource;

/**
 * @author tnegi
 * @author asif
 */
public interface DataSourceResources {

  /** Creates a new instance of DataSourceResources */
  public static final String JNDI_CONNECTION_POOL_DATA_SOURCE = "jdbc/ConnectionPoolDataSource";
  /* Default limit of maximum number of connection in the connection pool */
  public static final int CONNECTION_POOL_DEFAULT_MAX_LIMIT = 30;
  /* Default initial connection pool size */
  public static final int CONNECTION_POOL_DEFAULT_INIT_LIMIT = 10;
  /*
   * Default time in seconds after which the connections in the available pool
   * will expire
   */
  public static final int CONNECTION_POOL_DEFAULT_EXPIRATION_TIME = 600;
  //Default time in seconds after which the connections in the active cache
  // will be destroyed */
  public static final int CONNECTION_POOL_DEFAULT_ACTIVE_TIME_OUT = 120;
  //Default time in seconds after which the client thread for
  // retrieving connection will experience timeout
  public static final int CONNECTION_POOL_DEFAULT_CLIENT_TIME_OUT = 30;
  //Default Cleaner thread sleep time in seconds 30
  public static final int CLEANER_THREAD_SLEEP = 30;
}
