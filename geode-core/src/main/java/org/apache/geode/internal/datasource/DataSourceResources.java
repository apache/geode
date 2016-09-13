/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * DataSourceResources.java
 *
 * Created on February 24, 2005, 12:42 PM
 */
package com.gemstone.gemfire.internal.datasource;

/**
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
