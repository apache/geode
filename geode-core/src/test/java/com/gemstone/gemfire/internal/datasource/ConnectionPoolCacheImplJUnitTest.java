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
 * ConnectionPoolCacheImplJUnitTest.java JUnit based test
 * 
 * Created on March 3, 2005, 5:26 PM
 */
package com.gemstone.gemfire.internal.datasource;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.util.test.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.naming.Context;
import javax.sql.PooledConnection;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.fail;

/*
 * this is a Multithreaded test for datasource connection pool.
 */
@Category(IntegrationTest.class)
public class ConnectionPoolCacheImplJUnitTest {

	protected static ConnectionPoolCacheImpl poolCache = null;
  private Thread ThreadA;
  protected Thread ThreadB;
//  private Thread ThreadC;
  protected static int maxPoolSize;
  protected static GemFireConnPooledDataSource ds = null;
  private static GemFireConnectionPoolManager provider = null;
  private static Properties props = null;
  private static DistributedSystem ds1 = null;
  private static Cache cache = null;

  @Before
  public void setUp() {
    try {
      props = new Properties();
      props.setProperty(MCAST_PORT, "0");
      String path = TestUtil.getResourcePath(ConnectionPoolCacheImplJUnitTest.class, "/jta/cachejta.xml");
      props.setProperty(CACHE_XML_FILE, path);
      ds1 = DistributedSystem.connect(props);
      cache = CacheFactory.create(ds1);
    }
    catch (Exception e) {
      fail("Exception occured in creation of ds and cache due to " + e);
      e.printStackTrace();
    }
  }

  @After
  public void tearDown() {
    ds1.disconnect();
  }

  @Test
  public void testGetSimpleDataSource() throws Exception {
      Context ctx = cache.getJNDIContext();
      GemFireBasicDataSource ds = (GemFireBasicDataSource) ctx
          .lookup("java:/SimpleDataSource");
      Connection conn = ds.getConnection();
      if (conn == null)
          fail("DataSourceFactoryTest-testGetSimpleDataSource() Error in creating the GemFireBasicDataSource");
  }

  @Test
  public void testConnectionPoolFunctions() {
    try {
      createPool();
      PoolClient_1 clientA = new PoolClient_1();
      ThreadA = new Thread(clientA, "ThreadA");
      PoolClient_2 clientB = new PoolClient_2();
      ThreadB = new Thread(clientB, "ThreadB");
      ThreadA.start();
    }
    catch (Exception e) {
      fail("Exception occured in testConnectionPoolFunctions due to " + e);
      e.printStackTrace();
    }
  }

  public static void createPool() throws Exception {
    Context ctx = cache.getJNDIContext();
    GemFireConnPooledDataSource ds = (GemFireConnPooledDataSource) ctx
        .lookup("java:/PooledDataSource");
    provider = (GemFireConnectionPoolManager) ds.getConnectionProvider();
    poolCache = (ConnectionPoolCacheImpl) provider.getConnectionPoolCache();
    maxPoolSize = poolCache.getMaxLimit();
  }

  class PoolClient_1 implements Runnable {

    List poolConnlist = new ArrayList();

    public void run() {
      String threadName = Thread.currentThread().getName();
      //	System.out.println(" Inside Run method of " + threadName);
      int numConn = 0;
      while (poolConnlist.size() < maxPoolSize) {
        try {
          //	System.out.println(" Getting a connection from " + threadName);
          PooledConnection conn = (PooledConnection) poolCache
              .getPooledConnectionFromPool();
          poolConnlist.add(conn);
          numConn++;
          //System.out.println(" Got connection " + numConn + "from "+
          // threadName);
        }
        catch (Exception ex) {
          fail("Exception occured in trying to getPooledConnectionfromPool due to "
              + ex);
          ex.printStackTrace();
        }
      }
      if (numConn != maxPoolSize)
          fail("#### Error in filling the the connection pool from "
              + threadName);
      ThreadB.start();
      //System.out.println(" AFTER starting THREADB");
      int numC = 0;
//      int display = 0;
      long birthTime = 0;
      long newTime = 0;
      long duration = 0;
      for (numC = 0; numC < poolConnlist.size(); numC++) {
        try {
          birthTime = System.currentTimeMillis();
          while (true) {
            newTime = System.currentTimeMillis();
            duration = newTime - birthTime;
            if (duration > 5) break;
          }
//          display = numC + 1;
          //System.out.println(" Returning connection " + display + "from "+
          // threadName);
          poolCache
              .returnPooledConnectionToPool(poolConnlist
                  .get(numC));
          //System.out.println(" Returned connection " + display + "from "+
          // threadName);
        }
        catch (Exception ex) {
          fail("Exception occured in trying to returnPooledConnectiontoPool due to "
              + ex);
          ex.printStackTrace();
        }
      }
      if (numC != maxPoolSize)
          fail("#### Error in returning all the connections to the  pool from "
              + threadName);
      //System.out.println(" ****************Returned all connections " +
      // threadName + "***********");
    }
  }

  class PoolClient_2 implements Runnable {

    List poolConnlist = new ArrayList();

    public void run() {
      String threadName = Thread.currentThread().getName();
      //System.out.println(" Inside Run method of " + threadName);
      int numConn2 = 0;
//      int display = 0;
      while (numConn2 < maxPoolSize) {
        try {
          PooledConnection conn = (PooledConnection) poolCache
              .getPooledConnectionFromPool();
          poolConnlist.add(conn);
          numConn2++;
          //	System.out.println(" ********** Got connection " + numConn2+ "from
          // " + threadName);
        }
        catch (Exception ex) {
          fail("Exception occured in trying to getPooledConnectionFromPool due to "
              + ex);
          ex.printStackTrace();
        }
      }
      if (numConn2 != maxPoolSize)
          fail("#### Error in getting all connections from the " + threadName);
      //System.out.println(" ****************GOT ALL connections "+ threadName
      // + "***********");
    }
  }
}
