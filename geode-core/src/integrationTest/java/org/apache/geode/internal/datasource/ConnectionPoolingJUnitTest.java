/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.datasource;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.util.test.TestUtil;

public class ConnectionPoolingJUnitTest {
  private static final Logger logger = LogService.getLogger();

  private Thread ThreadA;
  protected Thread ThreadB;
  // private Thread ThreadC;
  protected static int maxPoolSize = 7;
  protected static DataSource ds = null;
  private static Properties props = null;
  private static DistributedSystem ds1 = null;
  protected static Cache cache = null;
  protected boolean encounteredException = false;

  @Before
  public void setUp() throws Exception {
    encounteredException = false;
    props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    String path = TestUtil.getResourcePath(ConnectionPoolingJUnitTest.class, "/jta/cachejta.xml");
    props.setProperty(CACHE_XML_FILE, path);
    ds1 = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds1);
  }

  @After
  public void tearDown() throws Exception {
    ds1.disconnect();
  }

  @Test
  public void testGetSimpleDataSource() throws Exception {
    Context ctx = cache.getJNDIContext();
    GemFireBasicDataSource ds = (GemFireBasicDataSource) ctx.lookup("java:/SimpleDataSource");
    Connection conn = ds.getConnection();
    if (conn == null) {
      fail(
          "DataSourceFactoryTest-testGetSimpleDataSource() Error in creating the GemFireBasicDataSource");
    }
  }

  /*
   * public static void main(String str[]) { ConnectionPoolingJUnitTest test = new
   * ConnectionPoolingJUnitTest(); test.setup(); try { test.testGetSimpleDataSource(); } catch
   * (Exception e) { // TODO Auto-generated catch block e.printStackTrace(); fail(); }
   * test.testConnectionPoolFunctions(); test.teardown();
   *
   */
  @Test
  public void testConnectionPoolFunctions() throws Exception {
    Context ctx = cache.getJNDIContext();
    ds = (DataSource) ctx.lookup("java:/PooledDataSource");
    PoolClient_1 clientA = new PoolClient_1();
    ThreadA = new Thread(clientA, "ThreadA");
    PoolClient_2 clientB = new PoolClient_2();
    ThreadB = new Thread(clientB, "ThreadB");
    // ThreadA.setDaemon(true);
    // ThreadB.setDaemon(true);
    ThreadA.start();
  }

  @Ignore("Disabled for bug #52242")
  @Test
  public void testXAPoolLeakage() throws Exception {
    final String tableName = "testXAPoolLeakage";
    // initialize cache and get user transaction
    try {
      int numThreads = 10;
      final int LOOP_COUNT = 10;

      // logger.debug ("Table name: " + tableName);
      Context ctx = cache.getJNDIContext();
      DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");

      // String sql = "create table " + tableName + " (id number primary key,
      // name varchar2(50))";
      // String sql = "create table " + tableName + " (id integer primary key,
      // name varchar(50))";
      String sql =
          "create table " + tableName + " (id varchar(50) NOT NULL, name varchar(50), CONSTRAINT "
              + tableName + "_key PRIMARY KEY(id))";
      logger.debug(sql);
      Connection conn = ds.getConnection();
      Statement sm = conn.createStatement();
      sm.execute(sql);
      sm.close();
      conn.close();
      Thread th[] = new Thread[numThreads];
      for (int i = 0; i < numThreads; ++i) {
        final int threadID = i;
        th[i] = new Thread(new Runnable() {
          private int key = threadID;

          public void run() {
            try {
              Context ctx = cache.getJNDIContext();
              // Operation with first XA Resource
              DataSource da1 = (DataSource) ctx.lookup("java:/XAMultiThreadedDataSource");
              int val = 0;
              for (int j = 0; j < LOOP_COUNT; ++j) {
                UserTransaction ta = null;
                try {
                  ta = (UserTransaction) ctx.lookup("java:/UserTransaction");

                } catch (NamingException e) {
                  encounteredException = true;
                  break;
                }

                try {
                  // Begin the user transaction
                  ta.begin();
                  for (int i = 1; i <= 50; i++) {
                    Connection conn = da1.getConnection();
                    Statement sm = conn.createStatement();

                    String sql = "insert into " + tableName + " values (" + "'" + key + "X" + ++val
                        + "','name" + i + "')";
                    sm.execute(sql);

                    sm.close();
                    conn.close();
                  }
                  if (j % 2 == 0) {
                    ta.commit();
                    logger.debug("Committed successfully for thread with id =" + key);
                  } else {
                    ta.rollback();
                    logger.debug("Rolled back successfully for thread with id =" + key);
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                  encounteredException = true;
                  break;
                }

              }
            } catch (Exception e) {
              e.printStackTrace();
              encounteredException = true;
            }
          }
        });
      }

      for (int i = 0; i < th.length; ++i) {
        th[i].start();
      }

      for (int i = 0; i < th.length; ++i) {
        Thread t = th[i];
        long ms = 90 * 1000;
        t.join(ms);
        if (t.isAlive()) {
          for (int j = 0; j < th.length; j++) {
            th[j].interrupt();
          }
          fail("Thread did not terminate after " + ms + " ms: " + t);
        }
      }
      assertFalse(encounteredException);

    } finally {
      logger.debug("Destroying table: " + tableName);

      Context ctx = cache.getJNDIContext();
      DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");
      Connection conn = ds.getConnection();
      logger.debug(" trying to drop table: " + tableName);
      String sql = "drop table " + tableName;
      Statement sm = conn.createStatement();
      sm.execute(sql);
      conn.close();
      logger.debug(" Dropped table: " + tableName);
    }
  }

  private class PoolClient_1 implements Runnable {
    public void run() {
      String threadName = Thread.currentThread().getName();
      logger.debug(" Inside Run method of " + threadName);
      int numConn = 0;
      Object[] connections = new Object[maxPoolSize];
      // Statement stmt = null;
      while (numConn < maxPoolSize) {
        try {
          logger.debug(" Getting a connection from " + threadName);
          // ds.getConnection();
          connections[numConn] = ds.getConnection();
          // Thread.sleep(500);
          numConn++;
          // if (numConn == 5)
          // ThreadB.start();
          logger.debug(" Got connection " + numConn + "from " + threadName);
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
      if (numConn != maxPoolSize)
        fail("#### Error in filling the the connection pool from " + threadName);
      ThreadB.start();
      logger.debug(" AFTER starting THREADB");
      int numC = 0;
      int display = 0;
      // long birthTime = 0;
      // long newTime = 0;
      // long duration = 0;
      Connection conn;
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("interrupted");
      }
      for (numC = 0; numC < numConn; numC++) {
        try {
          display = numC + 1;
          logger.debug(" Returning connection " + display + "from " + threadName);
          conn = (Connection) connections[numC];
          logger.debug(" ************************************" + conn);
          logger.debug(
              " !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! The connection is of type "
                  + conn.getClass());
          // goahead = false;
          // conn.close();
          logger.debug(" Returned connection " + display + "from " + threadName);
        } catch (Exception ex) {
          fail("Exception occurred in trying to returnPooledConnectiontoPool due to " + ex);
          ex.printStackTrace();
        }
      }
      if (numC != maxPoolSize)
        fail("#### Error in returning all the connections to the  pool from " + threadName);
      logger.debug(" ****************Returned all connections " + threadName + "***********");
    }
  }

  private static class PoolClient_2 implements Runnable {

    List poolConnlist = new ArrayList();

    public void run() {
      try {
        String threadName = Thread.currentThread().getName();
        logger.debug(" Inside Run method of " + threadName);
        int numConn2 = 0;
        // int display = 0;
        Object[] connections = new Object[maxPoolSize];
        while (numConn2 < maxPoolSize) {
          try {
            logger.debug(
                " _______________________________________________________________ " + numConn2);
            numConn2++;
            logger.debug(" ********** Before getting " + numConn2 + "from" + threadName);
            connections[numConn2 - 1] = ds.getConnection();
            logger.debug(" ********** Got connection " + numConn2 + "from" + threadName);
          } catch (Exception ex) {
            ex.printStackTrace();
          }
        }
        numConn2 = 0;
        while (numConn2 < maxPoolSize) {
          try {
            ((Connection) connections[numConn2]).close();
            numConn2++;
          } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        if (numConn2 != maxPoolSize)
          fail("#### Error in getting all connections from the " + threadName);
        logger.debug(" ****************GOT ALL connections " + threadName + "***********");
      } catch (Exception e1) {
        e1.printStackTrace();
        fail();
      }
    }
  }
}
