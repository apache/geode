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
package org.apache.geode.internal.jta;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class BlockingTimeOutJUnitTest {
  private static final Logger logger = LogService.getLogger();

  private static final int BLOCKING_TIMEOUT = 1;

  private static final int RANDOM = new Random().nextInt();

  private static Cache cache;
  private static String tblName;

  private static String readFile(String filename) throws Exception {
    BufferedReader br = new BufferedReader(new FileReader(filename));
    String nextLine = "";
    StringBuilder sb = new StringBuilder();
    while ((nextLine = br.readLine()) != null) {
      sb.append(nextLine);
      //
      // note:
      // BufferedReader strips the EOL character.
      //
      // sb.append(lineSep);
    }
    logger.debug("***********\n " + sb);
    return sb.toString();
  }

  private static String modifyFile(String str) throws Exception {
    String search = "<jndi-binding type=\"XAPooledDataSource\"";
    String last_search = "</jndi-binding>";
    String newDB = "newDB_" + RANDOM;
    String jndi_str =
        "<jndi-binding type=\"XAPooledDataSource\" jndi-name=\"XAPooledDataSource\"		              jdbc-driver-class=\"org.apache.derby.jdbc.EmbeddedDriver\" init-pool-size=\"1\" max-pool-size=\"2\" idle-timeout-seconds=\"600\" blocking-timeout-seconds=\""
            + BLOCKING_TIMEOUT
            + "\" login-timeout-seconds=\"25\" conn-pooled-datasource-class=\"org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource\" xa-datasource-class=\"org.apache.derby.jdbc.EmbeddedXADataSource\" user-name=\"mitul\" password=\"83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a\" connection-url=\"jdbc:derby:"
            + newDB + ";create=true\" >";
    String config_prop = "<config-property>"
        + "<config-property-name>description</config-property-name>"
        + "<config-property-type>java.lang.String</config-property-type>"
        + "<config-property-value>hi</config-property-value>" + "</config-property>"
        + "<config-property>" + "<config-property-name>user</config-property-name>"
        + "<config-property-type>java.lang.String</config-property-type>"
        + "<config-property-value>mitul</config-property-value>" + "</config-property>"
        + "<config-property>" + "<config-property-name>password</config-property-name>"
        + "<config-property-type>java.lang.String</config-property-type>"
        + "<config-property-value>83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a</config-property-value>	"
        + "</config-property>" + "<config-property>"
        + "<config-property-name>databaseName</config-property-name>"
        + "<config-property-type>java.lang.String</config-property-type>"
        + "<config-property-value>" + newDB + "</config-property-value>" + "</config-property>\n";
    String new_str = jndi_str + config_prop;
    /*
     * String new_str = " <jndi-binding type=\"XAPooledDataSource\" jndi-name=\"XAPooledDataSource\"
     * jdbc-driver-class=\"org.apache.derby.jdbc.EmbeddedDriver\"
     * init-pool-size=\"5\" max-pool-size=\"30\" idle-timeout-seconds=\"600\"
     * blocking-timeout-seconds=\"60\" login-timeout-seconds=\"25\"
     * conn-pooled-datasource-class=\"org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource\"
     * xa-datasource-class=\"org.apache.derby.jdbc.EmbeddedXADataSource\" user-name=\"mitul\"
     * password=\"83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a\"
     * connection-url=\"jdbc:derby:"+newDB+";create=true\" > <property
     * key=\"description\" value=\"hi\"/> <property key=\"databaseName\"
     * value=\""+newDB+"\"/> <property key=\"user\" value=\"mitul\"/> <property key=\"password\"
     * value=\"83f0069202c571faf1ae6c42b4ad46030e4e31c17409e19a\"/>";
     */
    int n1 = str.indexOf(search);
    logger.debug("Start Index = " + n1);
    int n2 = str.indexOf(last_search, n1);
    StringBuilder sbuff = new StringBuilder(str);
    logger.debug("END Index = " + n2);
    String modified_str = sbuff.replace(n1, n2, new_str).toString();
    return modified_str;
  }

  @SuppressWarnings("deprecation")
  private static String init(String className) throws Exception {
    DistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
    if (ds != null) {
      ds.disconnect();
    }
    logger.debug("PATH11 ");
    Properties props = new Properties();
    String path = System.getProperty("CACHEXMLFILE");
    logger.debug("PATH2 " + path);
    int pid = RANDOM;
    path = File.createTempFile("dunit-cachejta_", ".xml").getAbsolutePath();
    logger.debug("PATH " + path);
    /** * Return file as string and then modify the string accordingly ** */
    String file_as_str = readFile(
        createTempFileFromResource(CacheUtils.class, "cachejta.xml")
            .getAbsolutePath());
    file_as_str = file_as_str.replaceAll("newDB", "newDB_" + pid);
    String modified_file_str = modifyFile(file_as_str);
    FileOutputStream fos = new FileOutputStream(path);
    BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(fos));
    wr.write(modified_file_str);
    wr.flush();
    wr.close();
    props.setProperty(CACHE_XML_FILE, path);
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    String tableName = "";
    cache = new CacheFactory(props).create();
    if (className != null && !className.equals("")) {
      String time = new Long(System.currentTimeMillis()).toString();
      tableName = className + time;
      createTable(tableName);
    }
    tblName = tableName;
    return tableName;
  }

  private static void createTable(String tableName) throws Exception {
    Context ctx = cache.getJNDIContext();
    DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");
    String sql = "create table " + tableName
        + " (id integer NOT NULL, name varchar(50), CONSTRAINT the_key PRIMARY KEY(id))";
    logger.debug(sql);
    Connection conn = ds.getConnection();
    Statement sm = conn.createStatement();
    sm.execute(sql);
    sm.close();
    sm = conn.createStatement();
    for (int i = 1; i <= 10; i++) {
      sql = "insert into " + tableName + " values (" + i + ",'name" + i + "')";
      sm.addBatch(sql);
      logger.debug(sql);
    }
    sm.executeBatch();
    conn.close();
  }

  private static void destroyTable() throws Exception {
    try {
      String tableName = tblName;
      Context ctx = cache.getJNDIContext();
      DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");
      Connection conn = ds.getConnection();
      logger.debug(" trying to drop table: " + tableName);
      String sql = "drop table " + tableName;
      Statement sm = conn.createStatement();
      sm.execute(sql);
      conn.close();
    } catch (NamingException ne) {
      logger.debug("destroy table naming exception: " + ne);
      throw ne;
    } catch (SQLException se) {
      logger.debug("destroy table sql exception: " + se);
      throw se;
    } finally {
      closeCache();
    }
  }

  private static void closeCache() throws Exception {
    try {
      if (!cache.isClosed()) {
        cache.close();
      }
    } finally {
      InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
      if (ids != null) {
        ids.disconnect();
      }
      cache = null;
      tblName = null;
    }
  }

  @Before
  public void setUp() throws Exception {
    init("BlockingTimeOutJUnitTest");
  }

  @After
  public void tearDown() throws Exception {
    destroyTable();
  }

  private static void fail(String str, Throwable thr) {
    throw new AssertionError(str, thr);
  }

  @Test
  public void testBlockingTimeOut() throws Exception {
    runTest1();
    runTest2();
  }

  private void runTest1() throws Exception {
    final int MAX_CONNECTIONS = 2;
    DataSource ds = null;
    try {
      Context ctx = cache.getJNDIContext();
      ds = (DataSource) ctx.lookup("java:/XAPooledDataSource");
    } catch (NamingException e) {
      logger.debug("Naming Exception caught in lookup: " + e);
      fail("failed in naming lookup: ", e);
      return;
    } catch (Exception e) {
      logger.debug("Exception caught during naming lookup: " + e);
      fail("failed in naming lookup: ", e);
      return;
    }
    try {
      for (int count = 0; count < MAX_CONNECTIONS; count++) {
        ds.getConnection();
      }
    } catch (SQLException e) {
      logger.debug("Success SQLException caught in runTest1: " + e);
      fail("runTest1 SQL Exception caught: ", e);
    } catch (Exception e) {
      logger.debug("Exception caught in runTest1: " + e);
      fail("Exception caught in runTest1: ", e);
      e.printStackTrace();
    }
  }

  private void runTest2() throws Exception {
    final int MAX_CONNECTIONS = 2;
    logger.debug("runTest2 sleeping");
    Thread.sleep((BLOCKING_TIMEOUT + 2) * 1000);
    DataSource ds = null;
    try {
      Context ctx = cache.getJNDIContext();
      ds = (DataSource) ctx.lookup("java:/XAPooledDataSource");
    } catch (NamingException e) {
      logger.debug("Exception caught during naming lookup: " + e);
      fail("failed in naming lookup: ", e);
    } catch (Exception e) {
      logger.debug("Exception caught during naming lookup: " + e);
      fail("failed in because of unhandled excpetion: ", e);
    }
    try {
      for (int count = 0; count < MAX_CONNECTIONS; count++) {
        Connection con = ds.getConnection();
        assertNotNull("Connection object is null", con);

        logger.debug("runTest2 :acquired connection #" + count);
      }
    } catch (SQLException sqle) {
      logger.debug("SQLException caught in runTest2: " + sqle);
      fail("failed because of SQL exception : ", sqle);
    } catch (Exception e) {
      logger.debug("Exception caught in runTest2: " + e);
      fail("failed because of unhandled exception : ", e);
    }
  }
}
