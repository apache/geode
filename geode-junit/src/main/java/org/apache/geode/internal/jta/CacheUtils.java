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
/*
 * Utils.java
 *
 * Created on March 8, 2005, 4:16 PM
 */

package org.apache.geode.internal.jta;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.geode.CancelException;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.distributed.DistributedSystem;

public class CacheUtils {
  public static DistributedSystem ds;
  static Cache cache;
  static String tblName; // prabs

  // For use by hydra tests which use hydra Cache and Region APIs
  public static void setCache(Cache c) {
    cache = c;
  }

  public static String init() throws Exception {
    return init("");
  }

  public static String init(String className) throws Exception {
    Properties props = new Properties();
    props.setProperty(CACHE_XML_FILE,
        createTempFileFromResource(CacheUtils.class, "cachejta.xml")
            .getAbsolutePath());
    String tableName = "";
    props.setProperty(MCAST_PORT, "0");

    ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);
    if (className != null && !className.equals("")) {
      String time = new Long(System.currentTimeMillis()).toString();
      tableName = className + time;
      createTable(tableName);
    }

    return tableName;
  }

  public static void disconnect() {
    try {
      if (ds != null) {
        ds.disconnect();
      }
    } finally {
      cache = null;
      ds = null;
    }
  }

  public static String init(DistributedSystem ds1, String className) throws Exception {
    System.out.println("Entering CacheUtils.init, DS is " + ds1);
    String tableName = "";
    try {
      try {
        cache = CacheFactory.getInstance(ds1);
      } catch (CancelException cce) {
        cache = CacheFactory.create(ds1);
      }
      if (className != null && !className.equals("")) {
        String time = new Long(System.currentTimeMillis()).toString();
        tableName = className + time;
        createTable(tableName);
      }
    } catch (Exception e) {
      e.printStackTrace(System.err);
      throw new Exception("Exception in CacheUtils::init, The Exception is " + e);
    }
    return tableName;
  }

  public static void setTableName(String name) {
    tblName = name;
  }

  public static String getTableName() {
    return tblName;
  }

  public static void createTable(String tableName) throws NamingException, SQLException {
    Context ctx = cache.getJNDIContext();
    DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");

    // String sql = "create table " + tableName + " (id number primary key, name varchar2(50))";
    // String sql = "create table " + tableName + " (id integer primary key, name varchar(50))";
    String sql = "create table " + tableName
        + " (id integer NOT NULL, name varchar(50), CONSTRAINT the_key PRIMARY KEY(id))";
    System.out.println(sql);
    Connection conn = ds.getConnection();
    Statement sm = conn.createStatement();
    sm.execute(sql);
    sm.close();
    sm = conn.createStatement();
    for (int i = 1; i <= 100; i++) {
      sql = "insert into " + tableName + " values (" + i + ",'name" + i + "')";
      sm.addBatch(sql);
      System.out.println(sql);
    }
    sm.executeBatch();
    conn.close();
  }

  public static void listTableData(String tableName) throws NamingException, SQLException {
    Context ctx = cache.getJNDIContext();
    DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");

    String sql = "select * from " + tableName;

    Connection conn = ds.getConnection();
    try (Statement sm = conn.createStatement()) {
      ResultSet rs = sm.executeQuery(sql);
      while (rs.next()) {
        System.out.println("id " + rs.getString(1) + " name " + rs.getString(2));
      }
      rs.close();
    }
    conn.close();
  }

  public static void destroyTable(String tableName) throws NamingException, SQLException {
    Context ctx = cache.getJNDIContext();
    DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");
    Connection conn = ds.getConnection();
    // System.out.println (" trying to drop table: " + tableName);
    String sql = "drop table " + tableName;
    try (Statement sm = conn.createStatement()) {
      sm.execute(sql);
    }
    conn.close();
  }

  public static Cache getCache() {
    return cache;
  }

  public static void startCache() {
    try {
      if (cache.isClosed()) {
        cache = CacheFactory.create(ds);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void closeCache() {
    try {
      if (!cache.isClosed()) {
        cache.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void restartCache() {
    try {
      if (!cache.isClosed()) {
        cache.close();
      }
      cache = CacheFactory.create(ds);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static QueryService getQueryService() {
    if (cache.isClosed())
      startCache();
    return cache.getQueryService();
  }

  public static LogWriter getLogger() {
    return cache.getLogger();
  }

  public static void log(Object message) {
    System.out.println(message);
  }

}
