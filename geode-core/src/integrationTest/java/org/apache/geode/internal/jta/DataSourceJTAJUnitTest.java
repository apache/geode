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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

/**
 * This test case is to test the following test scenarios: 1) Get Simple DS outside transaction,
 * begin transaction, use XA DS to get transactional connection, make inserts/ updates using both XA
 * DS and Simple DS. Test for commit() and rollback() of both DS. 2) Test scenarios like multiple XA
 * Datasouces in User Txn and test the commit, rollback 3) Observe the above behavior when
 * connection exceeds Blocking Time Out. 4) Commit and rollback behavior when DDL query is issued
 * after inserts abd before commit or rollback.
 *
 */
public class DataSourceJTAJUnitTest {

  @AfterClass
  public static void afterClass() {
    disconnectDS();
  }

  // ///setUp and tearDown methods/////

  @BeforeClass
  public static void beforeClass() throws java.lang.Exception {
    disconnectDS();
  }

  static void disconnectDS() {
    DistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
    if (ds != null) {
      ds.disconnect();
    }
  }



  @Test
  public void testInsertUpdateOnSimpleAndXAdsCommit() throws Exception {
    Region currRegion = null;
    Cache cache;
    DistributedSystem ds = null;
    int tblIDFld;
    String tblNameFld;
    String tblName;
    String tableName = null;
    // boolean to_continue = true;
    final int XA_INSERTS = 3;
    final int SM_INSERTS = 1;

    // call to init method
    try {
      Properties props = new Properties();
      String path =
          createTempFileFromResource(CacheUtils.class, "cachejta.xml")
              .getAbsolutePath();
      props.setProperty(CACHE_XML_FILE, path);
      ds = connect(props);
      tableName = CacheUtils.init(ds, "JTATest");
      // System.out.println ("Table name: " + tableName);
      tblName = tableName;

      if (tableName == null || tableName.equals("")) {
        // to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    } catch (Exception e) {
      // to_continue = false;
      fail(" Aborting test at set up...[" + e + "]", e);
    }

    System.out.println("init for testscenario 1 Successful!");

    // test task
    cache = CacheUtils.getCache();
    tblName = tableName;
    currRegion = cache.getRegion(SEPARATOR + "root");
    tblIDFld = 1;
    tblNameFld = "test1";
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);

    // to delete all rows inserted in creatTable () of CacheUtils class
    // deleteRows method of JTAUtils class is used.
    jtaObj.deleteRows(tblName);

    // initialize cache and get user transaction
    Context ctx = cache.getJNDIContext();
    UserTransaction ta = null;
    Connection xa_conn = null;
    Connection sm_conn = null;
    try {
      ta = (UserTransaction) ctx.lookup("java:/UserTransaction");

    } catch (NamingException e) {
      fail(" fail in user txn lookup ", e);
    }

    try {

      // get the SimpleDataSource before the transaction begins
      DataSource sds = (DataSource) ctx.lookup("java:/SimpleDataSource");

      // Begin the user transaction
      ta.begin();

      // Obtain XAPooledDataSource
      DataSource da = (DataSource) ctx.lookup("java:/XAPooledDataSource");

      // obtain connection from SimpleDataSource and XAPooledDataSource
      xa_conn = da.getConnection();
      sm_conn = sds.getConnection();

      Statement xa_stmt = xa_conn.createStatement();
      Statement sm_stmt = sm_conn.createStatement();

      // perform inserts and updates using both viz. Simple and XA DataSources
      // String sqlSTR = "insert into " + tblName + " values (" + tblIDFld + ","
      // + "'" + tblNameFld + "'" + ")" ;

      String sqlSTR;

      // insert XA_INSERTS rows into timestamped table
      for (int i = 0; i < XA_INSERTS; i++) {
        tblIDFld = tblIDFld + i;
        sqlSTR =
            "insert into " + tblName + " values (" + tblIDFld + "," + "'" + tblNameFld + "'" + ")";
        xa_stmt.executeUpdate(sqlSTR);
      }

      // insert SM_INSERTS rows into timestamped table
      for (int j = 0; j < SM_INSERTS; j++) {
        tblIDFld = tblIDFld + j + 1;
        sqlSTR =
            "insert into " + tblName + " values (" + tblIDFld + "," + "'" + tblNameFld + "'" + ")";
        sm_stmt.executeUpdate(sqlSTR);
      }

      // close the Simple and XA statements
      xa_stmt.close();
      sm_stmt.close();

      // close the connections
      xa_conn.close();
      sm_conn.close();

      // commit the transaction
      ta.commit();
      System.out.println("Rows are committed in the database");
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      fail(" failed ", e);
    } finally {
      if (xa_conn != null) {
        try {
          // close the connections
          xa_conn.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    // get the number of rows in time stamped table
    // using getRows method of JTAUtils

    try {
      int num_rows = jtaObj.getRows(tblName);
      System.out.println("\nNumber of rows in Table under tests are: " + num_rows);

      // determine what got commited and what not

      switch (num_rows) {
        case SM_INSERTS + XA_INSERTS:
          System.out.println("Both Simple and XA DataSource transactions got committed!!");
          break;

        case SM_INSERTS:
          System.out.println("Only Simple DataSource transactions got committed!");
          break;

        case XA_INSERTS:
          System.out.println("Only XA DataSource transactions got committed!");
          break;

        default:
          System.out.println("Looks like that things are messed up...look into it");

      }

    } catch (SQLException sqle) {
      sqle.printStackTrace();
    }

    System.out.println("test task for testScenario1 Succeessful");

    // destroying table
    try {
      System.out.println("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      System.out.println("Closing cache...");
      System.out.println("destroyTable for testscenario 1 Successful!");
    } catch (Exception e) {
      fail(" failed during tear down of this test...", e);
    } finally {
      CacheUtils.closeCache();
      // CacheUtils.ds.disconnect();
      ds.disconnect();
    }
  }

  /**
   * The following test scenario is to test rollback. In this test scenario it is tested that if
   * transactions are rolled back then XA datasource transactions should be rolled back andSimple
   * datasource transactions should get committed
   */
  @Test
  public void testInsertUpdateOnSimpleAndXAdsRollback() throws Exception {
    Region currRegion = null;
    Cache cache;
    DistributedSystem ds = null;
    int tblIDFld;
    String tblNameFld;
    String tblName;
    String tableName = null;
    // boolean to_continue = true;
    final int XA_INSERTS = 3;
    final int SM_INSERTS = 1;

    // call to init method
    try {
      Properties props = new Properties();
      String path =
          createTempFileFromResource(CacheUtils.class, "cachejta.xml")
              .getAbsolutePath();
      props.setProperty(CACHE_XML_FILE, path);
      ds = connect(props);
      tableName = CacheUtils.init(ds, "JTATest");
      System.out.println("Table name: " + tableName);
      tblName = tableName;

      if (tableName == null || tableName.equals("")) {
        // to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    } catch (Exception e) {
      // to_continue = false;
      fail(" Aborting test at set up...[" + e + "]", e);
    }

    System.out.println("init for testscenario 2 Successful!");

    // test task
    cache = CacheUtils.getCache();
    tblName = tableName;
    currRegion = cache.getRegion(SEPARATOR + "root");
    tblIDFld = 1;
    tblNameFld = "test2";
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);

    // to delete all rows inserted in creatTable () of CacheUtils class
    // deleteRows method of JTAUtils class is used.
    jtaObj.deleteRows(tblName);

    // initialize cache and get user transaction
    Context ctx = cache.getJNDIContext();
    UserTransaction ta = null;
    Connection xa_conn = null;
    Connection sm_conn = null;
    try {
      ta = (UserTransaction) ctx.lookup("java:/UserTransaction");

    } catch (NamingException e) {
      fail(" fail in user txn lookup ", e);
    }

    try {

      // get the SimpleDataSource before the transaction begins
      DataSource sds = (DataSource) ctx.lookup("java:/SimpleDataSource");

      // Begin the user transaction
      ta.begin();

      // Obtain XAPooledDataSource
      DataSource da = (DataSource) ctx.lookup("java:/XAPooledDataSource");

      // obtain connection from SimpleDataSource and XAPooledDataSource
      xa_conn = da.getConnection();
      sm_conn = sds.getConnection();

      Statement xa_stmt = xa_conn.createStatement();
      Statement sm_stmt = sm_conn.createStatement();

      // perform inserts and updates using both viz. Simple and XA DataSources
      // String sqlSTR = "insert into " + tblName + " values (" + tblIDFld + ","
      // + "'" + tblNameFld + "'" + ")" ;

      String sqlSTR;

      // insert XA_INSERTS rows into timestamped table
      for (int i = 0; i < XA_INSERTS; i++) {
        tblIDFld = tblIDFld + i;
        sqlSTR =
            "insert into " + tblName + " values (" + tblIDFld + "," + "'" + tblNameFld + "'" + ")";
        xa_stmt.executeUpdate(sqlSTR);
      }

      // insert SM_INSERTS rows into timestamped table
      for (int j = 0; j < SM_INSERTS; j++) {
        tblIDFld = tblIDFld + j + 1;
        sqlSTR =
            "insert into " + tblName + " values (" + tblIDFld + "," + "'" + tblNameFld + "'" + ")";
        sm_stmt.executeUpdate(sqlSTR);
      }

      // close the Simple and XA statements
      xa_stmt.close();
      sm_stmt.close();

      // close the connections
      xa_conn.close();
      sm_conn.close();

      // rollback the transaction
      ta.rollback();
      System.out.println("Rows are rolled back in the database");
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      fail(" failed ", e);
    } finally {
      if (xa_conn != null) {
        try {
          // close the connections
          xa_conn.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    // get the number of rows in time stamped table
    // using getRows method of JTAUtils

    try {
      int num_rows = jtaObj.getRows(tblName);
      System.out.println("\nNumber of rows in Table under tests are: " + num_rows);

      // determine what got commited and what not

      switch (num_rows) {
        case 0:
          System.out.println("Both Simple and XA DataSource transactions got rolled back!!");
          break;

        case SM_INSERTS:
          System.out.println("Only Simple DataSource transactions got committed!");
          break;

        case XA_INSERTS:
          System.out.println("Only XA DataSource transactions got committed!");
          break;

        default:
          System.out.println("Looks like that things are messed up...look into it");

      }

    } catch (SQLException sqle) {
      sqle.printStackTrace();
    }

    System.out.println("test task for testScenario2 Succeessful");

    // destroying table
    try {
      System.out.println("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      System.out.println("Closing cache...");
      System.out.println("destroyTable for testscenario 2 Successful!");
    } catch (Exception e) {
      fail(" failed during tear down of this test...", e);
    } finally {
      CacheUtils.closeCache();
      // CacheUtils.ds.disconnect();
      ds.disconnect();
    }
  }

  /*
   * The following test scenarion is to test the the behaviour ofXA DataSource if obtained outside
   * the user transaction and transaction rolled back.
   */

  /*
   * public static void testInsertsOnTwoXAdsRollback () { Host host = Host.getHost (0); VM vm0 =
   * host.getVM (0); vm0.invoke (DataSourceJTAJUnitTest.class, "Scenario3"); }
   *
   * public static void Scenario3 () throws Exception { Region currRegion=null; Cache cache; int
   * tblIDFld; String tblNameFld; String tblName; String tableName=null; boolean to_continue = true;
   * final int XA_INSERTS_BEFORE=3; final int XA_INSERTS_AFTER=2;
   *
   * //call to init method try { tableName = CacheUtils.init("JTATest"); //System.out.println
   * ("Table name: " + tableName); tblName = tableName;
   *
   * if (tableName == null || tableName.equals ("")) { to_continue = false; fail
   * (" table name not created, Aborting test..."); } } catch (Exception e) { to_continue = false;
   * fail (" Aborting test at set up...[" + e + "]"); }
   *
   * System.out.println("init for testscenario 3 Successful!");
   *
   * //test task cache = CacheUtils.getCache (); tblName = tableName; currRegion =
   * cache.getRegion("/root"); tblIDFld = 1; tblNameFld = "test3"; JTAUtils jtaObj = new JTAUtils
   * (cache, currRegion);
   *
   * //to delete all rows inserted in creatTable () of CacheUtils class //deleteRows method of
   * JTAUtils class is used. int rowsDeleted = jtaObj.deleteRows(tblName);
   *
   * //initialize cache and get user transaction Context ctx = cache.getJNDIContext();
   * UserTransaction ta = null; Connection xaConnBeforeTxn = null; Connection xaConnAfterTxn = null;
   * try { ta = (UserTransaction)ctx.lookup("java:/UserTransaction");
   *
   * } catch (NamingException e) { fail (" fail in user txn lookup " + e); }
   *
   * try{
   *
   * //get the XADataSource before the transaction begins DataSource
   * xads=(DataSource)ctx.lookup("java:/XAPooledDataSource");
   * System.out.println("XA data source obtained outside transaction");
   *
   * //Begin the user transaction ta.begin(); System.out.println("Transaction begun successfully");
   *
   * //Obtain XAPooledDataSource DataSource da = (DataSource)ctx.lookup("java:/XAPooledDataSource");
   * System.out.println("XA data source obtained within transaction");
   *
   * //obtain connection from SimpleDataSource and XAPooledDataSource xaConnAfterTxn =
   * da.getConnection(); System.out.println("Done with xaConnAfterTxn"); xaConnBeforeTxn =
   * xads.getConnection(); //xaConnAfterTxn = da.getConnection();
   * System.out.println("Done with xaConnBeforeTxn");
   *
   * Statement stmtBefore = xaConnBeforeTxn.createStatement(); Statement stmtAfter =
   * xaConnAfterTxn.createStatement(); System.out.println("Done with both the statements");
   *
   * //perform inserts and updates using both XA DataSources //String sqlSTR = "insert into " +
   * tblName + " values (" + tblIDFld + "," + "'" + tblNameFld + "'" + ")" ;
   *
   * String sqlSTR;
   *
   * //insert XA_INSERTS rows into timestamped table for (int i=0; i<XA_INSERTS_BEFORE;i++){
   * tblIDFld=tblIDFld+i; sqlSTR = "insert into " + tblName + " values (" + tblIDFld + "," + "'" +
   * tblNameFld + "'" + ")" ; stmtBefore.executeUpdate(sqlSTR); }
   * System.out.println("Done with inserts by stmtBefore");
   *
   * //insert SM_INSERTS rows into timestamped table for (int j=0; j<XA_INSERTS_AFTER;j++){
   * tblIDFld=tblIDFld+j+5; sqlSTR = "insert into " + tblName + " values (" + tblIDFld + "," + "'" +
   * tblNameFld + "'" + ")" ; stmtAfter.executeUpdate(sqlSTR); }
   * System.out.println("Done with inserts by stmtAfter");
   *
   * //close both the XA statements stmtBefore.close(); stmtAfter.close();
   * System.out.println("Closed both the statements");
   *
   *
   * //close the connections xaConnBeforeTxn.close(); xaConnAfterTxn.close();
   * System.out.println("Closed both the connections");
   *
   *
   * //rollback the transaction ta.rollback ();
   * System.out.println("Rows are rolled back in the database"); } catch (SQLException e) {
   * e.printStackTrace(); } catch (Exception e){ fail (" failed " + e); } finally { if
   * (xaConnBeforeTxn != null || xaConnAfterTxn != null) { try { //close the connections
   * xaConnBeforeTxn.close(); xaConnAfterTxn.close();
   * System.out.println("Closed both the statements"); } catch (Exception e) { e.printStackTrace();
   * } } } //get the number of rows in time stamped table //using getRows method of JTAUtils
   *
   * try{ int num_rows = jtaObj.getRows(tblName);
   * System.out.println("\nNumber of rows in Table under tests are: " +num_rows);
   *
   * //determine what got commited and what not
   *
   * switch (num_rows) { case 0:
   * System.out.println("Both XA DataSource transactions got rolled back!!"); break;
   *
   * case XA_INSERTS_BEFORE: System.out.println(
   * "Only XA DataSource transactions got committed...which was obtained before txn!" ); break;
   *
   * case XA_INSERTS_AFTER:
   * System.out.println("Only XA DataSource transactions within txn got committed!" ); break;
   *
   * default: System.out.println("Looks like that things are messed up...look into it");
   *
   * }
   *
   * } catch (SQLException sqle){ sqle.printStackTrace(); }
   *
   * System.out.println("test task for testScenario3 Succeessful");
   *
   * //destroying table try { System.out.println ("Destroying table: " + tblName);
   * CacheUtils.destroyTable(tblName); System.out.println ("Closing cache...");
   * System.out.println("destroyTable for testscenario 3 Successful!"); } catch (Exception e) { fail
   * (" failed during tear down of this test..." + e); } finally { CacheUtils.closeCache();
   * CacheUtils.ds.disconnect(); } }//end of testscenario3
   */

  /**
   * The following test is to verify that any DDL command should not commit the inserts before
   * it...if rollback is issued for the transaction
   */
  @Test
  public void testInsertsDDLRollback() throws Exception {
    Region currRegion = null;
    Cache cache;
    DistributedSystem ds = null;
    int tblIDFld;
    String tblNameFld;
    String tblName;
    String tableName = null;
    // boolean to_continue = true;
    final int XA_INSERTS = 3;

    // call to init method
    try {
      Properties props = new Properties();
      String path =
          createTempFileFromResource(CacheUtils.class, "cachejta.xml")
              .getAbsolutePath();
      props.setProperty(CACHE_XML_FILE, path);
      ds = connect(props);
      tableName = CacheUtils.init(ds, "JTATest");
      // System.out.println ("Table name: " + tableName);
      tblName = tableName;

      if (tableName == null || tableName.equals("")) {
        // to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    } catch (Exception e) {
      // to_continue = false;
      fail(" Aborting test at set up...[" + e + "]", e);
    }

    System.out.println("init for testscenario 4 Successful!");

    // test task
    cache = CacheUtils.getCache();
    tblName = tableName;
    currRegion = cache.getRegion(SEPARATOR + "root");
    tblIDFld = 1;
    tblNameFld = "test4";
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);

    // to delete all rows inserted in creatTable () of CacheUtils class
    // deleteRows method of JTAUtils class is used.
    jtaObj.deleteRows(tblName);

    // initialize cache and get user transaction
    Context ctx = cache.getJNDIContext();
    UserTransaction ta = null;
    Connection xaCon = null;
    try {
      ta = (UserTransaction) ctx.lookup("java:/UserTransaction");

    } catch (NamingException e) {
      fail(" fail in user txn lookup ", e);
    }

    try {

      // Begin the user transaction
      ta.begin();

      // Obtain XAPooledDataSource
      DataSource da = (DataSource) ctx.lookup("java:/XAPooledDataSource");

      // obtain connection from SimpleDataSource and XAPooledDataSource
      xaCon = da.getConnection();

      Statement stmt = xaCon.createStatement();

      // perform inserts and updates using both XA DataSources
      // String sqlSTR = "insert into " + tblName + " values (" + tblIDFld + ","
      // + "'" + tblNameFld + "'" + ")" ;

      String sqlSTR;

      // insert XA_INSERTS rows into timestamped table
      for (int i = 0; i < XA_INSERTS; i++) {
        tblIDFld = tblIDFld + i;
        sqlSTR =
            "insert into " + tblName + " values (" + tblIDFld + "," + "'" + tblNameFld + "'" + ")";
        stmt.executeUpdate(sqlSTR);
      }

      // insert SM_INSERTS rows into timestamped table
      String sql = "create table TEST (id integer, name varchar(50))";
      stmt.executeUpdate(sql);

      sql = "drop table TEST";
      stmt.executeUpdate(sql);
      // close both the XA statements
      stmt.close();

      // close the connections
      xaCon.close();

      // rollback the transaction
      ta.rollback();
      System.out.println("Rows are rolled back in the database");

    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      fail(" failed ", e);
    } finally {
      if (xaCon != null) {
        try {
          // close the connections
          xaCon.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    // get the number of rows in time stamped table
    // using getRows method of JTAUtils

    try {
      int num_rows = jtaObj.getRows(tblName);
      System.out.println("\nNumber of rows in Table under tests are: " + num_rows);

      // determine what got commited and what not

      switch (num_rows) {
        case 0:
          System.out.println("Nothing is committed to database");
          break;

        case XA_INSERTS:
          System.out.println("Inserts are committed to database...they should not!!:-(");
          break;

        default:
          System.out.println("Looks like that things are messed up...look into it");

      }

    } catch (SQLException sqle) {
      sqle.printStackTrace();
    }

    System.out.println("test task for testScenario4 Succeessful");

    // destroying table
    try {
      System.out.println("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      System.out.println("Closing cache...");
      System.out.println("destroyTable for testscenario 4 Successful!");
    } catch (Exception e) {
      fail(" failed during tear down of this test...", e);
    } finally {
      CacheUtils.closeCache();
      // CacheUtils.ds.disconnect();
      ds.disconnect();
    }
  }

  /**
   * This is to test that if commit is issues thencommit happen for all the XA Resources
   * operations...involved in the transaction
   */
  @Test
  public void testMultipleXAResourceCommit() throws Exception {
    Region currRegion = null;
    Cache cache;
    DistributedSystem ds = null;
    int tblIDFld;
    String tblNameFld;
    String tblName;
    String tableName = null;
    // boolean to_continue = true;
    final int XA_INSERTS = 3;

    // call to init method
    try {
      Properties props = new Properties();
      String path =
          createTempFileFromResource(CacheUtils.class, "cachejta.xml")
              .getAbsolutePath();
      props.setProperty(CACHE_XML_FILE, path);
      ds = connect(props);
      tableName = CacheUtils.init(ds, "JTATest");
      // System.out.println ("Table name: " + tableName);
      tblName = tableName;

      if (tableName == null || tableName.equals("")) {
        // to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    } catch (Exception e) {
      // to_continue = false;
      fail(" Aborting test at set up...[" + e + "]", e);
    }

    System.out.println("init for testscenario 5 Successful!");

    // test task
    cache = CacheUtils.getCache();
    tblName = tableName;
    currRegion = cache.getRegion(SEPARATOR + "root");
    tblIDFld = 1;
    tblNameFld = "test5";
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);

    // to delete all rows inserted in creatTable () of CacheUtils class
    // deleteRows method of JTAUtils class is used.
    jtaObj.deleteRows(tblName);

    // initialize cache and get user transaction
    Context ctx = cache.getJNDIContext();
    UserTransaction ta = null;
    Connection xaCon1 = null, xaCon2 = null, xaCon3 = null;
    try {
      ta = (UserTransaction) ctx.lookup("java:/UserTransaction");

    } catch (NamingException e) {
      fail(" fail in user txn lookup ", e);
    }

    try {

      // Begin the user transaction
      ta.begin();

      // Operation with first XA Resource
      DataSource da1 = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      xaCon1 = da1.getConnection();
      Statement stmt1 = xaCon1.createStatement();
      String sqlSTR;
      for (int i = 0; i < XA_INSERTS; i++) {
        tblIDFld = tblIDFld + i;
        sqlSTR =
            "insert into " + tblName + " values (" + tblIDFld + "," + "'" + tblNameFld + "'" + ")";
        stmt1.executeUpdate(sqlSTR);
      }
      stmt1.close();
      xaCon1.close();

      // Operation with second XA Resource
      DataSource da2 = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      xaCon2 = da2.getConnection();
      Statement stmt2 = xaCon2.createStatement();
      for (int i = 0; i < XA_INSERTS; i++) {
        tblIDFld = tblIDFld + i + 5;
        sqlSTR =
            "insert into " + tblName + " values (" + tblIDFld + "," + "'" + tblNameFld + "'" + ")";
        stmt2.executeUpdate(sqlSTR);
      }
      stmt2.close();
      xaCon2.close();

      // Operation with third XA Resource
      DataSource da3 = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      xaCon3 = da3.getConnection();
      Statement stmt3 = xaCon3.createStatement();
      for (int i = 0; i < XA_INSERTS; i++) {
        tblIDFld = tblIDFld + 10;
        sqlSTR =
            "insert into " + tblName + " values (" + tblIDFld + "," + "'" + tblNameFld + "'" + ")";
        stmt3.executeUpdate(sqlSTR);
      }
      stmt3.close();
      xaCon3.close();

      // commit the transaction
      ta.commit();
      System.out.println("Rows are rolled back in the database");

    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      fail(" failed ", e);
    } finally {
      if (xaCon1 != null || xaCon2 != null || xaCon3 != null) {
        try {
          // close the connections
          xaCon1.close();
          xaCon2.close();
          xaCon3.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    // get the number of rows in time stamped table
    // using getRows method of JTAUtils

    try {
      int num_rows = jtaObj.getRows(tblName);
      System.out.println("\nNumber of rows in Table under tests are: " + num_rows);

      // determine what got commited and what not

      switch (num_rows) {
        case 0:
          System.out.println("Nothing is committed to database");
          break;

        case 3 * XA_INSERTS:
          System.out.println("All inserts successfully are committed to database");
          break;

        default:
          System.out.println("Looks like that things are messed up...look into it");

      }

    } catch (SQLException sqle) {
      sqle.printStackTrace();
    }

    System.out.println("test task for testScenario5 Succeessful");

    // destroying table
    try {
      System.out.println("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      System.out.println("Closing cache...");
      System.out.println("destroyTable for testscenario 5 Successful!");
    } catch (Exception e) {
      fail(" failed during tear down of this test...", e);
    } finally {
      CacheUtils.closeCache();
      // CacheUtils.ds.disconnect();
      ds.disconnect();
    }
  }

  /*
   * If time of one of the XA Resources exceeds the blocking time outthen all the operation for that
   * transaction should be rolled backeven though test tries to commit those
   */

  /*
   * public static void testXAResourceBtoCommit () { Host host = Host.getHost (0); VM vm0 =
   * host.getVM (0); vm0.invoke (DataSourceJTAJUnitTest.class, "Scenario6"); }
   *
   * public static void Scenario6 () throws Exception { Region currRegion=null; Cache cache; int
   * tblIDFld; String tblNameFld; String tblName; String tableName=null; boolean to_continue = true;
   * final int XA_INSERTS=3;
   *
   * //call to init method try { tableName = CacheUtils.init("JTATest"); //System.out.println
   * ("Table name: " + tableName); tblName = tableName;
   *
   * if (tableName == null || tableName.equals ("")) { to_continue = false; fail
   * (" table name not created, Aborting test..."); } } catch (Exception e) { to_continue = false;
   * fail (" Aborting test at set up...[" + e + "]"); }
   *
   * System.out.println("init for testscenario 6 Successful!");
   *
   * //test task cache = CacheUtils.getCache (); tblName = tableName; currRegion =
   * cache.getRegion("/root"); tblIDFld = 1; tblNameFld = "test6"; JTAUtils jtaObj = new JTAUtils
   * (cache, currRegion);
   *
   * //to delete all rows inserted in creatTable () of CacheUtils class //deleteRows method of
   * JTAUtils class is used. int rowsDeleted = jtaObj.deleteRows(tblName);
   *
   * //initialize cache and get user transaction Context ctx = cache.getJNDIContext();
   * UserTransaction ta = null; Connection xaCon1=null,xaCon2=null,xaCon3=null; try { ta =
   * (UserTransaction)ctx.lookup("java:/UserTransaction");
   *
   * } catch (NamingException e) { fail (" fail in user txn lookup " + e); }
   *
   * try{
   *
   * //Begin the user transaction ta.begin();
   *
   * //Operation with first XA Resource DataSource da1 =
   * (DataSource)ctx.lookup("java:/XAPooledDataSource"); xaCon1 = da1.getConnection(); Statement
   * stmt1 = xaCon1.createStatement(); String sqlSTR; for (int i=0; i<XA_INSERTS;i++){
   * tblIDFld=tblIDFld+i; sqlSTR = "insert into " + tblName + " values (" + tblIDFld + "," + "'" +
   * tblNameFld + "'" + ")" ; stmt1.executeUpdate(sqlSTR); } stmt1.close(); xaCon1.close();
   *
   * //Operation with second XA Resource DataSource da2 =
   * (DataSource)ctx.lookup("java:/XAPooledDataSource"); xaCon2 = da2.getConnection(); Statement
   * stmt2 = xaCon2.createStatement(); for (int i=0; i<XA_INSERTS;i++){ tblIDFld=tblIDFld+i+5;
   * sqlSTR = "insert into " + tblName + " values (" + tblIDFld + "," + "'" + tblNameFld + "'" + ")"
   * ; stmt2.executeUpdate(sqlSTR); } stmt2.close(); xaCon2.close();
   *
   * //Operation with third XA Resource DataSource da3 =
   * (DataSource)ctx.lookup("java:/XAPooledDataSource"); xaCon3 = da3.getConnection(); Statement
   * stmt3 = xaCon3.createStatement(); for (int i=0; i<XA_INSERTS;i++){ tblIDFld=tblIDFld+ 10;
   * sqlSTR = "insert into " + tblName + " values (" + tblIDFld + "," + "'" + tblNameFld + "'" + ")"
   * ; stmt3.executeUpdate(sqlSTR); } Thread.sleep(22*1000); //stmt3.close(); //xaCon3.close();
   *
   * //commit the transaction ta.commit ();
   * System.out.println("Rows are rolled back in the database");
   *
   * } catch (SQLException e) { e.printStackTrace(); } catch (Exception e){ fail (" failed " + e); }
   * finally { if (xaCon1!=null || xaCon2!=null || xaCon3!=null) { try { //close the connections
   * xaCon1.close(); xaCon2.close(); xaCon3.close(); } catch (Exception e) { e.printStackTrace(); }
   * } } //get the number of rows in time stamped table //using getRows method of JTAUtils
   *
   * try{ int num_rows = jtaObj.getRows(tblName);
   * System.out.println("\nNumber of rows in Table under tests are: " +num_rows);
   *
   * //determine what got commited and what not
   *
   * switch (num_rows) { case 0: System.out.println("Nothing is committed to database"); break;
   *
   * case 3*XA_INSERTS: System.out.println(
   * "All inserts successfully are committed to database...this is NOT EXPECTED" ); break;
   *
   * default: System.out.println("Looks like that things are messed up...look into it");
   *
   * }
   *
   * } catch (SQLException sqle){ sqle.printStackTrace(); }
   *
   * System.out.println("test task for testScenario6 Succeessful");
   *
   * //destroying table try { System.out.println ("Destroying table: " + tblName);
   * CacheUtils.destroyTable(tblName); System.out.println ("Closing cache...");
   * System.out.println("destroyTable for testscenario 6 Successful!"); } catch (Exception e) { fail
   * (" failed during tear down of this test..." + e); } finally { CacheUtils.closeCache();
   * CacheUtils.ds.disconnect(); } }//end of testscenario6
   */

  private static DistributedSystem connect(Properties props) {
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    return DistributedSystem.connect(props);
  }

  private static void fail(String str, Throwable thr) {
    throw new AssertionError(str, thr);
  }

  private static void fail(String str) {
    throw new AssertionError(str);
  }
}
