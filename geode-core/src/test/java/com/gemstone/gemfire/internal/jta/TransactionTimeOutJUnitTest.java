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
package com.gemstone.gemfire.internal.jta;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.datasource.GemFireTransactionDataSource;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.util.test.TestUtil;
import junit.framework.TestCase;
import org.junit.FixMethodOrder;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

import javax.naming.Context;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;
import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;

//import java.sql.SQLException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(IntegrationTest.class)
public class TransactionTimeOutJUnitTest extends TestCase {

  private static Properties props = null;
  private static DistributedSystem ds1 = null;
  private static Cache cache = null;

  protected void setUp() throws Exception {
    super.setUp();
    props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    int pid = new Random().nextInt();
    File tmpFile = File.createTempFile("dunit-cachejta_", ".xml");
    tmpFile.deleteOnExit();
    String path = tmpFile.getAbsolutePath();
    String file_as_str = readFile(TestUtil.getResourcePath(TransactionTimeOutJUnitTest.class, "/jta/cachejta.xml"));
    String modified_file_str= file_as_str.replaceAll("newDB", "newDB_" + pid);
    FileOutputStream fos = new FileOutputStream(path);
    BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(fos));
    wr.write(modified_file_str);
    wr.flush();
    wr.close();
    props.setProperty(CACHE_XML_FILE, path);
    // props.setProperty(cacheXmlFile,"D:\\projects\\JTA\\cachejta.xml");
    ds1 = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds1);
    cache.getLogger().fine("SWAP:running test:"+getName());
  }

  protected void tearDown() throws Exception {
    super.tearDown();
    ds1.disconnect();
  }

  public TransactionTimeOutJUnitTest(String arg0) {
    super(arg0);
  }

  public void test1One() {
    try {
      UserTransaction utx = new UserTransactionImpl();
      utx.begin();
      Thread.sleep(2000);
      utx.setTransactionTimeout(2);
      utx.setTransactionTimeout(200);
      utx.setTransactionTimeout(3);
      Thread.sleep(5000);
      //utx.commit();
    } catch (Exception e) {
      fail("Exception in TestSetTransactionTimeOut due to " + e);
    }
  }

  public void test2SetTransactionTimeOut() {
    try {
      UserTransaction utx = new UserTransactionImpl();
      utx.begin();
      utx.setTransactionTimeout(2);
      System.out.println("Going to sleep");
      Thread.sleep(6000);
      utx.begin();
      utx.commit();
    } catch (Exception e) {
      fail("Exception in TestSetTransactionTimeOut due to " + e);
    }
  }

  public void test3ExceptionOnCommitAfterTimeOut() {
    UserTransaction utx;
    boolean exceptionOccured = true;
    try {
      utx = new UserTransactionImpl();
      utx.setTransactionTimeout(2);
      utx.begin();
      Thread.sleep(4000);
      try {
        utx.commit();
      } catch (Exception e) {
        exceptionOccured = false;
      }
      if (exceptionOccured) {
        fail("TimeOut did not rollback the transaction");
      }
    } catch (Exception e) {
      fail("Exception in testExceptionOnCommitAfterTimeOut() due to " + e);
    }
  }

  public void test4MultipleSetTimeOuts() {
    UserTransaction utx;
    boolean exceptionOccured = true;
    try {
      utx = new UserTransactionImpl();
      utx.setTransactionTimeout(10);
      utx.begin();
      utx.setTransactionTimeout(8);
      utx.setTransactionTimeout(6);
      utx.setTransactionTimeout(2);
      Thread.sleep(6000);
      try {
        utx.commit();
      } catch (Exception e) {
        exceptionOccured = false;
      }
      if (exceptionOccured) {
        fail("TimeOut did not rollback the transaction");
      }
    } catch (Exception e) {
      fail("Exception in testExceptionOnCommitAfterTimeOut() due to " + e);
    }
  }

  public void test5TimeOutBeforeBegin() {
    boolean exceptionOccured = true;
    try {
      UserTransaction utx = new UserTransactionImpl();
      utx.setTransactionTimeout(4);
      utx.begin();
      Thread.sleep(6000);
      try {
        utx.commit();
      } catch (Exception e) {
        exceptionOccured = false;
      }
      if (exceptionOccured) {
        fail("TimeOut did not rollback the transaction");
      }
    } catch (Exception e) {
      fail("Exception in testExceptionOnCommitAfterTimeOut() due to " + e);
    }
  }

  public void test6CommitBeforeTimeOut() {
//    boolean exceptionOccured = true;
    try {
      UserTransaction utx = new UserTransactionImpl();
      utx.begin();
      utx.setTransactionTimeout(6);
      Thread.sleep(2000);
      try {
        utx.commit();
      } catch (Exception e) {
        fail("Transaction failed to commit although TimeOut was not exceeded due to "
            + e);
      }
    } catch (Exception e) {
      fail("Exception in testExceptionOnCommitAfterTimeOut() due to " + e);
    }
  }

  public void test7Commit() {
    try {
      Context ctx = cache.getJNDIContext();
      DataSource ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
      ds2.getConnection();
      GemFireTransactionDataSource ds = (GemFireTransactionDataSource) ctx
          .lookup("java:/XAPooledDataSource");
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      Connection conn = ds.getConnection();
      String sql = "create table newTable1 (id integer)";
      Statement sm = conn.createStatement();
      sm.execute(sql);
      utx.setTransactionTimeout(30);
      Thread.sleep(5000);
      utx.setTransactionTimeout(20);
      utx.setTransactionTimeout(10);
      sql = "insert into newTable1  values (1)";
      sm.execute(sql);
      utx.commit();
      sql = "select * from newTable1 where id = 1";
      ResultSet rs = sm.executeQuery(sql);
      if (!rs.next()) fail("Transaction not committed");
      sql = "drop table newTable1";
      sm.execute(sql);
      sm.close();
      conn.close();
    } catch (Exception e) {
      fail("Exception occured in test Commit due to " + e);
      e.printStackTrace();
    }
  }

  public void test8CommitAfterTimeOut() {
    try {
      boolean exceptionOccured = false;
      Context ctx = cache.getJNDIContext();
      DataSource ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
      ds2.getConnection();
      GemFireTransactionDataSource ds = (GemFireTransactionDataSource) ctx
          .lookup("java:/XAPooledDataSource");
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      Connection conn = ds.getConnection();
      String sql = "create table newTable2 (id integer)";
      Statement sm = conn.createStatement();
      sm.execute(sql);
      utx.setTransactionTimeout(30);
      sql = "insert into newTable2  values (1)";
      sm.execute(sql);
      sql = "select * from newTable2 where id = 1";
      ResultSet rs = sm.executeQuery(sql);
      if (!rs.next()) fail("Transaction not committed");
      sql = "drop table newTable2";
      sm.execute(sql);
      sm.close();
      conn.close();
      utx.setTransactionTimeout(1);
      Thread.sleep(3000);
      try {
        utx.commit();
      } catch (Exception e) {
        exceptionOccured = true;
      }
      if (!exceptionOccured) {
        fail("exception did not occur on commit although transaction timed out");
      }
    } catch (Exception e) {
      fail("Exception occured in test Commit due to " + e);
      e.printStackTrace();
    }
  }

  public void test9RollbackAfterTimeOut() {
    try {
      boolean exceptionOccured = false;
      Context ctx = cache.getJNDIContext();
      DataSource ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
      Connection conn2 = ds2.getConnection();
      GemFireTransactionDataSource ds = (GemFireTransactionDataSource) ctx
          .lookup("java:/XAPooledDataSource");
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      Connection conn = ds.getConnection();
      String sql = "create table newTable3 (id integer)";
      Statement sm = conn.createStatement();
      sm.execute(sql);
      utx.setTransactionTimeout(30);
      sql = "insert into newTable3  values (1)";
      sm.execute(sql);
      sql = "select * from newTable3 where id = 1";
      ResultSet rs = sm.executeQuery(sql);
      if (!rs.next()) fail("Transaction not committed");
      sql = "drop table newTable3";
      sm.execute(sql);
      sm.close();
      conn.close();
      conn2.close();
      utx.setTransactionTimeout(1);
      Thread.sleep(3000);
      try {
        utx.rollback();
      } catch (Exception e) {
        exceptionOccured = true;
      }
      if (!exceptionOccured) {
        fail("exception did not occur on rollback although transaction timed out");
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception occured in test Commit due to " + e);
    }
  }

  private static String readFile(String filename) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(filename));
    String nextLine = "";
    StringBuffer sb = new StringBuffer();
    while ((nextLine = br.readLine()) != null) {
      sb.append(nextLine);
      //
      // note:
      //   BufferedReader strips the EOL character.
      //
      //    sb.append(lineSep);
    }
    return sb.toString();
  }

}
