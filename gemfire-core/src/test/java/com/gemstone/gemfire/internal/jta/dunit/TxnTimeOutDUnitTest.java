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
package com.gemstone.gemfire.internal.jta.dunit;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.transaction.UserTransaction;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.jta.CacheUtils;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.util.test.TestUtil;

/**
*@author Mitul D Bid
*This test sees if the TransactionTimeOut works properly
*/
public class TxnTimeOutDUnitTest extends DistributedTestCase {

  static DistributedSystem ds;
  static Cache cache = null;
  private static String tblName;
  private boolean exceptionOccured = false;

  public TxnTimeOutDUnitTest(String name) {
    super(name);
  }

  public static void init() throws Exception {
    Properties props = new Properties();
    int pid = OSProcess.getId();
    String path = File.createTempFile("dunit-cachejta_", ".xml").getAbsolutePath();
    String file_as_str = readFile(TestUtil.getResourcePath(CacheUtils.class, "cachejta.xml"));
    String modified_file_str = file_as_str.replaceAll("newDB", "newDB_" + pid);
    FileOutputStream fos = new FileOutputStream(path);
    BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(fos));
    wr.write(modified_file_str);
    wr.flush();
    wr.close();
    props.setProperty("cache-xml-file", path);
//    props.setProperty("mcast-port", "10321");
    props.setProperty("log-level", getDUnitLogLevel());
    try {
//      ds = DistributedSystem.connect(props);
      ds = (new TxnTimeOutDUnitTest("temp")).getSystem(props);
      if (cache == null || cache.isClosed()) cache = CacheFactory.create(ds);
    }
    catch (Exception e) {
      e.printStackTrace(System.err);
      throw new Exception("" + e);
    }
  }

  public static Cache getCache() {
    return cache;
  }

  public static void startCache() {
    try {
      if (cache.isClosed()) {
        cache = CacheFactory.create(ds);
      }
    }
    catch (Exception e) {
	fail("Exception in starting cache due to "+e);
      e.printStackTrace();
    }
  }

  public static void closeCache() {
    try {
      if (cache != null && !cache.isClosed()) {
        cache.close();
      }
	   if (ds != null && ds.isConnected())
			ds.disconnect();

    }
    catch (Exception e) {
	fail("Exception in closing cache or ds due to "+e);
      e.printStackTrace();
    }
  }

  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(TxnTimeOutDUnitTest.class, "init");
  }

  public void tearDown2() throws NamingException, SQLException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    vm0.invoke(TxnTimeOutDUnitTest.class, "closeCache");
  }

  public void testMultiThreaded() throws NamingException, SQLException,Throwable {
    try{
	Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
	
	Object o[]= new Object[1];
	o[0]= new Integer(2);
        AsyncInvocation asyncObj1 =  vm0.invokeAsync(TxnTimeOutDUnitTest.class, "runTest3",o);
    
	Object o1[]= new Object[1];
	o1[0]= new Integer(2);
        AsyncInvocation asyncObj2 = vm0.invokeAsync(TxnTimeOutDUnitTest.class,
        "runTest3",o1);
    
	Object o2[]= new Object[1];
	o2[0]= new Integer(3);
        AsyncInvocation asyncObj3 = vm0.invokeAsync(TxnTimeOutDUnitTest.class,
        "runTest3",o2);
    
	Object o3[]= new Object[1];
	o3[0]= new Integer(3);
        AsyncInvocation asyncObj4 =vm0.invokeAsync(TxnTimeOutDUnitTest.class,
        "runTest3",o3);
	
	Object o4[]= new Object[1];
	o4[0]= new Integer(1);
        AsyncInvocation asyncObj5 = vm0.invokeAsync(TxnTimeOutDUnitTest.class,
        "runTest3",o4);

        DistributedTestCase.join(asyncObj1, 5 * 60 * 1000, getLogWriter());        
        if(asyncObj1.exceptionOccurred()){
          fail("asyncObj1 failed", asyncObj1.getException());
        }
        
        DistributedTestCase.join(asyncObj2, 5 * 60 * 1000, getLogWriter());        
        if(asyncObj2.exceptionOccurred()){
          fail("asyncObj2 failed", asyncObj2.getException());
        }
        
        DistributedTestCase.join(asyncObj3, 5 * 60 * 1000, getLogWriter());        
        if(asyncObj3.exceptionOccurred()){
          fail("asyncObj3 failed", asyncObj3.getException());
        }
        
        DistributedTestCase.join(asyncObj4, 5 * 60 * 1000, getLogWriter());        
        if(asyncObj4.exceptionOccurred()){
          fail("asyncObj4 failed", asyncObj4.getException());
        }
        
        DistributedTestCase.join(asyncObj5, 5 * 60 * 1000, getLogWriter());        
        if(asyncObj5.exceptionOccurred()){
          fail("asyncObj5 failed", asyncObj5.getException());
        }
        
  
  }
  catch(Exception e){
  fail("exception occured in testMultiThreaded due to "+e);
  e.printStackTrace();
  }
  }
  public static void testLoginTimeOut() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    AsyncInvocation asyncObj1 =  vm0.invokeAsync(TxnTimeOutDUnitTest.class,
        "runTest2");
    AsyncInvocation asyncObj2 =    vm0.invokeAsync(TxnTimeOutDUnitTest.class, "runTest1");

    DistributedTestCase.join(asyncObj1, 5 * 60 * 1000, getLogWriter());
    if(asyncObj1.exceptionOccurred()){
      fail("asyncObj1 failed", asyncObj1.getException());
    }
    
    DistributedTestCase.join(asyncObj2, 5 * 60 * 1000, getLogWriter());
    if(asyncObj2.exceptionOccurred()){
      fail("asyncObj2 failed", asyncObj2.getException());
    }
    
  }

  public static void runTest1() throws Exception {
    boolean exceptionOccured = false;
    try {
      system.getLogWriter().fine("<ExpectedException action=add> +" +
      		"DistributedSystemDisconnectedException" +
      		"</ExpectedException>");
      Context ctx = cache.getJNDIContext();
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      utx.setTransactionTimeout(2);
      Thread.sleep(6000);
      try {
        utx.commit();
      }
      catch (Exception e) {
        exceptionOccured = true;
      }
      if (!exceptionOccured)
          fail("Exception did not occur although was supposed to occur");
    }
    catch (Exception e) {
      getLogWriter().fine("Exception caught " + e);
      fail("failed in naming lookup: " + e);
    }
    finally {
      system.getLogWriter().fine("<ExpectedException action=remove> +" +
          "DistributedSystemDisconnectedException" +
          "</ExpectedException>");
    }
  }

  public static void runTest2() throws Exception {
    boolean exceptionOccured = false;
    try {
      Context ctx = cache.getJNDIContext();
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      utx.setTransactionTimeout(2);
      Thread.sleep(6000);
      try {
        utx.commit();
      }
      catch (Exception e) {
        exceptionOccured = true;
      }
      if (!exceptionOccured)
          fail("Exception did not occur although was supposed to occur");
    }
    catch (Exception e) {
      getLogWriter().fine("Exception caught " + e);
      fail("failed in naming lookup: " + e);
    }
  }

  public static  void runTest3(Object o) {
  
	   boolean exceptionOccured = false;
	   try 
	   {
			int sleeptime = ((Integer)o).intValue();
			Context ctx = cache.getJNDIContext();
			UserTransaction utx = (UserTransaction) ctx.lookup("java:/UserTransaction");
			utx.begin();
			utx.setTransactionTimeout(sleeptime);
			Thread.sleep(sleeptime * 2000);
			try {
				 utx.commit();
			}
			catch (Exception e) {
				 exceptionOccured = true;
			}
			if(!exceptionOccured)
			fail("exception did not occur although was supposed to"
			+" occur");
	   }
	   catch (Exception e) {
	   	fail("Exception in runTest3 due to "+e);
			e.printStackTrace();
	   }
  }
  private static String readFile(String filename) throws IOException {
//    String lineSep = System.getProperty("\n");
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
    getLogWriter().fine("***********\n " + sb);
    return sb.toString();
  }
} 

