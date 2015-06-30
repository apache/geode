/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Created on Mar 22, 2005
 */
package com.gemstone.gemfire.internal.datasource;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

import java.util.Properties;

//import com.gemstone.gemfire.internal.jta.CacheUtils;
import javax.transaction.TransactionManager;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.jndi.JNDIInvoker;
import com.gemstone.gemfire.util.test.TestUtil;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * @author Nand Kishor
 * 
 * This test check the graceful removal of all the resource
 * (DataSources , TransactionManager and UserTransaction and 
 * its associated thread) before we reconnect to the distributed 
 * syatem.
 */
@Category(IntegrationTest.class)
public class RestartJUnitTest {

  private static Properties props = null;
  private static DistributedSystem ds1 = null;
  private static Cache cache = null;

  @Test
  public void testCleanUp() {
	TransactionManager tm1 = null;
	TransactionManager tm2 = null;
    try{
    props = new Properties();
    props.setProperty("mcast-port","0");
    String path = TestUtil.getResourcePath(RestartJUnitTest.class, "/jta/cachejta.xml");
    props.setProperty("cache-xml-file",path);

    ds1 = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds1);
    tm1 = JNDIInvoker.getTransactionManager();
    cache.close();
    ds1.disconnect();

    ds1 = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds1);
    tm2 = JNDIInvoker.getTransactionManager();
    assertNotSame("TransactionManager are same in before restart and after restart",tm1,tm2);
    
    ds1.disconnect();
  }catch(Exception e){
    fail("Failed in restarting the distributed system");
}
  }
}

