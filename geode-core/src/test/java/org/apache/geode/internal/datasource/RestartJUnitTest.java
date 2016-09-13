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
 * Created on Mar 22, 2005
 */
package org.apache.geode.internal.datasource;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.util.test.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.transaction.TransactionManager;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

//import org.apache.geode.internal.jta.CacheUtils;

/**
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
      props.setProperty(MCAST_PORT, "0");
    String path = TestUtil.getResourcePath(RestartJUnitTest.class, "/jta/cachejta.xml");
      props.setProperty(CACHE_XML_FILE, path);

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

