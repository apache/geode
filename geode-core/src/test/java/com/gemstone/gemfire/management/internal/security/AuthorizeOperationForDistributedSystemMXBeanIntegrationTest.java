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
package com.gemstone.gemfire.management.internal.security;

import static org.junit.Assert.*;

import javax.management.remote.JMXPrincipal;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.util.test.TestUtil;

/**
 * Tests <code>JSONAuthorization.authorizeOperation(...)</code> for <code>DistributedSystemMXBean</code> operations.
 */
@Category(IntegrationTest.class)
public class AuthorizeOperationForDistributedSystemMXBeanIntegrationTest {

  @Test
  public void returnsFalseForUnauthorizedUser() throws Exception {    
    System.setProperty("resource.secDescriptor", TestUtil.getResourcePath(getClass(), "auth1.json")); 
    JSONAuthorization authorization = JSONAuthorization.create();        
    authorization.init(new JMXPrincipal("tushark"), null, null);
    
    JMXOperationContext context = new JMXOperationContext(MBeanJMXAdapter.getDistributedSystemName(), "queryData");
    boolean result = authorization.authorizeOperation(null, context);
    //assertTrue(result); TODO: why is this commented out? looks like this should be true but it isn't
    
    context = new JMXOperationContext(MBeanJMXAdapter.getDistributedSystemName(), "changeAlertLevel");
    result = authorization.authorizeOperation(null,context);
    assertFalse(result);
  }
}
