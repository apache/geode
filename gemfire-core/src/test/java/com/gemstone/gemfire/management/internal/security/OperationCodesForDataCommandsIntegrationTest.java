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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.internal.security.ResourceOperationContext.ResourceOperationCode;
import com.gemstone.gemfire.management.internal.security.AuthorizeOperationForMBeansIntegrationTest.TestAccessControl;
import com.gemstone.gemfire.management.internal.security.AuthorizeOperationForMBeansIntegrationTest.TestAuthenticator;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests operation codes for data commands.
 */
@Category(IntegrationTest.class)
@SuppressWarnings("deprecation")
public class OperationCodesForDataCommandsIntegrationTest {

  private GemFireCacheImpl cache;
  private DistributedSystem ds;
  private Map<String, ResourceOperationCode> commands = new HashMap<String, ResourceOperationCode>();
  
  @Rule
  public TestName testName = new TestName();
  
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    System.setProperty("resource-auth-accessor", TestAccessControl.class.getName());
    System.setProperty("resource-authenticator", TestAuthenticator.class.getName());
    
    Properties properties = new Properties();
    properties.put("name", testName.getMethodName());
    properties.put(DistributionConfig.LOCATORS_NAME, "");
    properties.put(DistributionConfig.MCAST_PORT_NAME, "0");
    properties.put(DistributionConfig.HTTP_SERVICE_PORT_NAME, "0");
    
    this.ds = DistributedSystem.connect(properties);
    this.cache = (GemFireCacheImpl) CacheFactory.create(ds);

    this.commands.put("put --key=k1 --value=v1 --region=/region1", ResourceOperationCode.PUT_REGION);    
    this.commands.put("locate entry --key=k1 --region=/region1", ResourceOperationCode.LOCATE_ENTRY_REGION);
    this.commands.put("query --query=\"select * from /region1\"", ResourceOperationCode.QUERYDATA_DS);
    this.commands.put("export data --region=value --file=value --member=value", ResourceOperationCode.EXPORT_DATA_REGION);
    this.commands.put("import data --region=value --file=value --member=value", ResourceOperationCode.IMPORT_DATA_REGION);
    this.commands.put("rebalance", ResourceOperationCode.REBALANCE_DS);
  }

  @After
  public void tearDown() throws IOException {
    if (this.cache != null) {
      this.cache.close();
      this.cache = null;
    }
    if (this.ds != null) {
      this.ds.disconnect();
      this.ds = null;
    }
  }
  
  @Test
  public void commandsShouldMapToCorrectResourceCodes() throws Exception {
    for (String command : this.commands.keySet()) {
      CLIOperationContext ctx = new CLIOperationContext(command);
      assertThat(ctx.getResourceOperationCode()).isEqualTo(this.commands.get(command));
    }
  }
}
