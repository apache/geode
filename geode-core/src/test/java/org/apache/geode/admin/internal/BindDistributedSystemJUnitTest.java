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
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.BIND_ADDRESS;
import static com.gemstone.gemfire.distributed.ConfigurationProperties.START_LOCATOR;
import static org.junit.Assert.assertEquals;

/**
 * Tests {@link com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl}.
 *
 * @created   August 30, 2004
 * @since GemFire     3.5
 */
@SuppressWarnings("deprecation")
@Category(IntegrationTest.class)
public class BindDistributedSystemJUnitTest {

  private final static int RETRY_ATTEMPTS = 3;
  private final static int RETRY_SLEEP = 100;

  private DistributedSystem system;

  @After
  public void tearDown() {
    if (this.system != null) {
      this.system.disconnect();
    }
    this.system = null;
  }
  
//  public void testBindToAddressNull() throws Exception {
//    DistributedSystemFactory.bindToAddress(null);
//     todo...
//  }
//
//  public void testBindToAddressEmpty() throws Exception {
//    DistributedSystemFactory.bindToAddress("");
//     todo...
//  }

  @Test
  public void testBindToAddressLoopback() throws Exception {
    String bindTo = "127.0.0.1";
    // make sure bindTo is the loopback... needs to be later in test...
    assertEquals(true, InetAddressUtil.isLoopback(bindTo));

    Properties props = new Properties();
    props.setProperty(BIND_ADDRESS, bindTo);
    props.setProperty(START_LOCATOR,
        "localhost["+AvailablePortHelper.getRandomAvailableTCPPort()+"]");
    this.system = com.gemstone.gemfire.distributed.DistributedSystem.connect(
        props);
        
    assertEquals(true, this.system.isConnected());

    // Because of fix for bug 31409
    this.system.disconnect();

  }


}

