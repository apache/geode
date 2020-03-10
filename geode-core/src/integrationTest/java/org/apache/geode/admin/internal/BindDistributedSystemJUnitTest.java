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
package org.apache.geode.admin.internal;

import static org.apache.geode.distributed.ConfigurationProperties.BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.After;
import org.junit.Test;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;

/**
 * Tests {@link org.apache.geode.admin.internal.AdminDistributedSystemImpl}.
 *
 * @created August 30, 2004
 * @since GemFire 3.5
 */
@SuppressWarnings("deprecation")
public class BindDistributedSystemJUnitTest {

  private static final int RETRY_ATTEMPTS = 3;
  private static final int RETRY_SLEEP = 100;

  private DistributedSystem system;

  @After
  public void tearDown() {
    if (this.system != null) {
      this.system.disconnect();
    }
    this.system = null;
  }

  // public void testBindToAddressNull() throws Exception {
  // DistributedSystemFactory.bindToAddress(null);
  // todo...
  // }
  //
  // public void testBindToAddressEmpty() throws Exception {
  // DistributedSystemFactory.bindToAddress("");
  // todo...
  // }

  @Test
  public void testBindToAddressLoopback() throws Exception {
    String bindTo = "127.0.0.1";

    Properties props = new Properties();
    props.setProperty(BIND_ADDRESS, bindTo);
    props.setProperty(START_LOCATOR,
        "localhost[" + AvailablePortHelper.getRandomAvailableTCPPort() + "]");
    this.system = org.apache.geode.distributed.DistributedSystem.connect(props);

    assertEquals(true, this.system.isConnected());

    // Because of fix for bug 31409
    this.system.disconnect();

  }


}
