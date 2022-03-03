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

import static org.apache.geode.distributed.ConfigurationProperties.CONSERVE_SOCKETS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;

import org.apache.geode.distributed.DistributedSystem;

/**
 * Provides common setUp and tearDown for testing the Admin API.
 *
 * @since GemFire 3.5
 */
public abstract class DistributedSystemTestCase {

  /** The DistributedSystem used for this test */
  protected DistributedSystem system;

  /**
   * Creates a "loner" <code>DistributedSystem</code> for this test.
   */
  @Before
  public void setUp() throws Exception {
    system = DistributedSystem.connect(defineProperties());
  }

  /**
   * Closes the "loner" <code>DistributedSystem</code>
   */
  @After
  public void tearDown() throws Exception {
    if (system != null) {
      system.disconnect();
    }
    system = null;
  }

  /**
   * Defines the <code>Properties</code> used to connect to the distributed system.
   */
  protected Properties defineProperties() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(CONSERVE_SOCKETS, "true");
    return props;
  }
}
