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
package org.apache.geode.management;

import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_JMX;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEventsListener;
import org.apache.geode.internal.cache.InternalCache;

/**
 * Integration test to ensure that Geode does not create MBeans when
 * {@link ConfigurationProperties#DISABLE_JMX} is set to true.
 */
public class DisableJmxIntegrationTest {

  private InternalCache cache;

  @Before
  public void setUp() {
    Properties config = new Properties();
    config.setProperty(DISABLE_JMX, "true");
    config.setProperty(LOCATORS, "");

    cache = (InternalCache) new CacheFactory(config).create();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void disableJmxPreventsRegistrationOfManagementListener() {
    InternalDistributedSystem system = cache.getInternalDistributedSystem();

    List<ResourceEventsListener> result = system.getResourceListeners();

    assertThat(result).isEmpty();
  }

  @Test
  public void disableJmxPreventsCreationOfMemberMXBean() {
    ManagementService managementService = ManagementService.getManagementService(cache);

    MemberMXBean result = managementService.getMemberMXBean();

    assertThat(result).isNull();
  }
}
