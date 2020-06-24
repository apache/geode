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
package org.apache.geode.management.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.eclipse.jetty.server.ServerConnector;
import org.junit.After;
import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalHttpService;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.impl.ServiceLoaderModuleService;

/**
 * The InternalHttpServiceJunitTest class is a test suite of test cases testing the contract and
 * functionality of the InternalHttpService class. It does not start Jetty.
 */
public class InternalHttpServiceJunitTest {
  private DistributionConfig distributionConfig;
  private InternalCache cache;

  public void setup(String bindAddress, int port) {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_BIND_ADDRESS_NAME, bindAddress);
    props.setProperty(DistributionConfig.HTTP_SERVICE_PORT_NAME, "" + port);
    distributionConfig = new DistributionConfigImpl(props);

    InternalDistributedSystem ds = mock(InternalDistributedSystem.class);
    when(ds.getConfig()).thenReturn(distributionConfig);

    cache = mock(InternalCache.class);
    when(cache.getDistributedSystem()).thenReturn(ds);
    when(cache.isServer()).thenReturn(true);
  }

  @After
  public void teardown() {}

  @Test
  public void testSetPortNoBindAddress() {
    setup("", 8090);

    final InternalHttpService jetty = new InternalHttpService();
    boolean didInit = jetty.init(cache, new ServiceLoaderModuleService(LogService.getLogger()));

    assertThat(didInit).as("Jetty did not initialize").isTrue();
    assertThat(jetty.getHttpServer().getConnectors()[0]).isNotNull();
    assertThat(((ServerConnector) jetty.getHttpServer().getConnectors()[0]).getPort())
        .isEqualTo(8090);
  }

  @Test
  public void testSetPortWithBindAddress() {
    setup("127.0.0.1", 10480);

    final InternalHttpService jetty = new InternalHttpService();
    boolean didInit = jetty.init(cache, new ServiceLoaderModuleService(LogService.getLogger()));

    assertThat(didInit).as("Jetty did not initialize").isTrue();
    assertThat(jetty.getHttpServer().getConnectors()[0]).isNotNull();
    assertThat(((ServerConnector) jetty.getHttpServer().getConnectors()[0]).getPort())
        .isEqualTo(10480);
    assertThat(((ServerConnector) jetty.getHttpServer().getConnectors()[0]).getHost())
        .isEqualTo("127.0.0.1");
  }
}
