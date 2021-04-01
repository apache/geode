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
package org.apache.geode.cache.wan.internal;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.Assert.assertTrue;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.ServiceLoader;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.spi.WANFactory;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({WanTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class GatewayReceiverXmlParsingValidationsJUnitTest {
  private Cache cache;
  private GatewayReceiverFactory receiverFactory;

  @Parameterized.Parameter
  public static String validationStrategy;

  @Rule
  public TestName testName = new SerializableTestName();

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> strategies() throws Exception {
    return Arrays.asList("DTD", "XSD");
  }

  @Test(expected = CacheXmlException.class)
  public void multipleReceiversShouldThrowException() {
    String cacheXmlFileName =
        createTempFileFromResource(getClass(),
            getClass().getSimpleName() + "." + testName.getMethodName() + ".cache.xml")
                .getAbsolutePath();
    cache = new CacheFactory().set(MCAST_PORT, "0").set(CACHE_XML_FILE, cacheXmlFileName).create();
  }

  @Test
  public void correctConfiguration() {
    String cacheXmlFileName =
        createTempFileFromResource(getClass(),
            getClass().getSimpleName() + "." + testName.getMethodName() + ".cache.xml")
                .getAbsolutePath();
    cache = new CacheFactory().set(MCAST_PORT, "0").set(CACHE_XML_FILE, cacheXmlFileName).create();

    assertThat(cache.getGatewayReceivers()).isNotEmpty();
    GatewayReceiver receiver = cache.getGatewayReceivers().iterator().next();

    ServiceLoader<WANFactory> loader = ServiceLoader.load(WANFactory.class);
    Iterator<WANFactory> itr = loader.iterator();
    assertThat(itr.hasNext()).isTrue();

    assertEquals(1501, receiver.getEndPort());
    assertEquals(1500, receiver.getStartPort());
    assertTrue(receiver.isManualStart());
    assertEquals(32768, receiver.getSocketBufferSize());
    assertTrue(receiver.getBindAddress().equals("localhost"));
    assertTrue(receiver.getHostnameForSenders().equals("localhost"));
    assertEquals(60000, receiver.getMaximumTimeBetweenPings());
  }

  @After
  public void tearDown() {
    if (cache != null) {
      cache.close();
    }
  }

  static class MyGatewayReceiverFactoryImpl implements GatewayReceiverFactory {
    InternalCache cache;
    int startPort;
    int endPort;
    int socketBuffSize;
    int timeBetPings;
    boolean manualStart;
    String bindAdd;
    String hostnameForSenders;

    public MyGatewayReceiverFactoryImpl(InternalCache cache) {
      this.cache = cache;
    }

    @Override
    public GatewayReceiverFactory setStartPort(int startPort) {
      this.startPort = startPort;
      return this;
    }

    @Override
    public GatewayReceiverFactory setEndPort(int endPort) {
      this.endPort = endPort;
      return this;
    }

    @Override
    public GatewayReceiverFactory setSocketBufferSize(int socketBufferSize) {
      this.socketBuffSize = socketBufferSize;
      return this;
    }

    @Override
    public GatewayReceiverFactory setBindAddress(String address) {
      this.bindAdd = address;
      return this;
    }

    @Override
    public GatewayReceiverFactory addGatewayTransportFilter(GatewayTransportFilter filter) {
      return null;
    }

    @Override
    public GatewayReceiverFactory removeGatewayTransportFilter(GatewayTransportFilter filter) {
      return null;
    }

    @Override
    public GatewayReceiverFactory setMaximumTimeBetweenPings(int time) {
      this.timeBetPings = time;
      return this;
    }

    @Override
    public GatewayReceiverFactory setHostnameForSenders(String address) {
      this.hostnameForSenders = address;
      return this;
    }

    @Override
    public GatewayReceiverFactory setManualStart(boolean start) {
      this.manualStart = start;
      return this;
    }

    @Override
    public GatewayReceiver create() {
      GatewayReceiver receiver = mock(GatewayReceiver.class);
      when(receiver.isManualStart()).thenReturn(this.manualStart);
      when(receiver.getBindAddress()).thenReturn(this.bindAdd);
      when(receiver.getEndPort()).thenReturn(this.endPort);
      when(receiver.getStartPort()).thenReturn(this.startPort);
      when(receiver.getSocketBufferSize()).thenReturn(this.socketBuffSize);
      when(receiver.getHostnameForSenders()).thenReturn(this.hostnameForSenders);
      when(receiver.getMaximumTimeBetweenPings()).thenReturn(this.timeBetPings);
      this.cache.addGatewayReceiver(receiver);
      return receiver;
    }

    public boolean isManualStart() {
      return this.manualStart;
    }
  }

}
