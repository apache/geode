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
/**
 *
 */
package org.apache.geode.internal.cache.wan;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.util.test.TestUtil;

@RunWith(PowerMockRunner.class)
@Category(IntegrationTest.class)
@PrepareForTest(WANServiceProvider.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PowerMockIgnore({"javax.management.*", "javax.security.*", "*.IntegrationTest"})
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

  @Before
  public void setUp() throws Exception {
    mockStatic(WANServiceProvider.class);
    receiverFactory = spy(GatewayReceiverFactory.class);
    when(WANServiceProvider.createGatewayReceiverFactory(any())).thenReturn(receiverFactory);
  }

  @Test(expected = CacheXmlException.class)
  public void multipleReceiversShouldThrowException() {
    String cacheXmlFileName = TestUtil.getResourcePath(getClass(),
        getClass().getSimpleName() + "." + testName.getMethodName() + ".cache.xml");
    cache = new CacheFactory().set(MCAST_PORT, "0").set(CACHE_XML_FILE, cacheXmlFileName).create();
  }

  @Test
  public void correctConfiguration() {
    String cacheXmlFileName = TestUtil.getResourcePath(getClass(),
        getClass().getSimpleName() + "." + testName.getMethodName() + ".cache.xml");
    cache = new CacheFactory().set(MCAST_PORT, "0").set(CACHE_XML_FILE, cacheXmlFileName).create();

    assertThat(cache.getGatewayReceivers()).isNotNull();
    verify(receiverFactory, times(1)).setEndPort(1501);
    verify(receiverFactory, times(1)).setStartPort(1500);
    verify(receiverFactory, times(1)).setManualStart(true);
    verify(receiverFactory, times(1)).setSocketBufferSize(32768);
    verify(receiverFactory, times(1)).setBindAddress("localhost");
    verify(receiverFactory, times(1)).setHostnameForSenders("localhost");
    verify(receiverFactory, times(1)).setMaximumTimeBetweenPings(60000);
    verify(receiverFactory, times(1)).create();
  }

  @After
  public void tearDown() {
    if (cache != null) {
      cache.close();
    }
  }
}
