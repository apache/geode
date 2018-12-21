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
package org.apache.geode.internal.cache.wan;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

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
import org.apache.geode.internal.cache.wan.spi.WANFactory;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.util.test.TestUtil;

@Category({WanTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class GatewayReceiverXmlParsingValidationsJUnitTest {
  private Cache cache;

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
    String cacheXmlFileName = TestUtil.getResourcePath(getClass(),
        getClass().getSimpleName() + "." + testName.getMethodName() + ".cache.xml");
    cache = new CacheFactory().set(MCAST_PORT, "0").set(CACHE_XML_FILE, cacheXmlFileName).create();
  }

  @Test
  public void correctConfiguration() {
    String cacheXmlFileName = TestUtil.getResourcePath(getClass(),
        getClass().getSimpleName() + "." + testName.getMethodName() + ".cache.xml");
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

}
