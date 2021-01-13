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
package org.apache.geode.cache.lucene;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import junitparams.JUnitParamsRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.WritablePdxInstance;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableConsumerIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ClientCacheRule;

@Category({SecurityTest.class, LuceneTest.class})
@RunWith(JUnitParamsRunner.class)
public class LuceneClientSecurityPostProcessingDUnitTest extends LuceneQueriesAccessorBase {

  private VM accessor2;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    accessor2 = Host.getHost(0).getVM(4);
  }

  @Rule
  public ClientCacheRule client = new ClientCacheRule();

  @Test
  public void verifyPostProcessing() {
    int serverPort1 = dataStore1.invoke(this::startCacheServer);
    dataStore1.invoke(this::createRegionIndex);

    int serverPort2 = dataStore2.invoke(this::startCacheServer);
    dataStore2.invoke(this::createRegionIndex);

    accessor.invoke(() -> startClient(serverPort1));
    accessor2.invoke(() -> startClient(serverPort2));

    accessor.invoke(this::putData);

    SerializableConsumerIF<Object> assertion = x -> {
      org.apache.geode.cache.lucene.test.TestObject testObject =
          (org.apache.geode.cache.lucene.test.TestObject) x;
      assertThat(testObject.getField2()).isEqualTo("***");
    };

    accessor.invoke(() -> executeTextSearch(assertion));
    accessor2.invoke(() -> executeTextSearch(assertion));
  }

  @Test
  public void verifyPdxPostProcessing() {
    int serverPort1 = dataStore1.invoke(this::startCacheServer);
    dataStore1.invoke(this::createRegionIndex);

    int serverPort2 = dataStore2.invoke(this::startCacheServer);
    dataStore2.invoke(this::createRegionIndex);

    accessor.invoke(() -> startClient(serverPort1));
    accessor2.invoke(() -> startClient(serverPort2));

    accessor.invoke(this::putPdxData);

    final SerializableConsumerIF<Object> pdxAssertion = x -> {
      PdxInstance pdx = (PdxInstance) x;
      assertThat(pdx.getField("field2")).isEqualTo("***");
    };

    accessor.invoke(() -> executeTextSearch(pdxAssertion));
    accessor2.invoke(() -> executeTextSearch(pdxAssertion));
  }

  private void putData() {
    Region<String, org.apache.geode.cache.lucene.test.TestObject> region =
        getClientCache().getRegion(REGION_NAME);

    for (int i = 0; i < 5; i++) {
      region.put("key-" + i, new org.apache.geode.cache.lucene.test.TestObject("hello", "world"));
    }
  }

  private void putPdxData() {
    Region<String, org.apache.geode.cache.lucene.test.TestPdxObject> region =
        getClientCache().getRegion(REGION_NAME);

    for (int i = 0; i < 5; i++) {
      region.put("key-" + i,
          new org.apache.geode.cache.lucene.test.TestPdxObject("hello", "world"));
    }
  }

  protected void createRegionIndex() {
    Cache cache = getCache();
    LuceneService luceneService = LuceneServiceProvider.get(cache);
    luceneService.createIndexFactory().addField("field1").create(INDEX_NAME, REGION_NAME);
    cache.createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
  }

  private int startCacheServer() throws IOException {
    disconnectFromDS();

    Properties props = getDistributedSystemProperties();
    props.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    props.setProperty(SECURITY_POST_PROCESSOR, RedactField2PostProcessor.class.getName());
    getSystem(props);

    CacheFactory cf = new CacheFactory();
    cf.setPdxReadSerialized(true);
    Cache cache = getCache(cf);

    CacheServer server = cache.addCacheServer();
    server.setPort(0);
    server.start();

    return server.getPort();
  }

  private void startClient(int serverPort) throws InterruptedException {
    Properties props = new Properties();
    props.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.cache.lucene.test.TestObject");
    props.setProperty("security-username", "DATA");
    props.setProperty("security-password", "DATA");
    props.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName());

    ClientCacheFactory clientCacheFactory = new ClientCacheFactory(props);
    clientCacheFactory.addPoolServer("localhost", serverPort);
    clientCacheFactory.setPdxReadSerialized(true);
    ClientCache clientCache = getClientCache(clientCacheFactory);
    clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(REGION_NAME);

    LuceneService service = LuceneServiceProvider.get(getCache());
    service.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "hello", "field1");
    service.waitUntilFlushed(INDEX_NAME, REGION_NAME, 5, TimeUnit.MINUTES);
  }

  private void executeTextSearch(Consumer<Object> assertion)
      throws LuceneQueryException, InterruptedException {
    LuceneService service = LuceneServiceProvider.get(getCache());
    LuceneQuery<String, Object> query = service
        .createLuceneQueryFactory()
        .create(INDEX_NAME, REGION_NAME, "hello", "field1");

    service.waitUntilFlushed(INDEX_NAME, REGION_NAME, 5, TimeUnit.MINUTES);

    List<LuceneResultStruct<String, Object>> results = query.findResults();

    assertThat(results).hasSize(5);

    for (LuceneResultStruct<String, Object> result : results) {
      assertion.accept(result.getValue());
    }
  }

  public static class RedactField2PostProcessor implements PostProcessor {

    @Override
    public void init(Properties securityProps) {}

    @Override
    public Object processRegionValue(Object principal, String regionName, Object key,
        Object value) {
      if (value instanceof org.apache.geode.cache.lucene.test.TestObject) {
        org.apache.geode.cache.lucene.test.TestObject newValue =
            new org.apache.geode.cache.lucene.test.TestObject(
                ((org.apache.geode.cache.lucene.test.TestObject) value).getField1(),
                ((org.apache.geode.cache.lucene.test.TestObject) value).getField2());
        newValue.setField2("***");
        return newValue;
      } else if (value instanceof PdxInstance) {
        WritablePdxInstance pdx = ((PdxInstance) value).createWriter();
        pdx.setField("field2", "***");
        return pdx;
      }
      return value;
    }
  }
}
