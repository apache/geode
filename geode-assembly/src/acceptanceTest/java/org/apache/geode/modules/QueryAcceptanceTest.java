/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.modules;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.CqResults;
import org.apache.geode.cache.query.CqStatusListener;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class QueryAcceptanceTest extends AbstractDockerizedAcceptanceTest {
  public QueryAcceptanceTest(String launchCommand) throws IOException, InterruptedException {
    launch(launchCommand);
  }

  @Before
  public void setup() {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "create region --name=TestRegion --type=PARTITION",
        "put --region=/TestRegion --key-class=java.lang.Long --key=1 --value-class=java.lang.String --value=hello",
        "put --region=/TestRegion --key-class=java.lang.Long --key=2 --value-class=java.lang.String --value=hola")
        .execute(gfshRule);
  }

  @After
  public void teardown() {
    GfshScript
        .of(getLocatorGFSHConnectionString(), "destroy region --name=TestRegion --if-exists")
        .execute(gfshRule);
  }

  @Test
  public void testQueryUsingJavaApi()
      throws NameResolutionException, TypeMismatchException, QueryInvocationTargetException,
      FunctionDomainException {

    ClientCache clientCache =
        new ClientCacheFactory().set("mcast-port", "0").addPoolServer(host, serverPort)
            .create();
    Region<Object, Object> partitionedRegion =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("TestRegion");

    SelectResults<Object> results = partitionedRegion.query("SELECT * FROM /TestRegion");

    assertThat(results.size()).isEqualTo(2);
    assertThat(results).contains("hello", "hola");

    clientCache.close();
  }

  @Test
  public void testQueryUsingGfsh() {
    assertThat(GfshScript.of(getLocatorGFSHConnectionString(),
        "query --query=\"SELECT * FROM /TestRegion\"")
        .execute(gfshRule).getOutputText()).contains("hello", "hola");
  }

  @Test
  public void testContinuousQuery() throws CqException, CqExistsException, RegionNotFoundException {
    ClientCache clientCache =
        new ClientCacheFactory().set("mcast-port", "0").addPoolServer(host, serverPort)
            .setPoolSubscriptionEnabled(true).create();
    Region<Object, Object> partitionedRegion =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("TestRegion");

    CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();
    EventListener cqListener = new EventListener();
    cqAttributesFactory.addCqListener(cqListener);
    CqAttributes cqAttributes = cqAttributesFactory.create();
    CqQuery cqQuery =
        clientCache.getQueryService().newCq("testCQ", "SELECT * FROM /TestRegion", cqAttributes);
    CqResults<Object> initialResults = cqQuery.executeWithInitialResults();

    assertThat(initialResults.size()).isEqualTo(2);
    GeodeAwaitility.await().untilAsserted(() -> assertThat(cqListener.isConnected).isTrue());

    partitionedRegion.put(3L, "bonjour");
    GeodeAwaitility.await().until(() -> cqListener.receivedEvents.size() == 1);

    assertThat(cqListener.receivedEvents.size()).isEqualTo(1);
    assertThat(cqListener.receivedEvents.get(0).getNewValue()).isEqualTo("bonjour");
    assertThat(cqListener.receivedErrors.size()).isEqualTo(0);

    cqQuery.close();
    partitionedRegion.destroy(3L);
    clientCache.close();
  }

  public static class EventListener implements CqStatusListener {
    public List<CqEvent> receivedEvents = new LinkedList<>();
    public List<CqEvent> receivedErrors = new LinkedList<>();
    public boolean isConnected = false;

    @Override
    public void onCqDisconnected() {
      isConnected = false;
    }

    @Override
    public void onCqConnected() {
      isConnected = true;
    }

    @Override
    public void onEvent(CqEvent aCqEvent) {
      receivedEvents.add(aCqEvent);
    }

    @Override
    public void onError(CqEvent aCqEvent) {
      receivedErrors.add(aCqEvent);
    }
  }
}
