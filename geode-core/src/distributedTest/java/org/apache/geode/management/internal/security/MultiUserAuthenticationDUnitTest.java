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

package org.apache.geode.management.internal.security;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ClientCacheRule;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({SecurityTest.class})
public class MultiUserAuthenticationDUnitTest {

  private static int SESSION_COUNT = 2;
  private static int KEY_COUNT = 2;

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public ClientCacheRule client = new ClientCacheRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator;

  @BeforeClass
  public static void beforeClass() throws Exception {
    IgnoredException.addIgnoredException("org.apache.geode.security.AuthenticationFailedException");

    locator = lsRule.startLocatorVM(0, l -> l.withSecurityManager(SimpleSecurityManager.class));

    Properties serverProps = new Properties();
    serverProps.setProperty("security-username", "cluster");
    serverProps.setProperty("security-password", "cluster");
    serverProps.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER, "*");
    lsRule.startServerVM(1, serverProps, locator.getPort());
    lsRule.startServerVM(2, serverProps, locator.getPort());

    // create region and put in some values
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=region --type=PARTITION").statusIsSuccess();
  }


  private static class TestObject implements DataSerializable {

    private String id;

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(id, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException {
      id = DataSerializer.readString(in);
    }
  }

  private static class TestObjectDataSerializer extends DataSerializer implements Serializable {

    @Override
    public Class<?>[] getSupportedClasses() {
      return new Class<?>[] {TestObject.class};
    }

    @Override
    public boolean toData(Object o, DataOutput out) {
      return o instanceof TestObject;
    }

    @Override
    public Object fromData(DataInput in) {
      return new TestObject();
    }

    @Override
    public int getId() {
      return 99;
    }
  }


  @Test
  public void validatePropogationOfDataSerializersInMultUserAuthMode() throws Exception {
    int locatorPort = locator.getPort();
    for (int i = 0; i < SESSION_COUNT; i++) {
      ClientCache cache = client.withCacheSetup(f -> f.setPoolSubscriptionEnabled(true)
              .setPoolMultiuserAuthentication(true)
              .addPoolLocator("localhost", locatorPort))
          .createCache();

      RegionService regionService1 = client.createAuthenticatedView("data", "data");
      RegionService regionService2 = client.createAuthenticatedView("cluster", "cluster");

      cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");

      Region region = regionService1.getRegion(SEPARATOR + "region");

      DataSerializer.register(TestObjectDataSerializer.class, regionService1);

      for (int j = 0; j < KEY_COUNT; j++) {
        String key = i + "" + j;
        region.put(key, new TestObject());
      }

      regionService1.close();
      regionService2.close();
      cache.close();
    }
  }

  @Test
  public void multiAuthenticatedView() throws Exception {
    int locatorPort = locator.getPort();
    for (int i = 0; i < SESSION_COUNT; i++) {
      ClientCache cache = client.withCacheSetup(f -> f.setPoolSubscriptionEnabled(true)
          .setPoolMultiuserAuthentication(true)
          .addPoolLocator("localhost", locatorPort))
          .createCache();

      RegionService regionService1 = client.createAuthenticatedView("data", "data");
      RegionService regionService2 = client.createAuthenticatedView("cluster", "cluster");

      cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("region");

      Region region = regionService1.getRegion(SEPARATOR + "region");
      Region region2 = regionService2.getRegion(SEPARATOR + "region");
      for (int j = 0; j < KEY_COUNT; j++) {
        String value = i + "" + j;
        region.put(value, value);
        assertThatThrownBy(() -> region2.put(value, value))
            .isInstanceOf(ServerOperationException.class);
      }
      regionService1.close();
      regionService2.close();
      cache.close();
    }
  }

  @Test
  public void multiUserCQ() throws Exception {
    int locatorPort = locator.getPort();
    client.withCacheSetup(f -> f.setPoolSubscriptionEnabled(true)
        .setPoolMultiuserAuthentication(true)
        .addPoolLocator("localhost", locatorPort))
        .createCache();

    // both are able to read data
    RegionService regionService1 = client.createAuthenticatedView("data", "data");
    RegionService regionService2 = client.createAuthenticatedView("dataRead", "dataRead");

    EventsCqListner listener1 = createAndExecuteCQ(regionService1.getQueryService(), "cq1",
        "select * from /region r where r.length<=2");
    EventsCqListner listener2 = createAndExecuteCQ(regionService2.getQueryService(), "cq2",
        "select * from /region r where r.length>=2");

    // put 3 data in the region
    gfsh.executeAndAssertThat("put --region=region --key=1 --value=1");
    gfsh.executeAndAssertThat("put --region=region --key=11 --value=11");
    gfsh.executeAndAssertThat("put --region=region --key=111 --value=111");

    await().untilAsserted(
        () -> assertThat(listener1.getKeys())
            .containsExactly("1", "11"));

    await().untilAsserted(
        () -> assertThat(listener2.getKeys())
            .containsExactly("11", "111"));

  }

  private static EventsCqListner createAndExecuteCQ(QueryService queryService, String cqName,
      String query)
      throws CqExistsException, CqException, RegionNotFoundException {
    CqAttributesFactory cqaf = new CqAttributesFactory();
    EventsCqListner listener = new EventsCqListner();
    cqaf.addCqListener(listener);

    CqQuery cq = queryService.newCq(cqName, query, cqaf.create());
    cq.execute();
    return listener;
  }

  private static class EventsCqListner implements CqListener {
    private List<String> keys = new ArrayList<>();

    @Override
    public void onEvent(CqEvent aCqEvent) {
      keys.add(aCqEvent.getKey().toString());
    }

    @Override
    public void onError(CqEvent aCqEvent) {}

    public List<String> getKeys() {
      return keys;
    }
  }
}
