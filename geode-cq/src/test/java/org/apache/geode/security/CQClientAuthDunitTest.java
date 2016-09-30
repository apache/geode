/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.security.templates.SamplePostProcessor;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class })
public class CQClientAuthDunitTest extends AbstractSecureServerDUnitTest {

  public CQClientAuthDunitTest(){
    this.postProcessor = SamplePostProcessor.class;
  }

  @Test
  public void testPostProcess(){
    String query = "select * from /AuthRegion";
    client1.invoke(()-> {
      Properties props = new Properties();
      props.setProperty(LOCATORS, "");
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName() + ".create");
      ClientCacheFactory factory = new ClientCacheFactory(props);

      factory.addPoolServer("localhost", this.serverPort);
      factory.setPoolThreadLocalConnections(false);
      factory.setPoolMinConnections(5);
      factory.setPoolSubscriptionEnabled(true);
      factory.setPoolMultiuserAuthentication(true);


      ClientCache clientCache = factory.create();
      Region region = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      Pool pool = PoolManager.find(region);

      Properties userProps = new Properties();
      userProps.setProperty("security-username", "super-user");
      userProps.setProperty("security-password", "1234567");
      ProxyCache cache = (ProxyCache)clientCache.createAuthenticatedView(userProps, pool.getName());

      QueryService qs = cache.getQueryService();

      CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();

      CqAttributes cqa = cqAttributesFactory.create();

      // Create the CqQuery
      CqQuery cq = qs.newCq("CQ1", query, cqa, true);
      cq.execute();
    });
  }

}
