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

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.GeodeDevRestClient;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class HttpServiceIntegrationTest {
  @ClassRule
  public static RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @Rule
  public ServerStarterRule server = new ServerStarterRule();

  private GeodeDevRestClient client;

  @Test
  public void withDevRestAndJmxManager() throws Exception {
    server.withRestService().withJMXManager().startServer();
    client =
        new GeodeDevRestClient("/geode/v1", "localhost", server.getHttpPort(), false);
    client.doGetAndAssert("/servers").hasStatusCode(200);

    client =
        new GeodeDevRestClient("/geode-mgmt/v1", "localhost", server.getHttpPort(), false);
    client.doGetAndAssert("/version").hasStatusCode(200);

    client =
        new GeodeDevRestClient("/pulse", "localhost", server.getHttpPort(), false);
    client.doGetAndAssert("/index.html").hasStatusCode(200);

  }

  @Test
  public void withDevRestOnly() throws Exception {
    server.withRestService().startServer();
    client =
        new GeodeDevRestClient("/geode/v1", "localhost", server.getHttpPort(), false);
    client.doGetAndAssert("/servers").hasStatusCode(200);

    client =
        new GeodeDevRestClient("/geode-mgmt/v1", "localhost", server.getHttpPort(), false);
    client.doGetAndAssert("/version").hasStatusCode(404);

    client =
        new GeodeDevRestClient("/pulse", "localhost", server.getHttpPort(), false);
    client.doGetAndAssert("/index.html").hasStatusCode(404);
  }

  @Test
  public void withAdminRestOnly() throws Exception {
    server.withJMXManager().withHttpService().startServer();
    client =
        new GeodeDevRestClient("/geode/v1", "localhost", server.getHttpPort(), false);
    client.doGetAndAssert("/servers").hasStatusCode(404);

    client =
        new GeodeDevRestClient("/geode-mgmt/v1", "localhost", server.getHttpPort(), false);
    client.doGetAndAssert("/version").hasStatusCode(200);

    client =
        new GeodeDevRestClient("/pulse", "localhost", server.getHttpPort(), false);
    client.doGetAndAssert("/index.html").hasStatusCode(200);
  }

}
