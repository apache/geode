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

package org.apache.geode.management.internal.cli.commands;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.web.domain.Link;
import org.apache.geode.management.internal.web.domain.LinkIndex;
import org.apache.geode.management.internal.web.http.HttpMethod;
import org.apache.geode.management.internal.web.shell.RestHttpOperationInvoker;
import org.apache.geode.test.dunit.rules.LocalLocatorStarterRule;
import org.apache.geode.test.dunit.rules.LocatorStarterBuilder;
import org.apache.geode.test.dunit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URI;
import java.util.HashMap;
import java.util.Set;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;

@Category(IntegrationTest.class)
public class QueryNamesOverHttpDUnitTest {
  @Rule
  public LocalLocatorStarterRule locatorRule = new LocatorStarterBuilder().buildInThisVM();

  @Test
  public void testQueryNameOverHttp() throws Exception {

    LinkIndex links = new LinkIndex();
    links.add(new Link("mbean-query",
        new URI("http://localhost:" + locatorRule.getHttpPort() + "/gemfire/v1/mbean/query"),
        HttpMethod.POST));
    RestHttpOperationInvoker invoker =
        new RestHttpOperationInvoker(links, mock(Gfsh.class), new HashMap<>());

    ObjectName objectName = ObjectName.getInstance("GemFire:type=Member,*");
    QueryExp query = Query.eq(Query.attr("Name"), Query.value("mock"));

    Set<ObjectName> names = invoker.queryNames(objectName, query);
    assertTrue(names.isEmpty());
  }
}
