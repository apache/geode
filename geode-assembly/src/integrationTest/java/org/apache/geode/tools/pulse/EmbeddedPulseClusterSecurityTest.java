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

package org.apache.geode.tools.pulse;

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.junit.categories.PulseTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.EmbeddedPulseRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.tools.pulse.internal.data.Cluster;

@Category({SecurityTest.class, PulseTest.class})
public class EmbeddedPulseClusterSecurityTest {
  private static final String QUERY = "select * from /regionA a order by a";

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withSecurityManager(SimpleSecurityManager.class)
      .withJMXManager()
      .withHttpService()
      .withRegion(REPLICATE, "regionA");

  @Rule
  public EmbeddedPulseRule pulse = new EmbeddedPulseRule();

  @Before
  public void useServerJmxPort() {
    pulse.useJmxPort(server.getJmxPort());
  }

  @Test
  public void acceptsAuthorizedUser() {
    // The test security manager authorizes "data" to read data
    String authorizedUser = "data";

    Cluster cluster = pulse.getRepository()
        .getClusterWithUserNameAndPassword(authorizedUser, authorizedUser);
    ObjectNode queryResult = cluster.executeQuery(QUERY, null, 0);

    assertThat(queryResult.toString())
        .contains("No Data Found");
  }

  @Test
  public void rejectsUnauthorizedUser() {
    // The test security manager does not authorize "cluster" to read data
    String unauthorizedUser = "cluster";

    Cluster cluster = pulse.getRepository()
        .getClusterWithUserNameAndPassword(unauthorizedUser, unauthorizedUser);
    ObjectNode queryResult = cluster.executeQuery(QUERY, null, 0);

    assertThat(queryResult.toString())
        .contains("not authorized for DATA:READ");
  }
}
