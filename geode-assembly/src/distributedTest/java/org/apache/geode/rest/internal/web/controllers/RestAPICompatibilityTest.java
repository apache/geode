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
 *
 */
package org.apache.geode.rest.internal.web.controllers;

import static org.apache.geode.rest.internal.web.controllers.RestUtilities.executeAndValidateGETRESTCalls;
import static org.apache.geode.rest.internal.web.controllers.RestUtilities.executeAndValidatePOSTRESTCalls;

import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.MemberStarterRule;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;
import org.apache.geode.util.internal.GeodeJsonMapper;

@Category({BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
public class RestAPICompatibilityTest {
  private final String oldVersion;
  private static ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    result.removeIf(s -> TestVersion.compare(s, "1.11.0") < 0);
    return result;
  }

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  public RestAPICompatibilityTest(String oldVersion) throws JsonProcessingException {
    this.oldVersion = oldVersion;

  }


  @Test
  public void restCommandExecutedOnLatestLocatorShouldBeBackwardsCompatible() throws Exception {
    // Initialize all cluster members with old versions
    MemberVM locator1 =
        cluster.startLocatorVM(0, 0, oldVersion, MemberStarterRule::withHttpService);
    int locatorPort1 = locator1.getPort();
    MemberVM locator2 =
        cluster.startLocatorVM(1, 0, oldVersion,
            x -> x.withConnectionToLocator(locatorPort1).withHttpService());
    int locatorPort2 = locator2.getPort();
    cluster
        .startServerVM(2, oldVersion, s -> s.withRegion(RegionShortcut.PARTITION, "region")
            .withConnectionToLocator(locatorPort1, locatorPort2));
    cluster
        .startServerVM(3, oldVersion, s -> s.withRegion(RegionShortcut.PARTITION, "region")
            .withConnectionToLocator(locatorPort1, locatorPort2));

    // Roll locators to the current version
    cluster.stop(0);
    // gradle sets a property telling us where the build is located
    final String buildDir = System.getProperty("geode.build.dir", System.getProperty("user.dir"));
    locator1 = cluster.startLocatorVM(0, l -> l.withHttpService().withPort(locatorPort1)
        .withConnectionToLocator(locatorPort2)
        .withSystemProperty("geode.build.dir", buildDir));
    cluster.stop(1);

    cluster.startLocatorVM(1,
        x -> x.withConnectionToLocator(locatorPort1).withHttpService().withPort(locatorPort2)
            .withConnectionToLocator(locatorPort1)
            .withSystemProperty("geode.build.dir", buildDir));

    gfsh.connectAndVerify(locator1);
    gfsh.execute("list members");
    // Execute REST api calls to from the new locators to the old servers to ensure that backwards
    // compatibility is maintained

    executeAndValidatePOSTRESTCalls(locator1.getHttpPort(), oldVersion);
    executeAndValidateGETRESTCalls(locator1.getHttpPort());

  }
}
