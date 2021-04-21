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
package org.apache.geode.rest.internal.web.controllers;

import static org.apache.geode.rest.internal.web.controllers.RestUtilities.executeAndValidateGETRESTCalls;
import static org.apache.geode.rest.internal.web.controllers.RestUtilities.executeAndValidatePOSTRESTCalls;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.version.VersionManager;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class RestAPIOnClusterWithNoRegionDUnitTest {
  private static ObjectMapper mapper = GeodeJsonMapper.getMapper();
  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Test
  public void whenRESTCmdToRebalanceIsExecutedOnClusterWithNoRegionThenItMustSucceed()
      throws Exception {
    MemberVM locator1 =
        cluster.startLocatorVM(0, l -> l.withHttpService().withPort(0));
    int locatorPort1 = locator1.getPort();
    MemberVM locator2 =
        cluster.startLocatorVM(1,
            x -> x.withConnectionToLocator(locatorPort1).withHttpService());
    int locatorPort2 = locator2.getPort();
    cluster
        .startServerVM(2, s -> s.withConnectionToLocator(locatorPort1, locatorPort2));
    cluster
        .startServerVM(3, s -> s.withConnectionToLocator(locatorPort1, locatorPort2));

    executeAndValidateGETRESTCalls(locator1.getHttpPort());
    executeAndValidatePOSTRESTCalls(locator1.getHttpPort(), VersionManager.CURRENT_VERSION);
  }


}
