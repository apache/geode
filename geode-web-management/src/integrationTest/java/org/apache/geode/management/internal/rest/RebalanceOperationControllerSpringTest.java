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

package org.apache.geode.management.internal.rest;

import static org.apache.geode.management.configuration.Links.URI_VERSION;
import static org.apache.geode.management.operation.RebalanceOperation.REBALANCE_ENDPOINT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.internal.api.LocatorClusterManagementService;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = MockLocatorContextLoader.class)
@WebAppConfiguration
public class RebalanceOperationControllerSpringTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  private LocatorWebContext context;
  private MockLocatorContextLoader mockLocator;
  private LocatorClusterManagementService cms;

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);
    mockLocator = (MockLocatorContextLoader) context.getLocator();
    cms = (LocatorClusterManagementService) mockLocator.getClusterManagementService();
  }

  @After
  public void clearMocks() {
    Mockito.clearInvocations(cms);
  }

  @Test
  public void getMappingRecognizesRebalanceIdWithDot() throws Exception {
    String rebalanceIdWithDot = "rebalance.id";
    String requestPath = URI_VERSION + REBALANCE_ENDPOINT + "/" + rebalanceIdWithDot;

    when(cms.get(any(), eq(rebalanceIdWithDot)))
        .thenReturn(new ClusterManagementOperationResult<>());

    context.perform(get(requestPath))
        .andExpect(status().is2xxSuccessful());

    verify(cms).get(any(), eq(rebalanceIdWithDot));
  }
}
