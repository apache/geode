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
package org.apache.geode.tools.pulse.internal.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Principal;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * Tests the {@code errorOnRegion} message the region-detail services produce when the requested
 * region path does not resolve.
 *
 * <p>
 * Paths containing characters such as {@code <} or {@code &} are encoded so the message displays
 * as written. Lookup is unaffected and uses the path as supplied.
 */
public class RegionErrorMessageEncodingTest {

  private static final String PATH_WITH_SPECIAL_CHARACTERS = "/orders<2026>&archive";
  private static final String ENCODED_MESSAGE =
      "Region [/orders&lt;2026&gt;&amp;archive] is not available";
  private static final String ORDINARY_PATH = "/mock-region";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private Repository repository;
  private Cluster cluster;
  private HttpServletRequest request;

  @Before
  public void setUp() {
    repository = mock(Repository.class);
    cluster = mock(Cluster.class);
    request = mock(HttpServletRequest.class);
    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn("admin");
    when(request.getUserPrincipal()).thenReturn(principal);
    when(repository.getCluster()).thenReturn(cluster);
    when(cluster.getServerName()).thenReturn("mock-cluster");
    // No region resolves, so every call below takes the error branch.
    when(cluster.getClusterRegion(anyString())).thenReturn(null);
  }

  @Test
  public void clusterSelectedRegionEncodesSpecialCharactersInErrorMessage() throws Exception {
    assertThat(selectedRegionError(PATH_WITH_SPECIAL_CHARACTERS)).isEqualTo(ENCODED_MESSAGE);
  }

  @Test
  public void clusterSelectedRegionsMemberEncodesSpecialCharactersInErrorMessage()
      throws Exception {
    assertThat(selectedRegionsMemberError(PATH_WITH_SPECIAL_CHARACTERS)).isEqualTo(ENCODED_MESSAGE);
  }

  @Test
  public void clusterSelectedRegionLeavesOrdinaryPathUnchanged() throws Exception {
    assertThat(selectedRegionError(ORDINARY_PATH))
        .isEqualTo("Region [" + ORDINARY_PATH + "] is not available");
  }

  @Test
  public void clusterSelectedRegionsMemberLeavesOrdinaryPathUnchanged() throws Exception {
    assertThat(selectedRegionsMemberError(ORDINARY_PATH))
        .isEqualTo("Region [" + ORDINARY_PATH + "] is not available");
  }

  @Test
  public void clusterSelectedRegionLooksTheRegionUpByTheSuppliedPath() throws Exception {
    selectedRegionError(PATH_WITH_SPECIAL_CHARACTERS);

    verify(cluster).getClusterRegion(PATH_WITH_SPECIAL_CHARACTERS);
  }

  @Test
  public void clusterSelectedRegionsMemberLooksTheRegionUpByTheSuppliedPath() throws Exception {
    selectedRegionsMemberError(PATH_WITH_SPECIAL_CHARACTERS);

    verify(cluster).getClusterRegion(PATH_WITH_SPECIAL_CHARACTERS);
  }

  private String selectedRegionError(String regionFullPath) throws Exception {
    when(request.getParameter("pulseData"))
        .thenReturn(pulseData("ClusterSelectedRegion", regionFullPath));

    ObjectNode json = new ClusterSelectedRegionService(repository).execute(request);

    return json.get("selectedRegion").get("errorOnRegion").asText();
  }

  private String selectedRegionsMemberError(String regionFullPath) throws Exception {
    when(request.getParameter("pulseData"))
        .thenReturn(pulseData("ClusterSelectedRegionsMember", regionFullPath));

    ObjectNode json = new ClusterSelectedRegionsMemberService(repository).execute(request);

    return json.get("selectedRegionsMembers").get("errorOnRegion").asText();
  }

  /** Builds the {@code pulseData} body the Pulse frontend posts for the region-detail page. */
  private static String pulseData(String service, String regionFullPath) {
    ObjectNode parameters = MAPPER.createObjectNode();
    parameters.put("regionFullPath", regionFullPath);
    ObjectNode root = MAPPER.createObjectNode();
    root.set(service, parameters);
    return root.toString();
  }
}
