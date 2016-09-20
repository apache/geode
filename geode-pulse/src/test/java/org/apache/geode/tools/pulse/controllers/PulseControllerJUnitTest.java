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
package org.apache.geode.tools.pulse.controllers;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.io.File;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import javax.servlet.ServletContextListener;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.geode.tools.pulse.internal.PulseAppListener;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.tools.pulse.internal.controllers.PulseController;
import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.PulseConfig;
import org.apache.geode.tools.pulse.internal.data.Repository;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
@PrepareForTest(Repository.class)
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration("classpath*:mvc-dispatcher-servlet.xml")
@PowerMockIgnore("*.IntegrationTest")
public class PulseControllerJUnitTest {

  private static final String PRINCIPAL_USER = "test-user";

  private static final String MEMBER_ID = "member1";
  private static final String MEMBER_NAME = "localhost-server";
  private static final String CLUSTER_NAME = "mock-cluster";
  private static final String REGION_NAME = "mock-region";
  private static final String REGION_PATH = "/" + REGION_NAME;
  private static final String REGION_TYPE = "PARTITION";
  private static final String AEQ_LISTENER = "async-event-listener";
  private static final String CLIENT_NAME = "client-1";
  private static final String PHYSICAL_HOST_NAME = "physical-host-1";
  private static final String GEMFIRE_VERSION = "1.0.0";

  private static final Principal principal;

  static {
    principal = () -> PRINCIPAL_USER;
  }

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Autowired
  private WebApplicationContext wac;

  private MockMvc mockMvc;

  private Cluster cluster;

  private final ObjectMapper mapper = new ObjectMapper();

  @Before
  public void setup() throws Exception {
    this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();


    cluster = Mockito.spy(Cluster.class);

    Cluster.Region region = new Cluster.Region();
    region.setName(REGION_NAME);
    region.setFullPath(REGION_PATH);
    region.setRegionType(REGION_TYPE);
    region.setMemberCount(1);
    region.setMemberName(new ArrayList<String>() {{
      add(MEMBER_NAME);
    }});
    region.setPutsRate(12.31D);
    region.setGetsRate(27.99D);
    Cluster.RegionOnMember regionOnMember = new Cluster.RegionOnMember();
    regionOnMember.setRegionFullPath(REGION_PATH);
    regionOnMember.setMemberName(MEMBER_NAME);
    region.setRegionOnMembers(new ArrayList<Cluster.RegionOnMember>() {{
      add(regionOnMember);
    }});
    cluster.addClusterRegion(REGION_PATH, region);

    Cluster.Member member = new Cluster.Member();
    member.setId(MEMBER_ID);
    member.setName(MEMBER_NAME);
    member.setUptime(1L);
    member.setHost(PHYSICAL_HOST_NAME);
    member.setGemfireVersion(GEMFIRE_VERSION);
    member.setCpuUsage(55.77123D);

    member.setMemberRegions(new HashMap<String, Cluster.Region>() {{
      put(REGION_NAME, region);
    }});

    Cluster.AsyncEventQueue aeq = new Cluster.AsyncEventQueue();
    aeq.setAsyncEventListener(AEQ_LISTENER);
    member.setAsyncEventQueueList(new ArrayList() {{
      add(aeq);
    }});

    Cluster.Client client = new Cluster.Client();
    client.setId("100");
    client.setName(CLIENT_NAME);
    client.setUptime(1L);
    member.setMemberClientsHMap(new HashMap<String, Cluster.Client>() {{
      put(CLIENT_NAME, client);
    }});

    cluster.setMembersHMap(new HashMap() {{
      put(MEMBER_NAME, member);
    }});
    cluster.setPhysicalToMember(new HashMap() {{
      put(PHYSICAL_HOST_NAME, new ArrayList() {{
        add(member);
      }});
    }});
    cluster.setServerName(CLUSTER_NAME);
    cluster.setMemoryUsageTrend(new CircularFifoBuffer() {{
      add(1);
      add(2);
      add(3);
    }});
    cluster.setWritePerSecTrend(new CircularFifoBuffer() {{
      add(1.29);
      add(2.3);
      add(3.0);
    }});
    cluster.setThroughoutReadsTrend(new CircularFifoBuffer() {{
      add(1);
      add(2);
      add(3);
    }});
    cluster.setThroughoutWritesTrend(new CircularFifoBuffer() {{
      add(4);
      add(5);
      add(6);
    }});

    Repository repo = Mockito.spy(Repository.class);

    // Set up a partial mock for some static methods
    spy(Repository.class);
    when(Repository.class, "get").thenReturn(repo);
    doReturn(cluster).when(repo).getCluster();

    PulseConfig config = new PulseConfig();
    File tempQueryLog = tempFolder.newFile("query_history.log");
    config.setQueryHistoryFileName(tempQueryLog.toString());
    doReturn(config).when(repo).getPulseConfig();

    PulseController.pulseVersion.setPulseVersion("not empty");
    PulseController.pulseVersion.setPulseBuildId("not empty");
    PulseController.pulseVersion.setPulseBuildDate("not empty");
    PulseController.pulseVersion.setPulseSourceDate("not empty");
    PulseController.pulseVersion.setPulseSourceRevision("not empty");
    PulseController.pulseVersion.setPulseSourceRepository("not empty");
  }

  @Test
  public void pulseUpdateForClusterDetails() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"ClusterDetails\":\"{}\"}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ClusterDetails.userName").value(PRINCIPAL_USER))
        .andExpect(jsonPath("$.ClusterDetails.totalHeap").value(0D))
        .andExpect(jsonPath("$.ClusterDetails.clusterName").value(CLUSTER_NAME))
    ;
  }

  @Test
  public void pulseUpdateForClusterDiskThroughput() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"ClusterDiskThroughput\":\"{}\"}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ClusterDiskThroughput.currentThroughputWrites").value(0D))
        .andExpect(jsonPath("$.ClusterDiskThroughput.throughputReads", contains(1, 2, 3)))
        .andExpect(jsonPath("$.ClusterDiskThroughput.currentThroughputReads").value(0D))
        .andExpect(jsonPath("$.ClusterDiskThroughput.throughputWrites", contains(4, 5, 6)))
    ;
  }

  @Test
  public void pulseUpdateForClusterGCPauses() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"ClusterJVMPauses\":\"{}\"}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ClusterJVMPauses.currentGCPauses").value(0))
        .andExpect(jsonPath("$.ClusterJVMPauses.gCPausesTrend").isEmpty())
    ;
  }

  @Test
  public void pulseUpdateForClusterKeyStatistics() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"ClusterKeyStatistics\":\"{}\"}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ClusterKeyStatistics.readPerSecTrend").isEmpty())
        .andExpect(jsonPath("$.ClusterKeyStatistics.queriesPerSecTrend").isEmpty())
        .andExpect(jsonPath("$.ClusterKeyStatistics.writePerSecTrend", contains(1.29, 2.3, 3.0)))
    ;
  }

  @Test
  public void pulseUpdateForClusterMember() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"ClusterMembers\":\"{}\"}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ClusterMembers.members[0].serverGroups[0]").value("Default"))
        .andExpect(jsonPath("$.ClusterMembers.members[0].cpuUsage").value(55.77D))
        .andExpect(jsonPath("$.ClusterMembers.members[0].clients").value(1))
        .andExpect(jsonPath("$.ClusterMembers.members[0].heapUsage").value(0))
        .andExpect(jsonPath("$.ClusterMembers.members[0].name").value(MEMBER_NAME))
        .andExpect(jsonPath("$.ClusterMembers.members[0].currentHeapUsage").value(0))
        .andExpect(jsonPath("$.ClusterMembers.members[0].isManager").value(false))
        .andExpect(jsonPath("$.ClusterMembers.members[0].threads").value(0))
        .andExpect(jsonPath("$.ClusterMembers.members[0].memberId").value(MEMBER_ID))
        .andExpect(jsonPath("$.ClusterMembers.members[0].redundancyZones[0]").value("Default"))
    ;
  }

  @Test
  public void pulseUpdateForClusterMembersRGraph() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"ClusterMembersRGraph\":\"{}\"}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ClusterMembersRGraph.memberCount").value(0))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.data").isEmpty())
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.name").value(0))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.id").value(0))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].id").value(PHYSICAL_HOST_NAME))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].name").value(PHYSICAL_HOST_NAME))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].data.loadAvg").value(0D))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].data.sockets").value(0))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].data.threads").value(0))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].data.cpuUsage").value(0D))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].data.memoryUsage").value(0))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].data.hostStatus").value("Normal"))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].data.$type").value("hostNormalNode"))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].id").value(MEMBER_ID))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].name").value(MEMBER_NAME))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].data.gemfireVersion").value(GEMFIRE_VERSION))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].data.memoryUsage").value(0))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].data.cpuUsage").value(55.77D))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].data.regions").value(1))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].data.host").value(PHYSICAL_HOST_NAME))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].data.port").value("-"))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].data.clients").value(1))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].data.gcPauses").value(0))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].data.numThreads").value(0))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].data.nodeType").value("memberNormalNode"))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].data.$type").value("memberNormalNode"))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].data.gatewaySender").value(0))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].data.gatewayReceiver").value(0))
        .andExpect(jsonPath("$.ClusterMembersRGraph.clustor.children[0].children[0].children").isEmpty())
    ;
  }

  @Test
  public void pulseUpdateForClusterMemoryUsage() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"ClusterMemoryUsage\":\"{}\"}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ClusterMemoryUsage.currentMemoryUsage").value(0))
        .andExpect(jsonPath("$.ClusterMemoryUsage.memoryUsageTrend", containsInAnyOrder(1, 2, 3)))
    ;
  }

  @Test
  public void pulseUpdateForClusterRegion() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"ClusterRegion\":\"{}\"}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ClusterRegion.clusterName").value(CLUSTER_NAME))
        .andExpect(jsonPath("$.ClusterRegion.userName").value(PRINCIPAL_USER))
        .andExpect(jsonPath("$.ClusterRegion.region[0].regionPath").value(REGION_PATH))
        .andExpect(jsonPath("$.ClusterRegion.region[0].diskReadsTrend").isEmpty())
        .andExpect(jsonPath("$.ClusterRegion.region[0].memoryUsage").value("0.0000"))
        .andExpect(jsonPath("$.ClusterRegion.region[0].getsRate").value(27.99D))
        .andExpect(jsonPath("$.ClusterRegion.region[0].wanEnabled").value(false))
        .andExpect(jsonPath("$.ClusterRegion.region[0].memberCount").value(1))
        .andExpect(jsonPath("$.ClusterRegion.region[0].memberNames[0].name").value(MEMBER_NAME))
        .andExpect(jsonPath("$.ClusterRegion.region[0].memberNames[0].id").value(MEMBER_ID))
        .andExpect(jsonPath("$.ClusterRegion.region[0].emptyNodes").value(0))
        .andExpect(jsonPath("$.ClusterRegion.region[0].type").value(REGION_TYPE))
        .andExpect(jsonPath("$.ClusterRegion.region[0].isEnableOffHeapMemory").value("OFF"))
        .andExpect(jsonPath("$.ClusterRegion.region[0].putsRate").value(12.31D))
        .andExpect(jsonPath("$.ClusterRegion.region[0].totalMemory").value(0))
        .andExpect(jsonPath("$.ClusterRegion.region[0].entryCount").value(0))
        .andExpect(jsonPath("$.ClusterRegion.region[0].compressionCodec").value("NA"))
        .andExpect(jsonPath("$.ClusterRegion.region[0].name").value(REGION_NAME))
        .andExpect(jsonPath("$.ClusterRegion.region[0].systemRegionEntryCount").value(0))
        .andExpect(jsonPath("$.ClusterRegion.region[0].persistence").value("OFF"))
        .andExpect(jsonPath("$.ClusterRegion.region[0].memoryReadsTrend").isEmpty())
        .andExpect(jsonPath("$.ClusterRegion.region[0].diskWritesTrend").isEmpty())
        .andExpect(jsonPath("$.ClusterRegion.region[0].memoryWritesTrend").isEmpty())
        .andExpect(jsonPath("$.ClusterRegion.region[0].dataUsage").value(0))
        .andExpect(jsonPath("$.ClusterRegion.region[0].entrySize").value("0.0000"))
    ;
  }

  @Test
  public void pulseUpdateForClusterRegions() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"ClusterRegions\":\"{}\"}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ClusterRegions.regions[0].regionPath").value(REGION_PATH))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].diskReadsTrend").isEmpty())
        .andExpect(jsonPath("$.ClusterRegions.regions[0].memoryUsage").value("0.0000"))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].getsRate").value(27.99D))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].wanEnabled").value(false))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].memberCount").value(1))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].memberNames[0].name").value(MEMBER_NAME))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].memberNames[0].id").value(MEMBER_ID))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].emptyNodes").value(0))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].type").value(REGION_TYPE))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].isEnableOffHeapMemory").value("OFF"))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].putsRate").value(12.31D))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].totalMemory").value(0))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].entryCount").value(0))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].compressionCodec").value("NA"))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].name").value(REGION_NAME))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].systemRegionEntryCount").value(0))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].persistence").value("OFF"))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].memoryReadsTrend").isEmpty())
        .andExpect(jsonPath("$.ClusterRegions.regions[0].diskWritesTrend").isEmpty())
        .andExpect(jsonPath("$.ClusterRegions.regions[0].memoryWritesTrend").isEmpty())
        .andExpect(jsonPath("$.ClusterRegions.regions[0].dataUsage").value(0))
        .andExpect(jsonPath("$.ClusterRegions.regions[0].entrySize").value("0.0000"))
    ;
  }

  @Test
  public void pulseUpdateForClusterSelectedRegion() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"ClusterSelectedRegion\":{\"regionFullPath\":\"" + REGION_PATH + "\"}}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.lruEvictionRate").value(0D))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.getsRate").value(27.99D))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.emptyNodes").value(0))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.type").value(REGION_TYPE))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.isEnableOffHeapMemory").value("OFF"))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.path").value(REGION_PATH))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.members[0].cpuUsage").value(55.77D))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.members[0].clients").value(1))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.members[0].heapUsage").value(0))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.members[0].name").value(MEMBER_NAME))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.members[0].currentHeapUsage").value(0))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.members[0].isManager").value(false))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.members[0].threads").value(0))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.members[0].memberId").value(MEMBER_ID))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.members[0].uptime").value("0 Hours 0 Mins 1 Secs"))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.memoryReadsTrend").isEmpty())
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.diskWritesTrend").isEmpty())
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.dataUsage").value(0))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.regionPath").value(REGION_PATH))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.diskReadsTrend").isEmpty())
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.memoryUsage").value("0.0000"))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.wanEnabled").value(false))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.memberCount").value(1))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.putsRate").value(12.31D))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.totalMemory").value(0))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.entryCount").value(0))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.compressionCodec").value("NA"))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.name").value(REGION_NAME))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.systemRegionEntryCount").value(0))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.persistence").value("OFF"))
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.memoryWritesTrend").isEmpty())
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.entrySize").value("0.0000"))
    ;
  }

  @Test
  public void pulseUpdateForClusterSelectedRegionsMember() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"ClusterSelectedRegionsMember\":{\"regionFullPath\":\"" + REGION_PATH + "\"}}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ClusterSelectedRegionsMember.selectedRegionsMembers.%s.diskReadsTrend", MEMBER_NAME).isEmpty())
        .andExpect(jsonPath("$.ClusterSelectedRegionsMember.selectedRegionsMembers.%s.regionFullPath", MEMBER_NAME).value(REGION_PATH))
        .andExpect(jsonPath("$.ClusterSelectedRegionsMember.selectedRegionsMembers.%s.entryCount", MEMBER_NAME).value(0))
        .andExpect(jsonPath("$.ClusterSelectedRegionsMember.selectedRegionsMembers.%s.accessor", MEMBER_NAME).value("True"))
        .andExpect(jsonPath("$.ClusterSelectedRegionsMember.selectedRegionsMembers.%s.memberName", MEMBER_NAME).value(MEMBER_NAME))
        .andExpect(jsonPath("$.ClusterSelectedRegionsMember.selectedRegionsMembers.%s.memoryReadsTrend", MEMBER_NAME).isEmpty())
        .andExpect(jsonPath("$.ClusterSelectedRegionsMember.selectedRegionsMembers.%s.diskWritesTrend", MEMBER_NAME).isEmpty())
        .andExpect(jsonPath("$.ClusterSelectedRegionsMember.selectedRegionsMembers.%s.memoryWritesTrend", MEMBER_NAME).isEmpty())
        .andExpect(jsonPath("$.ClusterSelectedRegionsMember.selectedRegionsMembers.%s.entrySize", MEMBER_NAME).value(0))
        .andExpect(jsonPath("$.ClusterSelectedRegionsMember.clusterName").value(CLUSTER_NAME))
        .andExpect(jsonPath("$.ClusterSelectedRegionsMember.userName").value(PRINCIPAL_USER))
    ;
  }

  @Test
  public void pulseUpdateForClusterWANInfo() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"ClusterWANInfo\":\"{}\"}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ClusterWANInfo.connectedClusters").isEmpty())
    ;
  }

  @Test
  public void pulseUpdateForMemberAsynchEventQueues() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"MemberAsynchEventQueues\":{\"memberName\":\"" + MEMBER_NAME + "\"}}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.MemberAsynchEventQueues.isAsyncEventQueuesPresent").value(true))
        .andExpect(jsonPath("$.MemberAsynchEventQueues.asyncEventQueues[0].batchTimeInterval").value(0))
        .andExpect(jsonPath("$.MemberAsynchEventQueues.asyncEventQueues[0].batchConflationEnabled").value(false))
        .andExpect(jsonPath("$.MemberAsynchEventQueues.asyncEventQueues[0].queueSize").value(0))
        .andExpect(jsonPath("$.MemberAsynchEventQueues.asyncEventQueues[0].senderType").value(false))
        .andExpect(jsonPath("$.MemberAsynchEventQueues.asyncEventQueues[0].asyncEventListener").value(AEQ_LISTENER))
        .andExpect(jsonPath("$.MemberAsynchEventQueues.asyncEventQueues[0].batchSize").value(0))
        .andExpect(jsonPath("$.MemberAsynchEventQueues.asyncEventQueues[0].primary").value(false))
    ;
  }

  @Test
  public void pulseUpdateForMemberClients() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"MemberClients\":{\"memberName\":\"" + MEMBER_NAME + "\"}}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.MemberClients.name").value(MEMBER_NAME))
        .andExpect(jsonPath("$.MemberClients.memberClients[0].puts").value(0))
        .andExpect(jsonPath("$.MemberClients.memberClients[0].cpuUsage").value("0.0000"))
        .andExpect(jsonPath("$.MemberClients.memberClients[0].clientId").value("100"))
        .andExpect(jsonPath("$.MemberClients.memberClients[0].queueSize").value(0))
        .andExpect(jsonPath("$.MemberClients.memberClients[0].clientCQCount").value(0))
        .andExpect(jsonPath("$.MemberClients.memberClients[0].name").value(CLIENT_NAME))
        .andExpect(jsonPath("$.MemberClients.memberClients[0].isConnected").value("No"))
        .andExpect(jsonPath("$.MemberClients.memberClients[0].threads").value(0))
        .andExpect(jsonPath("$.MemberClients.memberClients[0].isSubscriptionEnabled").value("No"))
        .andExpect(jsonPath("$.MemberClients.memberClients[0].gets").value(0))
        .andExpect(jsonPath("$.MemberClients.memberClients[0].uptime").value("0 Hours 0 Mins 1 Secs"))
    ;
  }

  @Test
  public void pulseUpdateForMemberDetails() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"MemberDetails\":{\"memberName\":\"" + MEMBER_NAME + "\"}}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.MemberDetails.name").value(MEMBER_NAME))
        .andExpect(jsonPath("$.MemberDetails.offHeapUsedSize").value(0))
        .andExpect(jsonPath("$.MemberDetails.diskStorageUsed").value(0D))
        .andExpect(jsonPath("$.MemberDetails.regionsCount").value(1))
        .andExpect(jsonPath("$.MemberDetails.clusterName").value(CLUSTER_NAME))
        .andExpect(jsonPath("$.MemberDetails.name").value(MEMBER_NAME))
        .andExpect(jsonPath("$.MemberDetails.threads").value(0))
        .andExpect(jsonPath("$.MemberDetails.clusterId").isNotEmpty())
        .andExpect(jsonPath("$.MemberDetails.numClients").value(1))
        .andExpect(jsonPath("$.MemberDetails.userName").value(PRINCIPAL_USER))
        .andExpect(jsonPath("$.MemberDetails.offHeapFreeSize").value(0))
        .andExpect(jsonPath("$.MemberDetails.memberId").value(MEMBER_ID))
        .andExpect(jsonPath("$.MemberDetails.status").value("Normal"))
    ;
  }

  @Test
  public void pulseUpdateForMemberDiskThroughput() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"MemberDiskThroughput\":{\"memberName\":\"" + MEMBER_NAME + "\"}}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.MemberDiskThroughput.throughputWritesTrend").isEmpty())
        .andExpect(jsonPath("$.MemberDiskThroughput.throughputReadsTrend").isEmpty())
        .andExpect(jsonPath("$.MemberDiskThroughput.throughputWrites").value(0D))
        .andExpect(jsonPath("$.MemberDiskThroughput.throughputReads").value(0D))
    ;
  }

  @Test
  public void pulseUpdateForMemberGatewayHub() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"MemberGatewayHub\":{\"memberName\":\"" + MEMBER_NAME + "\"}}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.MemberGatewayHub.isGatewayReceiver").value(false))
        .andExpect(jsonPath("$.MemberGatewayHub.asyncEventQueues[0].batchTimeInterval").value(0))
        .andExpect(jsonPath("$.MemberGatewayHub.asyncEventQueues[0].batchConflationEnabled").value(false))
        .andExpect(jsonPath("$.MemberGatewayHub.asyncEventQueues[0].queueSize").value(0))
        .andExpect(jsonPath("$.MemberGatewayHub.asyncEventQueues[0].senderType").value(false))
        .andExpect(jsonPath("$.MemberGatewayHub.asyncEventQueues[0].asyncEventListener").value(AEQ_LISTENER))
        .andExpect(jsonPath("$.MemberGatewayHub.asyncEventQueues[0].batchSize").value(0))
        .andExpect(jsonPath("$.MemberGatewayHub.asyncEventQueues[0].primary").value(false))
        .andExpect(jsonPath("$.MemberGatewayHub.isGatewaySender").value(false))
        .andExpect(jsonPath("$.MemberGatewayHub.regionsInvolved").isEmpty())
        .andExpect(jsonPath("$.MemberGatewayHub.gatewaySenders").isEmpty())
    ;
  }

  @Test
  public void pulseUpdateForMemberGCPauses() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"MemberGCPauses\":{\"memberName\":\"" + MEMBER_NAME + "\"}}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.MemberGCPauses.gcPausesCount").value(0))
        .andExpect(jsonPath("$.MemberGCPauses.gcPausesTrend").isEmpty())
    ;
  }

  @Test
  public void pulseUpdateForMemberHeapUsage() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"MemberHeapUsage\":{\"memberName\":\"" + MEMBER_NAME + "\"}}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.MemberHeapUsage.heapUsageTrend").isEmpty())
        .andExpect(jsonPath("$.MemberHeapUsage.currentHeapUsage").value(0))
    ;
  }

  @Test
  public void pulseUpdateForMemberKeyStatistics() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"MemberKeyStatistics\":{\"memberName\":\"" + MEMBER_NAME + "\"}}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.MemberKeyStatistics.readPerSecTrend").isEmpty())
        .andExpect(jsonPath("$.MemberKeyStatistics.cpuUsageTrend").isEmpty())
        .andExpect(jsonPath("$.MemberKeyStatistics.memoryUsageTrend").isEmpty())
        .andExpect(jsonPath("$.MemberKeyStatistics.writePerSecTrend").isEmpty())
    ;
  }

  @Test
  public void pulseUpdateForMemberRegions() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"MemberRegions\":{\"memberName\":\"" + MEMBER_NAME + "\"}}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.MemberRegions.name").value(MEMBER_NAME))
        .andExpect(jsonPath("$.MemberRegions.memberRegions[0].fullPath").value(REGION_PATH))
        .andExpect(jsonPath("$.MemberRegions.memberRegions[0].entryCount").value(0))
        .andExpect(jsonPath("$.MemberRegions.memberRegions[0].name").value(REGION_NAME))
        .andExpect(jsonPath("$.MemberRegions.memberRegions[0].diskStoreName").value(""))
        .andExpect(jsonPath("$.MemberRegions.memberRegions[0].gatewayEnabled").value(false))
        .andExpect(jsonPath("$.MemberRegions.memberRegions[0].entrySize").value("0.0000"))
        .andExpect(jsonPath("$.MemberRegions.memberId").value(MEMBER_ID))
        .andExpect(jsonPath("$.MemberRegions.status").value("Normal"))
    ;
  }

  @Test
  public void pulseUpdateForMembersList() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"MembersList\":{\"memberName\":\"" + MEMBER_NAME + "\"}}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.MembersList.clusterMembers[0].name").value(MEMBER_NAME))
        .andExpect(jsonPath("$.MembersList.clusterMembers[0].memberId").value(MEMBER_ID))
        .andExpect(jsonPath("$.MembersList.clusterName").value(CLUSTER_NAME))
    ;
  }

  @Test
  public void pulseUpdateForPulseVersion() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"PulseVersion\":\"{}\"}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.PulseVersion.sourceDate").value("not empty"))
        .andExpect(jsonPath("$.PulseVersion.sourceRepository").value("not empty"))
        .andExpect(jsonPath("$.PulseVersion.pulseVersion").value("not empty"))
        .andExpect(jsonPath("$.PulseVersion.sourceRevision").value("not empty"))
        .andExpect(jsonPath("$.PulseVersion.buildId").value("not empty"))
        .andExpect(jsonPath("$.PulseVersion.buildDate").value("not empty"))
    ;
  }

  @Test
  public void pulseUpdateForQueryStatistics() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"QueryStatistics\":\"{}\"}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.QueryStatistics.queriesList").isEmpty())
        .andExpect(jsonPath("$.QueryStatistics.connectedFlag").value(false))
        .andExpect(jsonPath("$.QueryStatistics.connectedErrorMsg").value(""))
    ;
  }

  @Test
  public void pulseUpdateForSystemAlerts() throws Exception {
    this.mockMvc.perform(post("/pulseUpdate")
        .param("pulseData", "{\"SystemAlerts\":{\"pageNumber\":\"1\"}}")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.SystemAlerts.pageNumber").value(1))
        .andExpect(jsonPath("$.SystemAlerts.connectedFlag").value(false))
        .andExpect(jsonPath("$.SystemAlerts.connectedErrorMsg").value(""))
        .andExpect(jsonPath("$.SystemAlerts.systemAlerts").isEmpty())
    ;
  }

  @Test
  public void authenticateUserNotLoggedIn() throws Exception {
    this.mockMvc.perform(get("/authenticateUser")
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.isUserLoggedIn").value(false));
  }

  @Test
  public void authenticateUserLoggedIn() throws Exception {
    this.mockMvc.perform(get("/authenticateUser")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.isUserLoggedIn").value(true));
  }

  @Test
  public void pulseVersion() throws Exception {
    this.mockMvc.perform(get("/pulseVersion")
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pulseVersion").isNotEmpty())
        .andExpect(jsonPath("$.buildId").isNotEmpty())
        .andExpect(jsonPath("$.buildDate").isNotEmpty())
        .andExpect(jsonPath("$.sourceDate").isNotEmpty())
        .andExpect(jsonPath("$.sourceRevision").isNotEmpty())
        .andExpect(jsonPath("$.sourceRepository").isNotEmpty())
    ;
  }

  @Test
  public void clearAlerts() throws Exception {
    this.mockMvc.perform(get("/clearAlerts")
        .param("alertType", "1")
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.pageNumber").value(1))
        .andExpect(jsonPath("$.systemAlerts").isEmpty())
        .andExpect(jsonPath("$.connectedFlag").value(false))
        .andExpect(jsonPath("$.status").value("deleted"))
    ;
  }

  @Test
  public void acknowledgeAlert() throws Exception {
    this.mockMvc.perform(get("/acknowledgeAlert")
        .param("alertId", "1")
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("deleted"));
  }

  @Test
  public void dataBrowserRegions() throws Exception {
    this.mockMvc.perform(get("/dataBrowserRegions")
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.clusterName").value(CLUSTER_NAME))
        .andExpect(jsonPath("$.connectedFlag").value(false))
        .andExpect(jsonPath("$.clusterRegions[0].fullPath").value(REGION_PATH))
        .andExpect(jsonPath("$.clusterRegions[0].regionType").value(REGION_TYPE))
    ;
  }

  @Test
  public void dataBrowserQuery() throws Exception {
    doReturn(mapper.createObjectNode().put("foo", "bar")).when(cluster).executeQuery(anyString(), anyString(), anyInt());

    this.mockMvc.perform(get("/dataBrowserQuery")
        .param("query", "SELECT * FROM " + REGION_PATH)
        .param("members", MEMBER_NAME)
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.foo").value("bar"))
    ;
  }

  @Test
  public void dataBrowserQueryHistory() throws Exception {
    dataBrowserQuery();

    this.mockMvc.perform(get("/dataBrowserQueryHistory")
        .param("action", "view")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.queryHistory[0].queryText").value("\"SELECT * FROM " + REGION_PATH + "\""))
    ;
  }

  @Test
  public void pulseProductSupport() throws Exception {
    this.mockMvc.perform(get("/pulseProductSupport")
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.product").value("gemfire"))
    ;
  }

  @Test
  public void getQueryStatisticsGridModel() throws Exception {
    this.mockMvc.perform(get("/getQueryStatisticsGridModel")
        .principal(principal)
        .accept(MediaType.parseMediaType(MediaType.APPLICATION_JSON_UTF8_VALUE)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.columnNames", containsInAnyOrder("Query",
            "NumExecution",
            "TotalExecutionTime(ns)",
            "NumExecutionsInProgress",
            "NumTimesCompiled",
            "NumTimesGlobalIndexLookup",
            "NumRowsModified",
            "ParseTime(ms)",
            "BindTime(ms)",
            "OptimizeTime(ms)",
            "RoutingInfoTime(ms)",
            "GenerateTime(ms)",
            "TotalCompilationTime(ms)",
            "ExecutionTime(ns)",
            "ProjectionTime(ns)",
            "RowsModificationTime(ns)",
            "QNNumRowsSeen",
            "QNMsgSendTime(ns)",
            "QNMsgSerTime(ns)",
            "QNRespDeSerTime(ns)")))
    ;
  }
}
