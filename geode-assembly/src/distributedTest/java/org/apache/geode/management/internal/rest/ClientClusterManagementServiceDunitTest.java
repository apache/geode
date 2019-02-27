package org.apache.geode.management.internal.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.GeodeManagementException;
import org.apache.geode.management.client.ClusterManagementServiceProvider;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class ClientClusterManagementServiceDunitTest {
  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(2);

  private MemberVM locator, server;
  private ClusterManagementService cmsClient;

  @Before
  public void before() {
    locator = cluster.startLocatorVM(0, l -> l.withHttpService());
    server = cluster.startServerVM(1, locator.getPort());
    cmsClient = ClusterManagementServiceProvider
        .getService("http://localhost:" + locator.getHttpPort() + "/geode-management");
  }

  @Test
  public void createRegion() {
    RegionConfig region = new RegionConfig();
    region.setName("customer");
    region.setType("REPLICATE");

    ClusterManagementResult result = cmsClient.create(region, "");

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getMemberStatuses()).containsKeys("server-1").hasSize(1);

    try {
      cmsClient.create(region, "");
      fail("Expected a GeodeManagementException here.");
    } catch (GeodeManagementException e) {
      assertThat(e.getServerException()).contains("EntityExistsException");
      assertThat(e.getMessage()).contains("cache element customer already exists");
    }
  }

  @Test
  public void createRegionWithInvalidName() throws Exception {
    RegionConfig region = new RegionConfig();
    region.setName("__test");

    try {
      cmsClient.create(region, "");
      fail("Expected a GeodeManagementException here.");
    } catch (GeodeManagementException e) {
      assertThat(e.getServerException()).contains("IllegalArgumentException");
      assertThat(e.getMessage()).contains("Region names may not begin with a double-underscore");
    }
  }
}
