package org.apache.geode.management.internal.configuration.realizers;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class RegionConfigRealizerIntegrationTest {

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withAutoStart();

  private RegionConfigRealizer realizer;
  private RegionConfig config;

  @Before
  public void setup() {
    config = new RegionConfig();
    realizer = new RegionConfigRealizer();
  }

  @Test
  public void sanityCheck() throws Exception {
    config.setName("test");
    config.setRefid("REPLICATE");

    realizer.create(config, server.getCache());

    Region<Object, Object> region = server.getCache().getRegion("test");
    assertThat(region).isNotNull();
    assertThat(region.getAttributes().getDataPolicy()).isEqualTo(DataPolicy.REPLICATE);
    assertThat(region.getAttributes().getScope()).isEqualTo(Scope.DISTRIBUTED_ACK);
  }
}
