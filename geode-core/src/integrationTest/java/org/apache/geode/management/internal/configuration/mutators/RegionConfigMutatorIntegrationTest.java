package org.apache.geode.management.internal.configuration.mutators;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

public class RegionConfigMutatorIntegrationTest {

  @Rule
  public LocatorStarterRule locator = new LocatorStarterRule().withAutoStart();

  private RegionConfigMutator mutator;
  private RegionConfig config;

  @Before
  public void before() throws Exception {
    config = new RegionConfig();
    mutator = new RegionConfigMutator();
  }

  @Test
  public void sanity() throws Exception {
    config.setRefid("REPLICATE");
    config.setName("test");
    CacheConfig cacheConfig =
        locator.getLocator().getConfigurationPersistenceService().getCacheConfig("cluster", true);

    mutator.add(config, cacheConfig);
    assertThat(CacheElement.findElement(cacheConfig.getRegions(), config.getId())).isNotNull();
  }
}
