package org.apache.geode.modules.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class RegionHelperIntegrationTest {
  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withNoCacheServer().withRegion(RegionShortcut.REPLICATE, "test");

  @Test
  public void generateXml() throws Exception {
    Region region = server.getCache().getRegion("/test");
    region.put("key", "value");
    String cacheXml = RegionHelper.generateCacheXml(server.getCache());

    // make sure the generated cache.xml skips region entries
    assertThat(cacheXml).doesNotContain("<entry>")
        .doesNotContain("<string>key</string>")
        .doesNotContain("<string>value</string>");
  }
}
