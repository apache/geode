package org.apache.geode.cache;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;

public class ClientAccessToInternalStatsTest {

  @Test
  public void canGetLongCounterAsInteger() {
    try (ClientCache cache = new ClientCacheFactory().create()) {
      Region region = cache.createClientRegionFactory(ClientRegionShortcut.LOCAL).create("local");
      region.put("key", "value");
      Statistics[] stats = cache.getDistributedSystem().findStatisticsByTextId("cachePerfStats");
      assertThat(stats).hasSize(1);
      assertThat(stats[0].getInt("puts")).isEqualTo(1);
    }
  }
}
