package org.apache.geode.redis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class RedisStatsIntegrationTest {

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @Test
  public void clientsStat_withConnectAndClose_isCorrect() throws InterruptedException {
    long initialClients = server.getServer().getStats().getClients();
    Jedis jedis = new Jedis("localhost", server.getPort(), 10000000);

    jedis.ping();
    assertThat(server.getServer().getStats().getClients()).isEqualTo(initialClients + 1);

    jedis.close();
    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(server.getServer().getStats().getClients()).isEqualTo(initialClients));
  }
}
