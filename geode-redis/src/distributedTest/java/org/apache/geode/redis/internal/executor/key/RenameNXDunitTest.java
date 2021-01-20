package org.apache.geode.redis.internal.executor.key;

import static java.lang.Integer.parseInt;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class RenameNXDunitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(3);

  AtomicInteger numberOfPassingTests = new AtomicInteger();
  static final String LOCAL_HOST = "127.0.0.1";
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  static Jedis jedis;
  static Jedis jedis2;

  static Properties locatorProperties;
  static MemberVM locator;

  static MemberVM server1;
  static MemberVM server2;

  @BeforeClass
  public static void setup() {
    locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());

    int redisServerPort1 = clusterStartUp.getRedisPort(1);
    int redisServerPort2 = clusterStartUp.getRedisPort(2);

    jedis = new Jedis(LOCAL_HOST, redisServerPort1, JEDIS_TIMEOUT);
    jedis2 = new Jedis(LOCAL_HOST, redisServerPort2, JEDIS_TIMEOUT);
  }

  @Before
  public void testSetup() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.disconnect();
    jedis2.disconnect();

    server1.stop();
    server2.stop();
  }

  @Test
  public void should_onlyAllowOneRename_givenMultipleThreadsRenamingToSameKeyConcurrently() {

    int index = 10000;
    do {
      doConcurrentReNameNX();
      index--;
    } while (index > 0);
  }

  private void doConcurrentReNameNX() {
    String KEY_1 = "key1";
    String KEY_2 = "key2";
    String CONTESTED_KEY = "contested";
    String VALUE = "value";
    Long server1Value = 0l;
    Long server2Value = 0l;

    jedis.set(KEY_1, VALUE);
    jedis2.set(KEY_2, VALUE);

    Future<Long> server_1_counter = doRenameNX(KEY_1, CONTESTED_KEY);
    Future<Long> server_2_counter = doRenameNX(KEY_2, CONTESTED_KEY);

    try {
      server1Value = server_1_counter.get();
      server2Value = server_2_counter.get();
    } catch (Exception e) {
      e.printStackTrace();
    }

    assertThat(server1Value + server2Value).isLessThanOrEqualTo(1);
    System.out.println(numberOfPassingTests.incrementAndGet() + " times passed");

    GeodeAwaitility.await().atMost(Duration.of(2, SECONDS)).until(() ->
        jedis.del(CONTESTED_KEY) == 0);
  }

  private Future<Long> doRenameNX(String oldKey, String newKey) {
    ExecutorService executor
        = Executors.newSingleThreadExecutor();
    return executor.submit(() -> {
      return jedis.renamenx(oldKey, newKey);
    });
  }

}
