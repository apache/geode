package org.apache.geode.redis.internal.executor.key;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractRenameNXIntegrationTest implements RedisPortSupplier {

  private Jedis jedis;

  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
  }

  @Test
  public void shouldFail_givenKeyToRenameDoesNotExist() {
    assertThatThrownBy(() -> jedis.renamenx("nonexistentKey", "pineApple"))
        .hasMessageContaining("ERR no such key");
  }

  @Test
  public void shouldRenameKey_givenNewNameDoesNotExist() {

    jedis.set("key", "value");
    jedis.renamenx("key", "pineApple");

    assertThat(jedis.get("key")).isNull();
    assertThat(jedis.get("pineApple")).isEqualTo("value");
  }

  @Test
  public void shouldReturn1_givenNewNameDoesNotExist() {

    jedis.set("key", "value");
    Long result = jedis.renamenx("key", "pineApple");

    assertThat(result).isEqualTo(1);
  }

  @Test
  public void shouldNotRenameKey_givenNewNameDoesExist() {
    jedis.set("key", "value");
    jedis.set("anotherExistingKey", "anotherValue");

    jedis.renamenx("key", "anotherExistingKey");

    assertThat(jedis.get("key")).isEqualTo("value");
    assertThat(jedis.get("anotherExistingKey")).isEqualTo("anotherValue");
  }

  @Test
  public void shouldReturn0_givenNewNameDoesNotExist() {
    jedis.set("key", "value");
    jedis.set("anotherExistingKey", "anotherValue");

    Long result = jedis.renamenx("key", "anotherExistingKey");

    assertThat(result).isEqualTo(0);
  }
}
