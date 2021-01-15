package org.apache.geode.redis.internal.executor.key;

import org.junit.ClassRule;

import org.apache.geode.NativeRedisTestRule;

public class RenameNXNativeRedisAcceptanceTest extends AbstractRenameNXIntegrationTest {
  @ClassRule
  public static NativeRedisTestRule redis = new NativeRedisTestRule();

  @Override
  public int getPort() {
    return redis.getPort();
  }

}
