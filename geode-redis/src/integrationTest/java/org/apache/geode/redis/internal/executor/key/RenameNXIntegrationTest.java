package org.apache.geode.redis.internal.executor.key;

import org.junit.ClassRule;

import org.apache.geode.redis.GeodeRedisServerRule;

public class RenameNXIntegrationTest extends AbstractRenameNXIntegrationTest {
  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @Override
  public int getPort() {
    return server.getPort();
  }

}
