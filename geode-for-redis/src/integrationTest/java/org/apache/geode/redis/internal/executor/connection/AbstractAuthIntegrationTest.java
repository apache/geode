/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.redis.internal.executor.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;


public abstract class AbstractAuthIntegrationTest {

  Jedis jedis;

  protected abstract void setupCacheWithSecurity() throws Exception;

  protected abstract void setupCacheWithoutSecurity() throws Exception;

  protected abstract int getPort();

  protected abstract String getUsername();

  protected abstract String getPassword();

  @Test
  public void givenSecurity_authWithIncorrectNumberOfArguments_fails() throws Exception {
    setupCacheWithSecurity();
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.AUTH))
        .hasMessageContaining("ERR wrong number of arguments for 'auth' command");
    assertThatThrownBy(
        () -> jedis.sendCommand(Protocol.Command.AUTH, "username", "password", "extraArg"))
            .hasMessageContaining("ERR syntax error");
  }

  @Test
  public void givenSecurity_clientCanAuthAfterFailedAuth_passes() throws Exception {
    setupCacheWithSecurity();

    assertThatThrownBy(() -> jedis.auth(getUsername(), "wrongpwd"))
        .hasMessageContaining("WRONGPASS invalid username-password pair or user is disabled.");

    assertThat(jedis.auth(getUsername(), getPassword())).isEqualTo("OK");
    assertThat(jedis.ping()).isEqualTo("PONG");
  }

  @Test
  public void givenSecurity_authorizedUser_passes() throws Exception {
    setupCacheWithSecurity();

    assertThatThrownBy(() -> jedis.set("foo", "bar"))
        .hasMessage("NOAUTH Authentication required.");

    assertThat(jedis.auth(getUsername(), getPassword())).isEqualTo("OK");

    jedis.set("foo", "bar"); // No exception
  }

  @Test
  public void givenSecurity_authWithCorrectPasswordForDefaultUser_passes() throws Exception {
    setupCacheWithSecurity();

    assertThat(jedis.auth(getPassword())).isEqualTo("OK");
    assertThat(jedis.ping()).isEqualTo("PONG");
  }

  @Test
  public void givenSecurity_authWithIncorrectPasswordForDefaultUser_fails() throws Exception {
    setupCacheWithSecurity();

    assertThatThrownBy(() -> jedis.auth("wrong-password"))
        .hasMessage("WRONGPASS invalid username-password pair or user is disabled.");
  }

  /**
   * Authentication and authorization is sometimes implemented and handled using thread locals.
   * Our security implementation can do this but the way it's used here does not (and must not)
   * since netty threads are shared between connections. This test should be using a single netty
   * thread so that we can be sure that multiple connections will not inadvertently leak auth data
   * between them.
   */
  @Test
  public void givenSecurity_separateClientRequest_doNotInteract() throws Exception {
    setupCacheWithSecurity();
    Jedis nonAuthorizedJedis = new Jedis("localhost", getPort(), 100000);
    Jedis authorizedJedis = new Jedis("localhost", getPort(), 100000);

    assertThat(authorizedJedis.auth(getUsername(), getPassword())).isEqualTo("OK");
    assertThat(authorizedJedis.set("foo", "bar")).isEqualTo("OK");

    assertThatThrownBy(() -> nonAuthorizedJedis.set("foo", "bar"))
        .hasMessage("NOAUTH Authentication required.");

    authorizedJedis.close();
    nonAuthorizedJedis.close();
  }

  @Test
  public void givenSecurity_lettuceV6AuthClient_passes() throws Exception {
    setupCacheWithSecurity();

    RedisURI uri =
        RedisURI.create(String.format("redis://%s@localhost:%d", getUsername(), getPort()));
    RedisClient client = RedisClient.create(uri);

    client.connect().sync().ping();
  }

  @Test
  public void givenNoSecurity_accessWithoutAuth_passes() throws Exception {
    setupCacheWithoutSecurity();

    assertThat(jedis.ping()).isEqualTo("PONG");
  }

}
