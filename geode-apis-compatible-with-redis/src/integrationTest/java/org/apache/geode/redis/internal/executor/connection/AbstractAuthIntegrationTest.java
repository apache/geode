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

  static final String USERNAME = "default";
  // Since we're going to use a SimpleSecurityManager where password == username means success
  static final String PASSWORD = USERNAME;
  Jedis jedis;

  protected abstract void setupCacheWithSecurity(boolean withSecurityManager) throws Exception;

  protected abstract void setupCacheWithoutSecurity() throws Exception;

  protected abstract int getPort();

  @Test
  public void givenSecurity_authWithIncorrectNumberOfArguments_fails() throws Exception {
    setupCacheWithSecurity(true);
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.AUTH))
        .hasMessageContaining("ERR wrong number of arguments for 'auth' command");
    assertThatThrownBy(
        () -> jedis.sendCommand(Protocol.Command.AUTH, "username", "password", "extraArg"))
            .hasMessageContaining("ERR syntax error");
  }

  @Test
  public void givenSecurity_clientCanAuthAfterFailedAuth_passes() throws Exception {
    setupCacheWithSecurity(true);

    assertThatThrownBy(() -> jedis.auth(USERNAME, "wrongpwd"))
        .hasMessageContaining("WRONGPASS invalid username-password pair or user is disabled.");

    assertThat(jedis.auth(USERNAME, PASSWORD)).isEqualTo("OK");
    assertThat(jedis.ping()).isEqualTo("PONG");
  }

  @Test
  public void givenSecurity_authorizedUser_passes() throws Exception {
    setupCacheWithSecurity(true);

    assertThatThrownBy(() -> jedis.set("foo", "bar"))
        .hasMessage("NOAUTH Authentication required.");

    String res = jedis.auth(USERNAME, PASSWORD);
    assertThat(res).isEqualTo("OK");

    jedis.set("foo", "bar"); // No exception
  }

  @Test
  public void givenSecurity_authWithCorrectPasswordForDefaultUser_passes() throws Exception {
    setupCacheWithSecurity(true);

    assertThat(jedis.auth(PASSWORD)).isEqualTo("OK");
    assertThat(jedis.ping()).isEqualTo("PONG");
  }

  @Test
  public void givenSecurity_authWithIncorrectPasswordForDefaultUser_fails() throws Exception {
    setupCacheWithSecurity(true);

    assertThatThrownBy(() -> jedis.auth("wrong-password"))
        .hasMessage("WRONGPASS invalid username-password pair or user is disabled.");
  }

  @Test
  public void givenSecurity_separateClientRequest_doNotInteract() throws Exception {
    setupCacheWithSecurity(true);
    Jedis nonAuthorizedJedis = new Jedis("localhost", getPort(), 100000);
    Jedis authorizedJedis = new Jedis("localhost", getPort(), 100000);

    assertThat(authorizedJedis.auth(USERNAME, PASSWORD)).isEqualTo("OK");
    assertThat(authorizedJedis.set("foo", "bar")).isEqualTo("OK");

    assertThatThrownBy(() -> nonAuthorizedJedis.set("foo", "bar"))
        .hasMessage("NOAUTH Authentication required.");

    authorizedJedis.close();
    nonAuthorizedJedis.close();
  }

  @Test
  public void givenSecurity_lettuceV6AuthClient_passes() throws Exception {
    setupCacheWithSecurity(true);

    RedisURI uri = RedisURI.create(String.format("redis://%s@localhost:%d", USERNAME, getPort()));
    RedisClient client = RedisClient.create(uri);

    client.connect().sync().ping();
  }

  @Test
  public void givenNoSecurity_authWithDefaultUsernameAndEmptyPassword_passes() throws Exception {
    setupCacheWithoutSecurity();

    assertThat(jedis.auth(USERNAME, "")).isEqualTo("OK");
    assertThat(jedis.ping()).isEqualTo("PONG");
  }

  @Test
  public void givenNoSecurity_authWithDefaultUsernameAndAnyPassword_passes() throws Exception {
    setupCacheWithoutSecurity();

    assertThat(jedis.auth(USERNAME, "wrong-password")).isEqualTo("OK");
    assertThat(jedis.ping()).isEqualTo("PONG");
  }

  @Test
  public void givenNoSecurity_authWithNonDefaultUsername_fails() throws Exception {
    setupCacheWithoutSecurity();

    assertThatThrownBy(() -> jedis.auth("wrong-username", PASSWORD))
        .hasMessage("WRONGPASS invalid username-password pair or user is disabled.");
  }

  @Test
  public void givenNoSecurity_authWithPasswordButWithoutUsername_fails() throws Exception {
    setupCacheWithoutSecurity();

    assertThatThrownBy(() -> jedis.auth("wrong-password"))
        .hasMessage(
            "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?");
  }

  @Test
  public void givenNoSecurity_lettuceV6AuthClient_defaultUsernameAndAnyPassword_passes()
      throws Exception {
    setupCacheWithoutSecurity();

    // Implicitly sends the default username ('default') as part of the AUTH request
    RedisURI uri =
        RedisURI.create(String.format("redis://%s@localhost:%d", "not-default", getPort()));
    RedisClient client = RedisClient.create(uri);

    assertThatThrownBy(() -> client.connect().sync().ping())
        .hasRootCauseMessage(
            "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?");
  }

  @Test
  public void givenNoSecurity_lettuceV6AuthClient_andNonDefaultUsername_fails()
      throws Exception {
    setupCacheWithoutSecurity();

    RedisURI uri = RedisURI.create(
        String.format("redis://%s:%s@localhost:%d", "wrong-username", "not-default", getPort()));
    RedisClient client = RedisClient.create(uri);

    assertThatThrownBy(() -> client.connect().sync().ping())
        .hasRootCauseMessage("WRONGPASS invalid username-password pair or user is disabled.");
  }

}
