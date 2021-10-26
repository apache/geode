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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_AUTHENTICATED;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNAUTHENTICATED_BULK;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNAUTHENTICATED_MULTIBULK;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.internal.netty.ByteToCommandDecoder;


public abstract class AbstractAuthIntegrationTest {

  Jedis jedis;

  protected abstract void setupCacheWithSecurityAndRegionName(String regionName) throws Exception;

  protected abstract void setupCacheWithSecurity(boolean needsWritePermission) throws Exception;

  protected abstract void setupCacheWithoutSecurity() throws Exception;

  protected abstract int getPort();

  protected abstract String getUsername();

  protected abstract String getPassword();

  @Test
  public void givenSecurity_authWithIncorrectNumberOfArguments_fails() throws Exception {
    setupCacheWithSecurity(false);
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.AUTH))
        .hasMessageContaining("ERR wrong number of arguments for 'auth' command");
    assertThatThrownBy(
        () -> jedis.sendCommand(Protocol.Command.AUTH, "username", "password", "extraArg"))
            .hasMessageContaining("ERR syntax error");
  }

  @Test
  public void givenSecurity_clientCanAuthAfterFailedAuth_passes() throws Exception {
    setupCacheWithSecurity(false);

    assertThatThrownBy(() -> jedis.auth(getUsername(), "wrongpwd"))
        .hasMessageContaining("WRONGPASS invalid username-password pair or user is disabled.");

    assertThat(jedis.auth(getUsername(), getPassword())).isEqualTo("OK");
    assertThat(jedis.ping()).isEqualTo("PONG");
  }

  @Test
  public void givenSecurity_authorizedUser_passes() throws Exception {
    setupCacheWithSecurity(true);

    assertThatThrownBy(() -> jedis.set("foo", "bar"))
        .hasMessage("NOAUTH Authentication required.");

    assertThat(jedis.auth(getUsername(), getPassword())).isEqualTo("OK");

    jedis.set("foo", "bar"); // No exception
  }

  @Test
  public void givenSecurity_authWithCorrectPasswordForDefaultUser_passes() throws Exception {
    setupCacheWithSecurity(false);

    assertThat(jedis.auth(getPassword())).isEqualTo("OK");
    assertThat(jedis.ping()).isEqualTo("PONG");
  }

  @Test
  public void givenSecurity_authWithIncorrectPasswordForDefaultUser_fails() throws Exception {
    setupCacheWithSecurity(false);

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
    setupCacheWithSecurity(true);
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
    setupCacheWithSecurity(false);

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

  @Test
  public void givenSecurityAndNonDefaultRegionName_authPasses() throws Exception {
    setupCacheWithSecurityAndRegionName("nonDefault");

    assertThat(jedis.auth(getPassword())).isEqualTo("OK");
    assertThat(jedis.ping()).isEqualTo("PONG");
  }

  @Test
  public void givenSecurity_largeMultiBulkRequestsFail_whenNotAuthenticated() throws Exception {
    setupCacheWithSecurity(false);

    try (Socket clientSocket = new Socket(BIND_ADDRESS, getPort())) {
      clientSocket.setSoTimeout(1000);
      PrintWriter out = new PrintWriter(clientSocket.getOutputStream());
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

      out.write("*100\r\n");
      out.flush();
      String response = in.readLine();

      assertThat(response).contains(ERROR_UNAUTHENTICATED_MULTIBULK);
    }
  }

  @Test
  public void givenSecurity_largeMultiBulkRequestsSucceed_whenAuthenticated() throws Exception {
    setupCacheWithSecurity(true);

    List<String> msetArgs = new ArrayList<>();
    for (int i = 0; i < ByteToCommandDecoder.UNAUTHENTICATED_MAX_ARRAY_SIZE; i++) {
      msetArgs.add("{hash}key-" + i);
      msetArgs.add("value-" + i);
    }

    assertThat(jedis.auth(getUsername(), getPassword())).isEqualTo("OK");
    assertThat(jedis.mset(msetArgs.toArray(new String[] {}))).isEqualTo("OK");
  }

  @Test
  public void givenNoSecurity_largeMultiBulkRequestsSucceed_whenNotAuthenticated()
      throws Exception {
    setupCacheWithoutSecurity();

    List<String> msetArgs = new ArrayList<>();
    for (int i = 0; i < ByteToCommandDecoder.UNAUTHENTICATED_MAX_ARRAY_SIZE; i++) {
      msetArgs.add("{hash}key-" + i);
      msetArgs.add("value-" + i);
    }

    assertThat(jedis.mset(msetArgs.toArray(new String[] {}))).isEqualTo("OK");
  }

  @Test
  public void givenSecurity_largeBulkStringRequestsFail_whenNotAuthenticated() throws Exception {
    setupCacheWithSecurity(false);

    try (Socket clientSocket = new Socket(BIND_ADDRESS, getPort())) {
      clientSocket.setSoTimeout(1000);
      PrintWriter out = new PrintWriter(clientSocket.getOutputStream());
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

      out.write("*1\r\n$100000000\r\n");
      out.flush();
      String response = in.readLine();

      assertThat(response).contains(ERROR_UNAUTHENTICATED_BULK);
    }
  }

  @Test
  public void givenSecurity_largeBulkStringRequestsSucceed_whenAuthenticated() throws Exception {
    setupCacheWithSecurity(true);
    int stringSize = ByteToCommandDecoder.UNAUTHENTICATED_MAX_BULK_STRING_LENGTH + 1;

    String largeString = StringUtils.repeat('a', stringSize);

    assertThat(jedis.auth(getUsername(), getPassword())).isEqualTo("OK");
    assertThat(jedis.set("key", largeString)).isEqualTo("OK");
  }

  @Test
  public void givenNoSecurity_largeBulkStringRequestsSucceed_whenNotAuthenticated()
      throws Exception {
    setupCacheWithoutSecurity();
    int stringSize = ByteToCommandDecoder.UNAUTHENTICATED_MAX_BULK_STRING_LENGTH + 1;

    String largeString = StringUtils.repeat('a', stringSize);

    assertThat(jedis.set("key", largeString)).isEqualTo("OK");
  }

  @Test
  public void givenSecurity_closingConnectionLogsClientOut() throws Exception {
    setupCacheWithSecurity(false);

    int localPort = AvailablePortHelper.getRandomAvailableTCPPort();

    try (Socket clientSocket = new Socket(BIND_ADDRESS, getPort(), InetAddress.getLoopbackAddress(),
        localPort)) {
      clientSocket.setSoTimeout(1000);
      PrintWriter out = new PrintWriter(clientSocket.getOutputStream());
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

      out.write("*3\r\n$4\r\nAUTH\r\n" +
          "$" + getUsername().length() + "\r\n" + getUsername() + "\r\n" +
          "$" + getPassword().length() + "\r\n" + getPassword() + "\r\n");
      out.flush();
      String response = in.readLine();

      assertThat(response).contains("OK");
    }

    AtomicReference<Socket> socketRef = new AtomicReference<>(null);

    await().pollInterval(Duration.ofSeconds(1))
        .untilAsserted(() -> assertThatNoException().isThrownBy(() -> socketRef.set(
            new Socket(BIND_ADDRESS, getPort(), InetAddress.getLoopbackAddress(), localPort))));

    try (Socket clientSocket = socketRef.get()) {
      clientSocket.setSoTimeout(1000);
      PrintWriter out = new PrintWriter(clientSocket.getOutputStream());
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

      out.write("*1\r\n$4\r\nPING\r\n");
      out.flush();
      String response = in.readLine();

      assertThat(response).contains(ERROR_NOT_AUTHENTICATED);
    }
  }

}
