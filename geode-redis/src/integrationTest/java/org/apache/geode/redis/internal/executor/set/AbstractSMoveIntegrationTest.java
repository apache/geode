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
package org.apache.geode.redis.internal.executor.set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.management.internal.cli.util.ThreePhraseGenerator;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractSMoveIntegrationTest implements RedisPortSupplier {
  private Jedis jedis;
  private Jedis jedis2;
  private static final ThreePhraseGenerator generator = new ThreePhraseGenerator();
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    jedis2 = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    jedis.flushAll();
    jedis.close();
    jedis2.close();
  }

  @Test
  public void givenSourceNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SMOVE))
        .hasMessageContaining("ERR wrong number of arguments for 'smove' command");
  }

  @Test
  public void givenDestinationNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SMOVE, "source"))
        .hasMessageContaining("ERR wrong number of arguments for 'smove' command");
  }

  @Test
  public void givenMemberNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SMOVE, "source", "destination"))
        .hasMessageContaining("ERR wrong number of arguments for 'smove' command");
  }

  @Test
  public void givenMoreThanFourArguments_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(
        () -> jedis.sendCommand(Protocol.Command.SMOVE, "key", "destination", "member", "extraArg"))
            .hasMessageContaining("ERR wrong number of arguments for 'smove' command");
  }

  @Test
  public void testSMove() {
    String source = generator.generate('x');
    String dest = generator.generate('x');
    String test = generator.generate('x');
    int elements = 10;
    String[] strings = generateStrings(elements, 'x');
    jedis.sadd(source, strings);

    long i = 1;
    for (String entry : strings) {
      long results = jedis.smove(source, dest, entry);
      assertThat(results).isEqualTo(1);
      assertThat(jedis.sismember(dest, entry)).isTrue();

      results = jedis.scard(source);
      assertThat(results).isEqualTo(strings.length - i);
      assertThat(jedis.scard(dest)).isEqualTo(i);
      i++;
    }

    assertThat(jedis.smove(test, dest, generator.generate('x'))).isEqualTo(0);
  }

  @Test
  public void testSMoveNegativeCases() {
    String source = "source";
    String dest = "dest";
    jedis.sadd(source, "sourceField");
    jedis.sadd(dest, "destField");
    String nonexistentField = "nonexistentField";

    assertThat(jedis.smove(source, dest, nonexistentField)).isEqualTo(0);
    assertThat(jedis.sismember(dest, nonexistentField)).isFalse();
    assertThat(jedis.smove(source, "nonexistentDest", nonexistentField)).isEqualTo(0);
    assertThat(jedis.smove("nonExistentSource", dest, nonexistentField)).isEqualTo(0);
  }

  @Test
  public void testConcurrentSMove() throws ExecutionException, InterruptedException {
    String source = generator.generate('x');
    String dest = generator.generate('y');
    int elements = 1000;
    String[] strings = generateStrings(elements, 'x');
    jedis.sadd(source, strings);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    Callable<Long> callable1 = () -> moveSetElements(source, dest, strings, jedis);
    Callable<Long> callable2 = () -> moveSetElements(source, dest, strings, jedis2);
    Future<Long> future1 = pool.submit(callable1);
    Future<Long> future2 = pool.submit(callable2);

    assertThat(future1.get() + future2.get()).isEqualTo(new Long(strings.length));
    assertThat(jedis.smembers(dest)).containsExactlyInAnyOrder(strings);
    assertThat(jedis.scard(source)).isEqualTo(0L);
  }

  private long moveSetElements(String source, String dest, String[] strings,
      Jedis jedis) {
    long results = 0;
    for (String entry : strings) {
      results += jedis.smove(source, dest, entry);
      Thread.yield();
    }
    return results;
  }

  private String[] generateStrings(int elements, char uniqueElement) {
    Set<String> strings = new HashSet<>();
    for (int i = 0; i < elements; i++) {
      String elem = generator.generate(uniqueElement);
      strings.add(elem);
    }
    return strings.toArray(new String[strings.size()]);
  }
}
