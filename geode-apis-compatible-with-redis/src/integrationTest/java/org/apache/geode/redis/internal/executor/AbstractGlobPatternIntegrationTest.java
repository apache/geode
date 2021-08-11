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

package org.apache.geode.redis.internal.executor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractGlobPatternIntegrationTest implements RedisIntegrationTest {

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
  public void asterisk_matchesAnySequenceOfCharacters() {
    jedis.set("ble", "value");
    jedis.set("blo9%^e", "value");
    jedis.set("blooo", "value");

    assertThat(jedis.keys("bl*e")).containsExactlyInAnyOrder("ble", "blo9%^e");
  }

  @Test
  public void questionMark_matchesAnySingleCharacter() {
    jedis.set("hollo", "value");
    jedis.set("hello", "value");
    jedis.set("hoollo", "value");

    assertThat(jedis.keys("h?llo")).containsExactlyInAnyOrder("hollo", "hello");
  }

  @Test
  public void charactersInsideBrackets_matchSpecifiedCharacters() {
    jedis.set("top", "value");
    jedis.set("tap", "value");
    jedis.set("tep", "value");

    assertThat(jedis.keys("t[oa]p")).containsExactlyInAnyOrder("top", "tap");
  }

  @Test
  public void dashInsideBrackets_matchesSpecifiedRangeOfCharacters() {
    jedis.set("smch", "value");
    jedis.set("srch", "value");
    jedis.set("such", "value");

    assertThat(jedis.keys("s[m-s]ch")).containsExactlyInAnyOrder("smch", "srch");
  }

  @Test
  public void caretInsideBrackets_doesNotMatchSpecifiedCharacter() {
    jedis.set("patches", "value");
    jedis.set("petches", "value");
    jedis.set("potches", "value");

    assertThat(jedis.keys("p[^o]tches")).containsExactlyInAnyOrder("petches", "patches");
  }

  @Test
  public void charactersInsideUnclosedBracket_matchSpecifiedCharacters() {
    jedis.set("*", "value");
    jedis.set("}", "value");
    jedis.set("4", "value");
    jedis.set("[", "value");

    assertThat(jedis.keys("[*}4")).containsExactlyInAnyOrder("*", "}", "4");
  }

  @Test
  public void dashInsideUnclosedBracket_matchesSpecifiedRangeOfCharacters() {
    jedis.set("sm", "value");
    jedis.set("ss", "value");
    jedis.set("ssch", "value");

    assertThat(jedis.keys("s[m-sch")).containsExactlyInAnyOrder("sm", "ss");
  }

  @Test
  public void caretInsideUnclosedBracket_doesNotMatchSpecifiedCharacter() {
    jedis.set("kermi", "value");
    jedis.set("kermu", "value");
    jedis.set("kermo", "value");

    assertThat(jedis.keys("kerm[^i")).containsExactlyInAnyOrder("kermu", "kermo");
  }

  @Test
  public void doubleOpenBracket_matchesSpecifiedCharacters() {
    jedis.set("*", "value");
    jedis.set("[", "value");
    jedis.set("[4", "value");
    jedis.set("&8t", "value");

    assertThat(jedis.keys("[[*")).containsExactlyInAnyOrder("*", "[");
  }

  @Test
  public void escapedOpenBracket_matchesVerbatim() {
    jedis.set("[", "value");
    jedis.set("[o9$", "value");
    jedis.set("such", "value");

    assertThat(jedis.keys("\\[*")).containsExactlyInAnyOrder("[", "[o9$");
  }

  @Test
  public void escapedQuestionMark_matchesVerbatim() {
    jedis.set("?", "value");
    jedis.set("?oo", "value");
    jedis.set("such", "value");

    assertThat(jedis.keys("\\?*")).containsExactlyInAnyOrder("?", "?oo");
  }

  @Test
  public void escapedAsterisk_matchesVerbatim() {
    jedis.set("*", "value");
    jedis.set("*9x", "value");
    jedis.set("such", "value");

    assertThat(jedis.keys("\\**")).containsExactlyInAnyOrder("*", "*9x");
  }

  @Test
  public void caretOutsideBrackets_matchesVerbatim() {
    jedis.set("^o", "value");
    jedis.set("^o99", "value");
    jedis.set("^O", "value");

    assertThat(jedis.keys("^o*")).containsExactlyInAnyOrder("^o", "^o99");
  }

  @Test
  public void openCurly_matchesVerbatim() {
    jedis.set("{", "value");
    jedis.set("{&0", "value");
    jedis.set("bogus", "value");

    assertThat(jedis.keys("{*")).containsExactlyInAnyOrder("{", "{&0");
  }

  @Test
  public void closedCurly_matchesVerbatim() {
    jedis.set("}", "value");
    jedis.set("}&0", "value");
    jedis.set("bogus", "value");

    assertThat(jedis.keys("}*")).containsExactlyInAnyOrder("}", "}&0");
  }

  @Test
  public void comma_matchesVerbatim() {
    jedis.set("kermit", "value");
    jedis.set("kerm,t", "value");
    jedis.set("kermot", "value");
    jedis.set("kermat", "value");

    assertThat(jedis.keys("kerm[i,o]t")).containsExactlyInAnyOrder("kermit", "kermot", "kerm,t");
  }

  @Test
  public void exclamationPoint_matchesVerbatim() {
    jedis.set("kerm!t", "value");
    jedis.set("kermot", "value");
    jedis.set("kermit", "value");

    assertThat(jedis.keys("kerm[!i]t")).containsExactlyInAnyOrder("kermit", "kerm!t");
  }

  @Test
  public void period_matchesVerbatim() {
    jedis.set("...", "value");
    jedis.set(".", "value");
    jedis.set(".&*", "value");
    jedis.set("kermat", "value");

    assertThat(jedis.keys(".*")).containsExactlyInAnyOrder("...", ".", ".&*");
  }

  @Test
  public void plusSign_matchesVerbatim() {
    jedis.set("ab", "value");
    jedis.set("aaaaab", "value");
    jedis.set("a+b", "value");

    assertThat(jedis.keys("a+b")).containsExactlyInAnyOrder("a+b");
  }

  @Test
  public void orSign_matchesVerbatim() {
    jedis.set("cat|dog", "value");
    jedis.set("cat", "value");
    jedis.set("dog", "value");

    assertThat(jedis.keys("cat|dog")).containsExactlyInAnyOrder("cat|dog");
  }

  @Test
  public void dollarSign_matchesVerbatim() {
    jedis.set("the cat", "value");
    jedis.set("the cat$", "value");
    jedis.set("dog", "value");

    assertThat(jedis.keys("the cat$")).containsExactlyInAnyOrder("the cat$");
  }

  @Test
  public void parentheses_matchVerbatim() {
    jedis.set("orange(cat)z", "value");
    jedis.set("orange", "value");
    jedis.set("orangecat", "value");

    assertThat(jedis.keys("orange(cat)?")).containsExactlyInAnyOrder("orange(cat)z");
  }

  @Test
  public void escapedBackslash_matchesVerbatim() {
    jedis.set("\\", "value");
    jedis.set("\\!", "value");
    jedis.set("*", "value");

    assertThat(jedis.keys("\\\\*")).containsExactlyInAnyOrder("\\", "\\!");
  }
}
