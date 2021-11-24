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
package org.apache.geode.redis.internal.commands.executor;

import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class GlobPatternTest {
  private GlobPattern createPattern(String pattern) {
    return new GlobPattern(stringToBytes(pattern));
  }

  @Test
  public void verifyEmptyInput() {
    assertThat(createPattern("*").matches(stringToBytes(""))).isFalse();
    assertThat(createPattern("**").matches(stringToBytes(""))).isFalse();
    assertThat(createPattern("?").matches(stringToBytes(""))).isFalse();
    assertThat(createPattern("").matches(stringToBytes(""))).isTrue();
  }

  @Test
  public void verifyOneByteInput() {
    assertThat(createPattern("*").matches(stringToBytes("a"))).isTrue();
    assertThat(createPattern("**").matches(stringToBytes("b"))).isTrue();
    assertThat(createPattern("?").matches(stringToBytes("@"))).isTrue();
    assertThat(createPattern("").matches(stringToBytes("a"))).isFalse();
  }

  @Test
  public void patternThatEndsWithWildcards() {
    assertThat(createPattern("foo****").matches(stringToBytes("foo"))).isTrue();
    assertThat(createPattern("foo****").matches(stringToBytes("foo1"))).isTrue();
    assertThat(createPattern("foo****").matches(stringToBytes("foo11111111111"))).isTrue();
    assertThat(createPattern("foo****").matches(stringToBytes("fo11111111111"))).isFalse();
  }

  @Test
  public void patternsWithEmbeddedWildcards() {
    assertThat(createPattern("foo****bar").matches(stringToBytes("foobar"))).isTrue();
    assertThat(createPattern("foo****bar").matches(stringToBytes("foo123bar"))).isTrue();
    assertThat(createPattern("foo****bar").matches(stringToBytes("foo123bar!"))).isFalse();
    assertThat(createPattern("*bar*").matches(stringToBytes("foobarbaz"))).isTrue();
    assertThat(createPattern("*bar*joe").matches(stringToBytes("foorbarbazjoe"))).isTrue();
    assertThat(createPattern("*bar*joe").matches(stringToBytes("foorbarbazjoey"))).isFalse();
  }

  @Test
  public void caseSensitive() {
    assertThat(createPattern("foo****").matches(stringToBytes("Foo"))).isFalse();
    assertThat(createPattern("foo****").matches(stringToBytes("foo"))).isTrue();
    assertThat(createPattern("[a-z]oo").matches(stringToBytes("Foo"))).isFalse();
    assertThat(createPattern("[a-z]oo").matches(stringToBytes("goo"))).isTrue();
  }

  @Test
  public void escapeMakesSpecialsLiteral() {
    assertThat(createPattern("b\\*r").matches(stringToBytes("b*r"))).isTrue();
    assertThat(createPattern("b\\*r").matches(stringToBytes("bar"))).isFalse();
    assertThat(createPattern("b\\").matches(stringToBytes("b\\"))).isTrue();
  }

  @Test
  public void charSets() {
    assertThat(createPattern("[^a-z]").matches(stringToBytes("A"))).isTrue();
    assertThat(createPattern("[z-a]").matches(stringToBytes("z"))).isTrue();
    assertThat(createPattern("[z-a]").matches(stringToBytes("a"))).isTrue();
    assertThat(createPattern("[z-a]").matches(stringToBytes("m"))).isTrue();
    assertThat(createPattern("[^a-a]").matches(stringToBytes("a"))).isFalse();
    assertThat(createPattern("[a-a]").matches(stringToBytes("a"))).isTrue();
    assertThat(createPattern("[a-a]").matches(stringToBytes("b"))).isFalse();
    assertThat(createPattern("[a\\-z]").matches(stringToBytes("-"))).isTrue();
    assertThat(createPattern("[a").matches(stringToBytes("a"))).isTrue();
  }
}
