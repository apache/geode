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

package org.apache.geode.internal.serialization;

import static org.assertj.core.api.Assertions.assertThat;

import junit.framework.TestCase;
import org.junit.Test;

public class UnknownVersionJUnitTest extends TestCase {

  @Test
  public void testEqualMinSameIdentity() {
    final Version versionOrdinal = construct(Short.MIN_VALUE);
    validateEqual(versionOrdinal, versionOrdinal);
  }

  @Test
  public void testEqualMinDifferentIdentity() {
    validateEqual(construct(Short.MIN_VALUE), construct(Short.MIN_VALUE));
  }

  @Test
  public void testEqualMaxSameIdentity() {
    final UnknownVersion versionOrdinal = construct(Short.MAX_VALUE);
    validateEqual(versionOrdinal, versionOrdinal);
  }

  @Test
  public void testEqualMaxDifferentIdentity() {
    validateEqual(construct(Short.MAX_VALUE), construct(Short.MAX_VALUE));
  }

  @Test
  public void testUnequalVersionOrdinals() {
    validateUnequal((short) 7, (short) 8);
  }

  @Test
  public void testUnequalVersionOrdinalsLimits() {
    validateUnequal(Short.MIN_VALUE, Short.MAX_VALUE);
  }

  @Test
  public void testHashMin() {
    validateHash(Short.MIN_VALUE);
  }

  @Test
  public void testHashMax() {
    validateHash(Short.MAX_VALUE);
  }

  @Test
  public void testStringMin() {
    assertThat(construct(Short.MIN_VALUE).toString())
        .isEqualTo("UnknownVersion[ordinal=-32768]");
  }

  @Test
  public void testStringMax() {
    assertThat(construct(Short.MAX_VALUE).toString())
        .isEqualTo("UnknownVersion[ordinal=32767]");
  }

  @Test
  public void testCompareToNull() {
    assertThat(construct((short) 3).compareTo(null)).isEqualTo(1);
  }

  @Test
  public void testEqualsIncompatible() {
    assertThat(construct((short) 6).equals("howdy!")).isFalse();
  }

  private void validateEqual(final Version a, final Version b) {
    assertThat(a.compareTo(b)).isEqualTo(0);
    assertThat(a.equals(b)).isTrue();
    assertThat(a.isNewerThan(b)).isFalse();
    assertThat(a.isNotNewerThan(b)).isTrue();
    assertThat(a.isOlderThan(b)).isFalse();
    assertThat(a.isNotOlderThan(b)).isTrue();
  }

  private void validateUnequal(final short smallerShort, final short largerShort) {
    final Version smaller = construct(smallerShort);
    final Version larger = construct(largerShort);

    assertThat(smaller.compareTo(larger)).isLessThan(0);
    assertThat(smaller.equals(larger)).isFalse();
    assertThat(smaller.isNewerThan(larger)).isFalse();
    assertThat(smaller.isOlderThan(larger)).isTrue();

    assertThat(larger.compareTo(smaller)).isGreaterThan(0);
    assertThat(larger.equals(smaller)).isFalse();
    assertThat(larger.isNewerThan(smaller)).isTrue();
    assertThat(larger.isOlderThan(smaller)).isFalse();

    // now test the boolean inverses

    assertThat(smaller.isNewerThan(larger)).isNotEqualTo(smaller.isNotNewerThan(larger));
    assertThat(larger.isNewerThan(smaller)).isNotEqualTo(larger.isNotNewerThan(smaller));
    assertThat(smaller.isOlderThan(larger)).isNotEqualTo(smaller.isNotOlderThan(larger));
    assertThat(larger.isOlderThan(smaller)).isNotEqualTo(larger.isNotOlderThan(smaller));
  }

  private void validateHash(final short ordinal) {
    final Version a = construct(ordinal);
    final Version b = construct(ordinal);
    assertThat(a.equals(b)).isTrue();
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

  private UnknownVersion construct(final short minValue) {
    return new UnknownVersion(minValue);
  }

}
