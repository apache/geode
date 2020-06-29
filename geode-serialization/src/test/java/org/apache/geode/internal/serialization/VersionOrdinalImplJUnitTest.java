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

public class VersionOrdinalImplJUnitTest extends TestCase {

  @Test
  public void testEqualMinSameIdentity() {
    final VersionOrdinalImpl versionOrdinal = new VersionOrdinalImpl(Short.MIN_VALUE);
    validateEqual(versionOrdinal, versionOrdinal);
  }

  @Test
  public void testEqualMinDifferentIdentity() {
    validateEqual(new VersionOrdinalImpl(Short.MIN_VALUE), new VersionOrdinalImpl(Short.MIN_VALUE));
  }

  @Test
  public void testEqualMaxSameIdentity() {
    final VersionOrdinalImpl versionOrdinal = new VersionOrdinalImpl(Short.MAX_VALUE);
    validateEqual(versionOrdinal, versionOrdinal);
  }

  @Test
  public void testEqualMaxDifferentIdentity() {
    validateEqual(new VersionOrdinalImpl(Short.MAX_VALUE), new VersionOrdinalImpl(Short.MAX_VALUE));
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
    assertThat(new VersionOrdinalImpl(Short.MIN_VALUE).toString())
        .isEqualTo("VersionOrdinal[ordinal=-32768]");
  }

  @Test
  public void testStringMax() {
    assertThat(new VersionOrdinalImpl(Short.MAX_VALUE).toString())
        .isEqualTo("VersionOrdinal[ordinal=32767]");
  }

  @Test
  public void testCompareToNull() {
    assertThat(new VersionOrdinalImpl((short) 3).compareTo(null)).isEqualTo(1);
  }

  @Test
  public void testEqualsIncompatible() {
    assertThat(new VersionOrdinalImpl((short) 6).equals("howdy!")).isFalse();
  }

  private void validateEqual(final VersionOrdinal a, final VersionOrdinal b) {
    assertThat(a.compareTo(b)).isEqualTo(0);
    assertThat(a.equals(b)).isTrue();
    assertThat(a.isNewerThan(b)).isFalse();
    assertThat(a.isNotNewerThan(b)).isTrue();
    assertThat(a.isOlderThan(b)).isFalse();
    assertThat(a.isNotOlderThan(b)).isTrue();
  }

  private void validateUnequal(final short smallerShort, final short largerShort) {
    final VersionOrdinalImpl smaller = new VersionOrdinalImpl(smallerShort);
    final VersionOrdinalImpl larger = new VersionOrdinalImpl(largerShort);

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
    final VersionOrdinalImpl a = new VersionOrdinalImpl(ordinal);
    final VersionOrdinalImpl b = new VersionOrdinalImpl(ordinal);
    assertThat(a.equals(b)).isTrue();
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }

}
