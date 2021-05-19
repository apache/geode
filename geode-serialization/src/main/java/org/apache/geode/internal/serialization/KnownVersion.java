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

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.geode.annotations.Immutable;

/**
 * Enumerated type for client / server and p2p version.
 *
 * There are dependencies in versioning code that require newer versions to have ordinals higher
 * than older versions in order to protect against deserialization problems. Client/server code also
 * uses greater-than comparison of ordinals in backward-compatibility checks.
 *
 * @since GemFire 5.7
 */
@Immutable
public class KnownVersion extends AbstractVersion {

  /** The name of this version */
  private final transient String name;

  /** The product name of this version */
  private final transient String productName;

  /** The suffix to use in toDataPre / fromDataPre method names */
  private final transient String methodSuffix;

  // the major, minor and release bits of the release
  private final byte major;
  private final byte minor;
  private final byte release;
  private final byte patch;
  private final boolean modifiesClientServerProtocol;

  public static final int HIGHEST_VERSION = 150;

  @Immutable
  private static final KnownVersion[] VALUES = new KnownVersion[HIGHEST_VERSION + 1];

  /**
   * Reserved token that cannot be used for product version but as a flag in internal contexts.
   */
  static final byte TOKEN_ORDINAL = -1;
  static final int TOKEN_ORDINAL_INT = (TOKEN_ORDINAL & 0xFF);

  @Immutable
  public static final KnownVersion TOKEN =
      new KnownVersion("", "TOKEN", (byte) -1, (byte) 0, (byte) 0, (byte) 0, TOKEN_ORDINAL, true);

  private static final short GFE_7099_ORDINAL = 21;

  @Immutable
  @Deprecated
  public static final KnownVersion GFE_7099 =
      new KnownVersion("GFE", "7.0.99", (byte) 7, (byte) 0, (byte) 99, (byte) 0, GFE_7099_ORDINAL,
          true);

  private static final short GFE_71_ORDINAL = 22;

  @Immutable
  @Deprecated
  public static final KnownVersion GFE_71 =
      new KnownVersion("GFE", "7.1", (byte) 7, (byte) 1, (byte) 0, (byte) 0, GFE_71_ORDINAL, true);

  // 23-29 available for 7.x variants

  private static final short GFE_80_ORDINAL = 30;

  @Immutable
  @Deprecated
  public static final KnownVersion GFE_80 =
      new KnownVersion("GFE", "8.0", (byte) 8, (byte) 0, (byte) 0, (byte) 0, GFE_80_ORDINAL, true);

  // 31-34 available for 8.0.x variants

  private static final short GFE_8009_ORDINAL = 31;

  @Immutable
  @Deprecated
  public static final KnownVersion GFE_8009 =
      new KnownVersion("GFE", "8.0.0.9", (byte) 8, (byte) 0, (byte) 0, (byte) 9, GFE_8009_ORDINAL,
          true);

  private static final short GFE_81_ORDINAL = 35;

  @Immutable
  public static final KnownVersion GFE_81 =
      new KnownVersion("GFE", "8.1", (byte) 8, (byte) 1, (byte) 0, (byte) 0, GFE_81_ORDINAL, true);

  // 36-39 available for 8.1.x variants

  // 40-44 was reserved for 8.2.x but was never used in any release.

  private static final short GFE_90_ORDINAL = 45; // this is also GEODE 1.0.0-incubating

  @Immutable
  public static final KnownVersion GFE_90 =
      new KnownVersion("GFE", "9.0", (byte) 9, (byte) 0, (byte) 0, (byte) 0, GFE_90_ORDINAL, true);

  // prior to v1.2.0 GEODE_1_1_0 was named GFE_91. This was used for both the rel/v1.1.0
  // and rel/v1.1.1 releases
  private static final short GEODE_1_1_0_ORDINAL = 50;

  @Immutable
  public static final KnownVersion GEODE_1_1_0 =
      new KnownVersion("GEODE", "1.1.0", (byte) 1, (byte) 1, (byte) 0, (byte) 0,
          GEODE_1_1_0_ORDINAL, true);

  // This ordinal was never used
  private static final short GEODE_1_1_1_ORDINAL = 55;

  @Immutable
  public static final KnownVersion GEODE_1_1_1 =
      new KnownVersion("GEODE", "1.1.1", (byte) 1, (byte) 1, (byte) 1, (byte) 0,
          GEODE_1_1_1_ORDINAL, true);

  private static final short GEODE_1_2_0_ORDINAL = 65;

  @Immutable
  public static final KnownVersion GEODE_1_2_0 =
      new KnownVersion("GEODE", "1.2.0", (byte) 1, (byte) 2, (byte) 0, (byte) 0,
          GEODE_1_2_0_ORDINAL, true);

  private static final short GEODE_1_3_0_ORDINAL = 70;

  @Immutable
  public static final KnownVersion GEODE_1_3_0 =
      new KnownVersion("GEODE", "1.3.0", (byte) 1, (byte) 3, (byte) 0, (byte) 0,
          GEODE_1_3_0_ORDINAL, true);

  private static final short GEODE_1_4_0_ORDINAL = 75;

  @Immutable
  public static final KnownVersion GEODE_1_4_0 =
      new KnownVersion("GEODE", "1.4.0", (byte) 1, (byte) 4, (byte) 0, (byte) 0,
          GEODE_1_4_0_ORDINAL, true);

  private static final short GEODE_1_5_0_ORDINAL = 80;

  @Immutable
  public static final KnownVersion GEODE_1_5_0 =
      new KnownVersion("GEODE", "1.5.0", (byte) 1, (byte) 5, (byte) 0, (byte) 0,
          GEODE_1_5_0_ORDINAL, true);

  private static final short GEODE_1_6_0_ORDINAL = 85;

  @Immutable
  public static final KnownVersion GEODE_1_6_0 =
      new KnownVersion("GEODE", "1.6.0", (byte) 1, (byte) 6, (byte) 0, (byte) 0,
          GEODE_1_6_0_ORDINAL, true);

  private static final short GEODE_1_7_0_ORDINAL = 90;

  @Immutable
  public static final KnownVersion GEODE_1_7_0 =
      new KnownVersion("GEODE", "1.7.0", (byte) 1, (byte) 7, (byte) 0, (byte) 0,
          GEODE_1_7_0_ORDINAL, true);

  private static final short GEODE_1_8_0_ORDINAL = 95;

  @Immutable
  public static final KnownVersion GEODE_1_8_0 =
      new KnownVersion("GEODE", "1.8.0", (byte) 1, (byte) 8, (byte) 0, (byte) 0,
          GEODE_1_8_0_ORDINAL, true);

  private static final short GEODE_1_9_0_ORDINAL = 100;

  @Immutable
  public static final KnownVersion GEODE_1_9_0 =
      new KnownVersion("GEODE", "1.9.0", (byte) 1, (byte) 9, (byte) 0, (byte) 0,
          GEODE_1_9_0_ORDINAL, true);


  private static final byte GEODE_1_10_0_ORDINAL = 105;

  @Immutable
  public static final KnownVersion GEODE_1_10_0 =
      new KnownVersion("GEODE", "1.10.0", (byte) 1, (byte) 10, (byte) 0, (byte) 0,
          GEODE_1_10_0_ORDINAL, true);

  private static final short GEODE_1_11_0_ORDINAL = 110;

  @Immutable
  public static final KnownVersion GEODE_1_11_0 =
      new KnownVersion("GEODE", "1.11.0", (byte) 1, (byte) 11, (byte) 0, (byte) 0,
          GEODE_1_11_0_ORDINAL, true);

  private static final short GEODE_1_12_0_ORDINAL = 115;

  @Immutable
  public static final KnownVersion GEODE_1_12_0 =
      new KnownVersion("GEODE", "1.12.0", (byte) 1, (byte) 12, (byte) 0, (byte) 0,
          GEODE_1_12_0_ORDINAL, true);

  private static final short GEODE_1_12_1_ORDINAL = 116;

  @Immutable
  public static final KnownVersion GEODE_1_12_1 =
      new KnownVersion("GEODE", "1.12.1", (byte) 1, (byte) 12, (byte) 1, (byte) 0,
          GEODE_1_12_1_ORDINAL, true);

  private static final short GEODE_1_13_0_ORDINAL = 120;

  @Immutable
  public static final KnownVersion GEODE_1_13_0 =
      new KnownVersion("GEODE", "1.13.0", (byte) 1, (byte) 13, (byte) 0, (byte) 0,
          GEODE_1_13_0_ORDINAL, true);

  private static final short GEODE_1_13_1_ORDINAL = 121;

  @Immutable
  public static final KnownVersion GEODE_1_13_1 =
      new KnownVersion("GEODE", "1.13.1", (byte) 1, (byte) 13, (byte) 1, (byte) 0,
          GEODE_1_13_1_ORDINAL, true);

  private static final short GEODE_1_14_0_ORDINAL = 125;

  @Immutable
  public static final KnownVersion GEODE_1_14_0 =
      new KnownVersion("GEODE", "1.14.0", (byte) 1, (byte) 14, (byte) 0, (byte) 0,
          GEODE_1_14_0_ORDINAL, true);

  private static final short GEODE_1_15_0_ORDINAL = 150;

  @Immutable
  public static final KnownVersion GEODE_1_15_0 =
      new KnownVersion("GEODE", "1.15.0", (byte) 1, (byte) 15, (byte) 0, (byte) 0,
          GEODE_1_15_0_ORDINAL);

  /* NOTE: when adding a new version bump the ordinal by 10. Ordinals can be short ints */

  /**
   * The oldest non-deprecated version supported.
   */
  @Immutable
  public static final KnownVersion OLDEST = GFE_81;

  /**
   * This constant must be set to the most current version of the product. !!! NOTE: update
   * HIGHEST_VERSION when changing CURRENT !!!
   */
  @Immutable
  public static final KnownVersion CURRENT = GEODE_1_15_0;

  /**
   * A lot of versioning code needs access to the current version's ordinal
   */
  @Immutable
  public static final short CURRENT_ORDINAL = CURRENT.ordinal();

  /**
   * an invalid KnownVersion used for tests and instead of null values
   */
  private static final short validOrdinalForTesting = 2;

  @Immutable
  public static final KnownVersion TEST_VERSION =
      new KnownVersion("TEST", "VERSION", (byte) 0, (byte) 0,
          (byte) 0, (byte) 0, validOrdinalForTesting, true);

  private KnownVersion(String productName, String name, byte major, byte minor, byte release,
      byte patch, short ordinal) {
    this(productName, name, major, minor, release, patch, ordinal, false);
  }

  private KnownVersion(String productName, String name, byte major, byte minor, byte release,
      byte patch, short ordinal, boolean modifiesClientServerProtocol) {
    super(ordinal);
    this.productName = productName;
    this.name = name;
    this.major = major;
    this.minor = minor;
    this.release = release;
    this.patch = patch;
    this.modifiesClientServerProtocol = modifiesClientServerProtocol;

    methodSuffix = this.productName + "_" + this.major + "_" + this.minor + "_" + this.release + "_"
        + this.patch;

    if (ordinal != TOKEN_ORDINAL) {
      VALUES[ordinal] = this;
    }
  }

  public static KnownVersion getCurrentVersion() {
    return CURRENT;
  }

  public KnownVersion getClientServerProtocolVersion() {
    for (short i = ordinal(); i >= 0; i--) {
      if (VALUES[i] != null && VALUES[i].modifiesClientServerProtocol) {
        return VALUES[i];
      }
    }
    throw new IllegalStateException("There is no valid clientServerProtocolVersion for " + this);
  }

  public static String unsupportedVersionMessage(final short ordinal) {
    return String.format(
        "Peer or client version with ordinal %s not supported. Highest known version is %s",
        ordinal, CURRENT.name);
  }

  public String getMethodSuffix() {
    return methodSuffix;
  }

  public String getName() {
    return name;
  }

  public short getMajor() {
    return major;
  }

  public short getMinor() {
    return minor;
  }

  public short getRelease() {
    return release;
  }

  public short getPatch() {
    return patch;
  }

  @Override
  public String toString() {
    return productName + " " + name;
  }

  public static Iterable<? extends KnownVersion> getAllVersions() {
    return Arrays.stream(VALUES).filter(x -> x != null && x != TEST_VERSION)
        .collect(Collectors.toList());
  }

  /**
   * @return KnownVersion for ordinal if known, otherwise null.
   */
  public static KnownVersion getKnownVersion(final short ordinal) {
    if (ordinal == TOKEN_ORDINAL) {
      return TOKEN;
    }
    if (ordinal < TOKEN_ORDINAL || ordinal >= VALUES.length) {
      return null;
    }
    return VALUES[ordinal];
  }

  public boolean hasClientServerProtocolChange() {
    return modifiesClientServerProtocol;
  }
}
