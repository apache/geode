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
  private final byte majorVersion;
  private final byte minorVersion;
  private final byte release;
  private final byte patch;

  public static final int HIGHEST_VERSION = 125;

  @Immutable
  private static final KnownVersion[] VALUES = new KnownVersion[HIGHEST_VERSION + 1];

  /**
   * Reserved token that cannot be used for product version but as a flag in internal contexts.
   */
  static final byte TOKEN_ORDINAL = -1;
  static final int TOKEN_ORDINAL_INT = (TOKEN_ORDINAL & 0xFF);

  @Immutable
  public static final KnownVersion TOKEN =
      new KnownVersion("", "TOKEN", (byte) -1, (byte) 0, (byte) 0, (byte) 0, TOKEN_ORDINAL);

  private static final short GFE_56_ORDINAL = 0;

  @Immutable
  public static final KnownVersion GFE_56 =
      new KnownVersion("GFE", "5.6", (byte) 5, (byte) 6, (byte) 0, (byte) 0, GFE_56_ORDINAL);

  private static final short GFE_57_ORDINAL = 1;

  @Immutable
  public static final KnownVersion GFE_57 =
      new KnownVersion("GFE", "5.7", (byte) 5, (byte) 7, (byte) 0, (byte) 0, GFE_57_ORDINAL);

  private static final short GFE_58_ORDINAL = 3;

  @Immutable
  public static final KnownVersion GFE_58 =
      new KnownVersion("GFE", "5.8", (byte) 5, (byte) 8, (byte) 0, (byte) 0, GFE_58_ORDINAL);

  private static final short GFE_603_ORDINAL = 4;

  @Immutable
  public static final KnownVersion GFE_603 =
      new KnownVersion("GFE", "6.0.3", (byte) 6, (byte) 0, (byte) 3, (byte) 0, GFE_603_ORDINAL);

  private static final short GFE_61_ORDINAL = 5;

  @Immutable
  public static final KnownVersion GFE_61 =
      new KnownVersion("GFE", "6.1", (byte) 6, (byte) 1, (byte) 0, (byte) 0, GFE_61_ORDINAL);

  private static final short GFE_65_ORDINAL = 6;

  @Immutable
  public static final KnownVersion GFE_65 =
      new KnownVersion("GFE", "6.5", (byte) 6, (byte) 5, (byte) 0, (byte) 0, GFE_65_ORDINAL);

  private static final short GFE_651_ORDINAL = 7;

  @Immutable
  public static final KnownVersion GFE_651 =
      new KnownVersion("GFE", "6.5.1", (byte) 6, (byte) 5, (byte) 1, (byte) 0, GFE_651_ORDINAL);

  private static final short GFE_6516_ORDINAL = 12;

  @Immutable
  public static final KnownVersion GFE_6516 =
      new KnownVersion("GFE", "6.5.1.6", (byte) 6, (byte) 5, (byte) 1, (byte) 6, GFE_6516_ORDINAL);

  private static final short GFE_66_ORDINAL = 16;

  @Immutable
  public static final KnownVersion GFE_66 =
      new KnownVersion("GFE", "6.6", (byte) 6, (byte) 6, (byte) 0, (byte) 0, GFE_66_ORDINAL);

  private static final short GFE_662_ORDINAL = 17;

  @Immutable
  public static final KnownVersion GFE_662 =
      new KnownVersion("GFE", "6.6.2", (byte) 6, (byte) 6, (byte) 2, (byte) 0, GFE_662_ORDINAL);

  private static final short GFE_6622_ORDINAL = 18;

  @Immutable
  public static final KnownVersion GFE_6622 =
      new KnownVersion("GFE", "6.6.2.2", (byte) 6, (byte) 6, (byte) 2, (byte) 2, GFE_6622_ORDINAL);

  private static final short GFE_70_ORDINAL = 19;

  @Immutable
  public static final KnownVersion GFE_70 =
      new KnownVersion("GFE", "7.0", (byte) 7, (byte) 0, (byte) 0, (byte) 0, GFE_70_ORDINAL);

  private static final short GFE_701_ORDINAL = 20;

  @Immutable
  public static final KnownVersion GFE_701 =
      new KnownVersion("GFE", "7.0.1", (byte) 7, (byte) 0, (byte) 1, (byte) 0, GFE_701_ORDINAL);

  private static final short GFE_7099_ORDINAL = 21;

  @Immutable
  public static final KnownVersion GFE_7099 =
      new KnownVersion("GFE", "7.0.99", (byte) 7, (byte) 0, (byte) 99, (byte) 0, GFE_7099_ORDINAL);

  private static final short GFE_71_ORDINAL = 22;

  @Immutable
  public static final KnownVersion GFE_71 =
      new KnownVersion("GFE", "7.1", (byte) 7, (byte) 1, (byte) 0, (byte) 0, GFE_71_ORDINAL);

  // 23-29 available for 7.x variants

  private static final short GFE_80_ORDINAL = 30;

  @Immutable
  public static final KnownVersion GFE_80 =
      new KnownVersion("GFE", "8.0", (byte) 8, (byte) 0, (byte) 0, (byte) 0, GFE_80_ORDINAL);

  // 31-34 available for 8.0.x variants

  private static final short GFE_8009_ORDINAL = 31;

  @Immutable
  public static final KnownVersion GFE_8009 =
      new KnownVersion("GFE", "8.0.0.9", (byte) 8, (byte) 0, (byte) 0, (byte) 9, GFE_8009_ORDINAL);

  private static final short GFE_81_ORDINAL = 35;

  @Immutable
  public static final KnownVersion GFE_81 =
      new KnownVersion("GFE", "8.1", (byte) 8, (byte) 1, (byte) 0, (byte) 0, GFE_81_ORDINAL);

  // 36-39 available for 8.1.x variants

  private static final short GFE_82_ORDINAL = 40;

  @Immutable
  public static final KnownVersion GFE_82 =
      new KnownVersion("GFE", "8.2", (byte) 8, (byte) 2, (byte) 0, (byte) 0, GFE_82_ORDINAL);

  // 41-44 available for 8.2.x variants

  private static final short GFE_90_ORDINAL = 45; // this is also GEODE 1.0.0-incubating

  @Immutable
  public static final KnownVersion GFE_90 =
      new KnownVersion("GFE", "9.0", (byte) 9, (byte) 0, (byte) 0, (byte) 0, GFE_90_ORDINAL);

  // prior to v1.2.0 GEODE_1_1_0 was named GFE_91. This was used for both the rel/v1.1.0
  // and rel/v1.1.1 releases
  private static final short GEODE_1_1_0_ORDINAL = 50;

  @Immutable
  public static final KnownVersion GEODE_1_1_0 =
      new KnownVersion("GEODE", "1.1.0", (byte) 1, (byte) 1, (byte) 0, (byte) 0,
          GEODE_1_1_0_ORDINAL);

  // This ordinal was never used
  private static final short GEODE_1_1_1_ORDINAL = 55;

  @Immutable
  public static final KnownVersion GEODE_1_1_1 =
      new KnownVersion("GEODE", "1.1.1", (byte) 1, (byte) 1, (byte) 1, (byte) 0,
          GEODE_1_1_1_ORDINAL);

  private static final short GEODE_1_2_0_ORDINAL = 65;

  @Immutable
  public static final KnownVersion GEODE_1_2_0 =
      new KnownVersion("GEODE", "1.2.0", (byte) 1, (byte) 2, (byte) 0, (byte) 0,
          GEODE_1_2_0_ORDINAL);

  private static final short GEODE_1_3_0_ORDINAL = 70;

  @Immutable
  public static final KnownVersion GEODE_1_3_0 =
      new KnownVersion("GEODE", "1.3.0", (byte) 1, (byte) 3, (byte) 0, (byte) 0,
          GEODE_1_3_0_ORDINAL);

  private static final short GEODE_1_4_0_ORDINAL = 75;

  @Immutable
  public static final KnownVersion GEODE_1_4_0 =
      new KnownVersion("GEODE", "1.4.0", (byte) 1, (byte) 4, (byte) 0, (byte) 0,
          GEODE_1_4_0_ORDINAL);

  private static final short GEODE_1_5_0_ORDINAL = 80;

  @Immutable
  public static final KnownVersion GEODE_1_5_0 =
      new KnownVersion("GEODE", "1.5.0", (byte) 1, (byte) 5, (byte) 0, (byte) 0,
          GEODE_1_5_0_ORDINAL);

  private static final short GEODE_1_6_0_ORDINAL = 85;

  @Immutable
  public static final KnownVersion GEODE_1_6_0 =
      new KnownVersion("GEODE", "1.6.0", (byte) 1, (byte) 6, (byte) 0, (byte) 0,
          GEODE_1_6_0_ORDINAL);

  private static final short GEODE_1_7_0_ORDINAL = 90;

  @Immutable
  public static final KnownVersion GEODE_1_7_0 =
      new KnownVersion("GEODE", "1.7.0", (byte) 1, (byte) 7, (byte) 0, (byte) 0,
          GEODE_1_7_0_ORDINAL);

  private static final short GEODE_1_8_0_ORDINAL = 95;

  @Immutable
  public static final KnownVersion GEODE_1_8_0 =
      new KnownVersion("GEODE", "1.8.0", (byte) 1, (byte) 8, (byte) 0, (byte) 0,
          GEODE_1_8_0_ORDINAL);

  private static final short GEODE_1_9_0_ORDINAL = 100;

  @Immutable
  public static final KnownVersion GEODE_1_9_0 =
      new KnownVersion("GEODE", "1.9.0", (byte) 1, (byte) 9, (byte) 0, (byte) 0,
          GEODE_1_9_0_ORDINAL);


  private static final byte GEODE_1_10_0_ORDINAL = 105;

  @Immutable
  public static final KnownVersion GEODE_1_10_0 =
      new KnownVersion("GEODE", "1.10.0", (byte) 1, (byte) 10, (byte) 0, (byte) 0,
          GEODE_1_10_0_ORDINAL);

  private static final short GEODE_1_11_0_ORDINAL = 110;

  @Immutable
  public static final KnownVersion GEODE_1_11_0 =
      new KnownVersion("GEODE", "1.11.0", (byte) 1, (byte) 11, (byte) 0, (byte) 0,
          GEODE_1_11_0_ORDINAL);

  private static final short GEODE_1_12_0_ORDINAL = 115;

  @Immutable
  public static final KnownVersion GEODE_1_12_0 =
      new KnownVersion("GEODE", "1.12.0", (byte) 1, (byte) 12, (byte) 0, (byte) 0,
          GEODE_1_12_0_ORDINAL);

  private static final short GEODE_1_13_0_ORDINAL = 120;

  @Immutable
  public static final KnownVersion GEODE_1_13_0 =
      new KnownVersion("GEODE", "1.13.0", (byte) 1, (byte) 13, (byte) 0, (byte) 0,
          GEODE_1_13_0_ORDINAL);

  private static final short GEODE_1_14_0_ORDINAL = 125;

  @Immutable
  public static final KnownVersion GEODE_1_14_0 =
      new KnownVersion("GEODE", "1.14.0", (byte) 1, (byte) 14, (byte) 0, (byte) 0,
          GEODE_1_14_0_ORDINAL);

  /* NOTE: when adding a new version bump the ordinal by 5. Ordinals can be short ints */

  /**
   * This constant must be set to the most current version of the product. !!! NOTE: update
   * HIGHEST_VERSION when changing CURRENT !!!
   */
  @Immutable
  public static final KnownVersion CURRENT = GEODE_1_14_0;

  /**
   * A lot of versioning code needs access to the current version's ordinal
   */
  @Immutable
  public static final short CURRENT_ORDINAL = CURRENT.ordinal();

  /**
   * version ordinal for test Backward compatibility.
   */
  private static final short validOrdinalForTesting = 2;

  @Immutable
  public static final KnownVersion TEST_VERSION =
      new KnownVersion("TEST", "VERSION", (byte) 0, (byte) 0,
          (byte) 0, (byte) 0, validOrdinalForTesting);

  /** Creates a new instance of <code>Version</code> */
  private KnownVersion(String product, String name, byte major, byte minor, byte release,
      byte patch,
      short ordinal) {
    super(ordinal);
    this.productName = product;
    this.name = name;
    this.majorVersion = major;
    this.minorVersion = minor;
    this.release = release;
    this.patch = patch;
    this.methodSuffix = this.productName + "_" + this.majorVersion + "_" + this.minorVersion + "_"
        + this.release + "_" + this.patch;
    if (ordinal != TOKEN_ORDINAL) {
      VALUES[ordinal()] = this;
    }
  }

  public static KnownVersion getCurrentVersion() {
    return CURRENT;
  }

  public static String unsupportedVersionMessage(final short ordinal) {
    return String.format(
        "Peer or client version with ordinal %s not supported. Highest known version is %s",
        ordinal, CURRENT.name);
  }

  public String getMethodSuffix() {
    return this.methodSuffix;
  }

  public String getName() {
    return this.name;
  }

  public short getMajorVersion() {
    return this.majorVersion;
  }

  public short getMinorVersion() {
    return this.minorVersion;
  }

  public short getRelease() {
    return this.release;
  }

  public short getPatch() {
    return this.patch;
  }

  /**
   * Returns a string representation for this <code>Version</code>.
   *
   * @return the name of this operation.
   */
  @Override
  public String toString() {
    return this.productName + " " + this.name;
  }

  public byte[] toBytes() {
    byte[] bytes = new byte[2];
    bytes[0] = (byte) (ordinal() >> 8);
    bytes[1] = (byte) ordinal();
    return bytes;
  }

  public static Iterable<? extends KnownVersion> getAllVersions() {
    return Arrays.asList(VALUES).stream().filter(x -> x != null && x != TEST_VERSION)
        .collect(Collectors.toList());
  }

  /**
   * package-protected for use by Versioning factory
   */
  static KnownVersion getKnownVersion(final short ordinal,
      final KnownVersion returnWhenUnknown) {
    if (ordinal == TOKEN_ORDINAL) {
      return TOKEN;
    }
    if (ordinal < TOKEN_ORDINAL || ordinal >= VALUES.length || VALUES[ordinal] == null) {
      return returnWhenUnknown;
    }
    return VALUES[ordinal];
  }

}
