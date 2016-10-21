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

package org.apache.geode.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.geode.cache.UnsupportedVersionException;
import org.apache.geode.internal.cache.tier.sockets.CommandInitializer;
import org.apache.geode.internal.i18n.LocalizedStrings;

/**
 * Enumerated type for client / server and p2p version.
 * 
 * There are dependencies in versioning code that require newer versions to have ordinals higher
 * than older versions in order to protect against deserialization problems. Client/server code also
 * uses greater-than comparison of ordinals in backward-compatibility checks.
 * 
 * @since GemFire 5.7
 */
public final class Version implements Comparable<Version> {

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

  /**
   * Set to non-null if the underlying GemFire version is different from product version
   */
  private Version gemfireVersion;

  /** byte used as ordinal to represent this <code>Version</code> */
  private final short ordinal;

  public static final int HIGHEST_VERSION = 45;

  private static final Version[] VALUES = new Version[HIGHEST_VERSION + 1];

  /**
   * Reserved token that cannot be used for product version but as a flag in internal contexts.
   */
  private static final byte TOKEN_ORDINAL = -1;
  private static final int TOKEN_ORDINAL_INT = (TOKEN_ORDINAL & 0xFF);

  public static final Version TOKEN =
      new Version("", "TOKEN", (byte) -1, (byte) 0, (byte) 0, (byte) 0, TOKEN_ORDINAL);

  private static final byte GFE_56_ORDINAL = 0;

  public static final Version GFE_56 =
      new Version("GFE", "5.6", (byte) 5, (byte) 6, (byte) 0, (byte) 0, GFE_56_ORDINAL);

  private static final byte GFE_57_ORDINAL = 1;

  public static final Version GFE_57 =
      new Version("GFE", "5.7", (byte) 5, (byte) 7, (byte) 0, (byte) 0, GFE_57_ORDINAL);

  private static final byte GFE_58_ORDINAL = 3;

  public static final Version GFE_58 =
      new Version("GFE", "5.8", (byte) 5, (byte) 8, (byte) 0, (byte) 0, GFE_58_ORDINAL);

  private static final byte GFE_603_ORDINAL = 4;

  public static final Version GFE_603 =
      new Version("GFE", "6.0.3", (byte) 6, (byte) 0, (byte) 3, (byte) 0, GFE_603_ORDINAL);

  private static final byte GFE_61_ORDINAL = 5;

  public static final Version GFE_61 =
      new Version("GFE", "6.1", (byte) 6, (byte) 1, (byte) 0, (byte) 0, GFE_61_ORDINAL);

  private static final byte GFE_65_ORDINAL = 6;

  public static final Version GFE_65 =
      new Version("GFE", "6.5", (byte) 6, (byte) 5, (byte) 0, (byte) 0, GFE_65_ORDINAL);

  private static final byte GFE_651_ORDINAL = 7;

  public static final Version GFE_651 =
      new Version("GFE", "6.5.1", (byte) 6, (byte) 5, (byte) 1, (byte) 0, GFE_651_ORDINAL);

  private static final byte GFE_6516_ORDINAL = 12;

  public static final Version GFE_6516 =
      new Version("GFE", "6.5.1.6", (byte) 6, (byte) 5, (byte) 1, (byte) 6, GFE_6516_ORDINAL);

  private static final byte GFE_66_ORDINAL = 16;

  public static final Version GFE_66 =
      new Version("GFE", "6.6", (byte) 6, (byte) 6, (byte) 0, (byte) 0, GFE_66_ORDINAL);

  private static final byte GFE_662_ORDINAL = 17;

  public static final Version GFE_662 =
      new Version("GFE", "6.6.2", (byte) 6, (byte) 6, (byte) 2, (byte) 0, GFE_662_ORDINAL);

  private static final byte GFE_6622_ORDINAL = 18;

  public static final Version GFE_6622 =
      new Version("GFE", "6.6.2.2", (byte) 6, (byte) 6, (byte) 2, (byte) 2, GFE_6622_ORDINAL);

  private static final byte GFE_70_ORDINAL = 19;

  public static final Version GFE_70 =
      new Version("GFE", "7.0", (byte) 7, (byte) 0, (byte) 0, (byte) 0, GFE_70_ORDINAL);

  private static final byte GFE_701_ORDINAL = 20;

  public static final Version GFE_701 =
      new Version("GFE", "7.0.1", (byte) 7, (byte) 0, (byte) 1, (byte) 0, GFE_701_ORDINAL);

  private static final byte GFE_7099_ORDINAL = 21;

  public static final Version GFE_7099 =
      new Version("GFE", "7.0.99", (byte) 7, (byte) 0, (byte) 99, (byte) 0, GFE_7099_ORDINAL);

  private static final byte GFE_71_ORDINAL = 22;

  public static final Version GFE_71 =
      new Version("GFE", "7.1", (byte) 7, (byte) 1, (byte) 0, (byte) 0, GFE_71_ORDINAL);

  // 23-29 available for 7.x variants

  private static final byte GFE_80_ORDINAL = 30;

  public static final Version GFE_80 =
      new Version("GFE", "8.0", (byte) 8, (byte) 0, (byte) 0, (byte) 0, GFE_80_ORDINAL);

  // 31-34 available for 8.0.x variants

  private static final byte GFE_8009_ORDINAL = 31;

  public static final Version GFE_8009 =
      new Version("GFE", "8.0.0.9", (byte) 8, (byte) 0, (byte) 0, (byte) 9, GFE_8009_ORDINAL);

  private static final byte GFE_81_ORDINAL = 35;

  public static final Version GFE_81 =
      new Version("GFE", "8.1", (byte) 8, (byte) 1, (byte) 0, (byte) 0, GFE_81_ORDINAL);

  // 36-39 available for 8.1.x variants

  private static final byte GFE_82_ORDINAL = 40;

  public static final Version GFE_82 =
      new Version("GFE", "8.2", (byte) 8, (byte) 2, (byte) 0, (byte) 0, GFE_82_ORDINAL);

  // 41-44 available for 8.2.x variants

  private static final byte GFE_90_ORDINAL = 45;

  public static final Version GFE_90 =
      new Version("GFE", "9.0", (byte) 9, (byte) 0, (byte) 0, (byte) 0, GFE_90_ORDINAL);

  /**
   * This constant must be set to the most current version of the product. !!! NOTE: update
   * HIGHEST_VERSION when changing CURRENT !!!
   */
  public static final Version CURRENT = GFE_90;

  /**
   * A lot of versioning code needs access to the current version's ordinal
   */
  public static final short CURRENT_ORDINAL = CURRENT.ordinal();

  public static final short NOT_SUPPORTED_ORDINAL = 59;

  /**
   * version ordinal for test Backward compatibility.
   */
  private static final byte validOrdinalForTesting = 2;

  public static final Version TEST_VERSION = new Version("TEST", "VERSION", (byte) 0, (byte) 0,
      (byte) 0, (byte) 0, validOrdinalForTesting);

  /** Creates a new instance of <code>Version</code> */
  private Version(String product, String name, byte major, byte minor, byte release, byte patch,
      byte ordinal) {
    this.productName = product;
    this.name = name;
    this.majorVersion = major;
    this.minorVersion = minor;
    this.release = release;
    this.patch = patch;
    this.ordinal = ordinal;
    this.methodSuffix = this.productName + "_" + this.majorVersion + "_" + this.minorVersion + "_"
        + this.release + "_" + this.patch;
    this.gemfireVersion = null;
    if (ordinal != TOKEN_ORDINAL) {
      VALUES[this.ordinal] = this;
    }
  }

  /**
   * Creates a new instance of <code>Version</code> with a different underlying GemFire version
   */
  private Version(String product, String name, byte major, byte minor, byte release, byte patch,
      byte ordinal, Version gemfireVersion) {
    this(product, name, major, minor, release, patch, ordinal);
    this.gemfireVersion = gemfireVersion;
  }

  /** Return the <code>Version</code> represented by specified ordinal */
  public static Version fromOrdinal(short ordinal, boolean forGFEClients)
      throws UnsupportedVersionException {
    // Un-version client(client's prior to release 5.7) doesn't send version
    // byte in the handshake. So the next byte in the handshake has value 59 and
    // is interpreted as version byte. We are not supporting version 59 to
    // distinguish version client from unversion. Please use version ordinal 60
    // after 58 if required.
    if (ordinal == NOT_SUPPORTED_ORDINAL) {
      throw new UnsupportedVersionException("Un-versioned clients are not supported. ");
    }
    if (ordinal == TOKEN_ORDINAL) {
      return TOKEN;
    }
    // for clients also check that there must be a commands object mapping
    // for processing
    if ((VALUES.length < ordinal + 1) || VALUES[ordinal] == null
        || (forGFEClients && CommandInitializer.getCommands(VALUES[ordinal]) == null)) {
      throw new UnsupportedVersionException(LocalizedStrings.Version_REMOTE_VERSION_NOT_SUPPORTED
          .toLocalizedString(ordinal, CURRENT.name));
    }
    return VALUES[ordinal];
  }

  /**
   * return the version corresponding to the given ordinal, or CURRENT if the ordinal isn't valid
   * 
   * @param ordinal
   * @return the corresponding ordinal
   */
  public static Version fromOrdinalOrCurrent(short ordinal) {
    if (ordinal == TOKEN_ORDINAL) {
      return TOKEN;
    }
    final Version version;
    if ((VALUES.length < ordinal + 1) || (version = VALUES[ordinal]) == null) {
      return CURRENT;
    }
    return version;
  }

  /**
   * Write the given ordinal (result of {@link #ordinal()}) to given {@link DataOutput}. This keeps
   * the serialization of ordinal compatible with previous versions writing a single byte to
   * DataOutput when possible, and a token with 2 bytes if it is large.
   * 
   * @param out the {@link DataOutput} to write the ordinal write to
   * @param ordinal the version to be written
   * @param compressed if true, then use single byte for ordinal < 128, and three bytes for beyond
   *        that, else always use three bytes where the first byte is {@link #TOKEN_ORDINAL}; former
   *        mode is useful for interoperatibility with previous versions while latter to use fixed
   *        size for writing version; typically former will be used for P2P/client-server
   *        communications while latter for persisting to disk; we use the token to ensure that
   *        {@link #readOrdinal(DataInput)} can deal with both compressed/uncompressed cases
   *        seemlessly
   */
  public static void writeOrdinal(DataOutput out, short ordinal, boolean compressed)
      throws IOException {
    if (compressed && ordinal <= Byte.MAX_VALUE) {
      out.writeByte(ordinal);
    } else {
      out.writeByte(TOKEN_ORDINAL);
      out.writeShort(ordinal);
    }
  }

  /**
   * Write this {@link Version}'s ordinal (result of {@link #ordinal()}) to given
   * {@link DataOutput}. This keeps the serialization of ordinal compatible with previous versions
   * writing a single byte to DataOutput when possible, and a token with 2 bytes if it is large.
   * 
   * @param out the {@link DataOutput} to write the ordinal write to
   * @param compressed if true, then use single byte for ordinal < 128, and three bytes for beyond
   *        that, else always use three bytes where the first byte is {@link #TOKEN_ORDINAL}; former
   *        mode is useful for interoperatibility with previous versions while latter to use fixed
   *        size for writing version; typically former will be used for P2P/client-server
   *        communications while latter for persisting to disk; we use the token to ensure that
   *        {@link #readOrdinal(DataInput)} can deal with both compressed/uncompressed cases
   *        seemlessly
   */
  public final void writeOrdinal(DataOutput out, boolean compressed) throws IOException {
    writeOrdinal(out, this.ordinal, compressed);
  }

  /**
   * Fixed number of bytes required for serializing this version when "compressed" flag is false in
   * {@link #writeOrdinal(DataOutput, boolean)}.
   */
  public static final int uncompressedSize() {
    return 3;
  }

  /**
   * Fixed number of bytes required for serializing this version when "compressed" flag is true in
   * {@link #writeOrdinal(DataOutput, boolean)}.
   */
  public final int compressedSize() {
    if (ordinal <= Byte.MAX_VALUE) {
      return 1;
    } else {
      return 3;
    }
  }

  /**
   * Write the given ordinal (result of {@link #ordinal()}) to given {@link ByteBuffer}. This keeps
   * the serialization of ordinal compatible with previous versions writing a single byte to
   * DataOutput when possible, and a token with 2 bytes if it is large.
   * 
   * @param buffer the {@link ByteBuffer} to write the ordinal write to
   * @param ordinal the version to be written
   * @param compressed if true, then use single byte for ordinal < 128, and three bytes for beyond
   *        that, else always use three bytes where the first byte is {@link #TOKEN_ORDINAL}
   */
  public static void writeOrdinal(ByteBuffer buffer, short ordinal, boolean compressed)
      throws IOException {
    if (compressed && ordinal <= Byte.MAX_VALUE) {
      buffer.put((byte) ordinal);
    } else {
      buffer.put(TOKEN_ORDINAL);
      buffer.putShort(ordinal);
    }
  }

  /**
   * Reads ordinal as written by {@link #writeOrdinal} from given {@link DataInput}.
   */
  public static short readOrdinal(DataInput in) throws IOException {
    final byte ordinal = in.readByte();
    if (ordinal != TOKEN_ORDINAL) {
      return ordinal;
    } else {
      return in.readShort();
    }
  }

  /**
   * Return the <code>Version</code> reading from given {@link DataInput} as serialized by
   * {@link #writeOrdinal(DataOutput, boolean)}.
   * 
   * If the incoming ordinal is greater than or equal to current ordinal then this will return null
   * or {@link #CURRENT} indicating that version is same as that of {@link #CURRENT} assuming that
   * peer will support this JVM.
   * 
   * This method is not meant to be used for client-server protocol since servers cannot support
   * higher version clients, rather is only meant for P2P/JGroups messaging where a mixed version of
   * servers can be running at the same time. Similarly cannot be used when recovering from disk
   * since higher version data cannot be read.
   * 
   * @param in the {@link DataInput} to read the version from
   * @param returnNullForCurrent if true then return null if incoming version >= {@link #CURRENT}
   *        else return {@link #CURRENT}
   */
  public static Version readVersion(DataInput in, boolean returnNullForCurrent) throws IOException {
    return fromOrdinalNoThrow(readOrdinal(in), returnNullForCurrent);
  }

  /**
   * Return the <code>Version</code> represented by specified ordinal while not throwing exception
   * if given ordinal is higher than any known ones or does not map to an actual Version instance
   * due to gaps in the version ordinal sequence.
   */
  public static Version fromOrdinalNoThrow(short ordinal, boolean returnNullForCurrent) {
    if (ordinal == TOKEN_ORDINAL) {
      return TOKEN;
    }
    if (ordinal >= VALUES.length || VALUES[ordinal] == null) {
      return returnNullForCurrent ? null : CURRENT;
    }
    return VALUES[ordinal];
  }

  /**
   * Reads ordinal as written by {@link #writeOrdinal} from given {@link InputStream}. Returns -1 on
   * end of stream.
   */
  public static short readOrdinalFromInputStream(InputStream is) throws IOException {
    final int ordinal = is.read();
    if (ordinal != -1) {
      if (ordinal != TOKEN_ORDINAL_INT) {
        return (short) ordinal;
      } else {
        // two byte ordinal
        final int ordinalPart1 = is.read();
        final int ordinalPart2 = is.read();
        if ((ordinalPart1 | ordinalPart2) >= 0) {
          return (short) ((ordinalPart1 << 8) | ordinalPart2);
        } else {
          return -1;
        }
      }
    } else {
      return -1;
    }
  }

  public Version getGemFireVersion() {
    return this.gemfireVersion != null ? this.gemfireVersion : this;
  }

  public final String getMethodSuffix() {
    return this.methodSuffix;
  }

  public final String getProductName() {
    return this.productName;
  }

  public final String getName() {
    return this.name;
  }

  public final short getMajorVersion() {
    return this.majorVersion;
  }

  public final short getMinorVersion() {
    return this.minorVersion;
  }

  public final short getRelease() {
    return this.release;
  }

  public final short getPatch() {
    return this.patch;
  }

  public final short ordinal() {
    return this.ordinal;
  }

  /**
   * Returns whether this <code>Version</code> is compatible with the input <code>Version</code>
   * 
   * @param version The <code>Version</code> to compare
   * @return whether this <code>Version</code> is compatible with the input <code>Version</code>
   */
  public boolean compatibleWith(Version version) {
    return true;
  }

  /**
   * Finds the Version instance corresponding to the given ordinal and returns the result of
   * compareTo(Version)
   * 
   * @param other the ordinal of the other Version object
   * @return negative if this version is older, positive if this version is newer, 0 if this is the
   *         same version
   */
  public final int compareTo(short other) {
    // first try to find the actual Version object
    Version v = fromOrdinalNoThrow(other, false);
    if (v == null) {
      // failing that we use the old method of comparing Versions:
      return this.ordinal() - other;
    }
    return compareTo(v);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int compareTo(Version other) {
    if (other != null) {
      // [bruce] old implementation used ordinals for comparison, but this requires
      // ordinals to be in increasing order, which may not always be possible
      // // byte min/max can't overflow int, so use (a-b)
      // final int thisOrdinal = this.ordinal;
      // final int otherOrdinal = o.ordinal;
      // return (thisOrdinal - otherOrdinal);
      // [bruce] new implementation uses major/minor/patch/build
      if (this.majorVersion > other.majorVersion) {
        return 1;
      } else if (other.majorVersion > this.majorVersion) {
        return -1;
      }
      if (this.minorVersion > other.minorVersion) {
        return 1;
      } else if (other.minorVersion > this.minorVersion) {
        return -1;
      }
      if (this.release > other.release) {
        return 1;
      } else if (other.release > this.release) {
        return -1;
      }
      if (this.patch > other.patch) {
        return 1;
      } else if (other.patch > this.patch) {
        return -1;
      }
      return 0;
    } else {
      return 1;
    }
  }

  /**
   * Returns a string representation for this <code>Version</code>.
   * 
   * @return the name of this operation.
   */
  @Override
  public String toString() {
    if (this.gemfireVersion == null) {
      return this.productName + " " + this.name;
    } else {
      return this.productName + " " + this.name + '[' + this.gemfireVersion.toString() + ']';
    }
  }

  public static String toString(short ordinal) {
    if (ordinal <= CURRENT.ordinal) {
      try {
        return fromOrdinal(ordinal, false).toString();
      } catch (UnsupportedVersionException uve) {
        // ignored in toString()
      }
    }
    return "UNKNOWN[ordinal=" + ordinal + ']';
  }

  @Override
  public boolean equals(Object other) {
    if (other == this)
      return true;
    if (other != null && other.getClass() == Version.class) {
      return this.ordinal == ((Version) other).ordinal;
    } else {
      return false;
    }
  }

  public boolean equals(Version other) {
    return other != null && this.ordinal == other.ordinal;
  }

  @Override
  public int hashCode() {
    int result = 17;
    final int mult = 37;
    result = mult * result + this.ordinal;
    return result;
  }

  public byte[] toBytes() {
    byte[] bytes = new byte[2];
    bytes[0] = (byte) (ordinal >> 8);
    bytes[1] = (byte) ordinal;
    return bytes;
  }

}
