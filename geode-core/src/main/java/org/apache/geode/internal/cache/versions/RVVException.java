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
package org.apache.geode.internal.cache.versions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataSerializable;

/**
 * RVV exceptions are part of a RegionVersionVector. They are held by RegionVersionHolders.
 * <p>
 *
 * An RVV exception represents a missing range of versions from a member. This is represented as a
 * pair of version numbers: the last version we've received from the member and the next version
 * we've received from the member. The span of versions between (but not including) these two
 * numbers are the versions that are missing.
 * <p>
 *
 * RVVException also tracks the scattering of versions we've received that fall within the range of
 * the exception. As these missing versions are received the RVVException coalesces the range. If
 * isFilled() returns true after adding a missing version to the RVVException, the hole has been
 * filled and the RVVException is no longer needed.
 * <p>
 *
 * New exceptions are common to see as operations arrive from parallel threads operating on the same
 * region. These exceptions are soon mended and disappear.
 * <p>
 *
 * Old exceptions indicate that we missed something that we are unlikely to receive without asking
 * for it.
 * <p>
 *
 * RVVException currently depends on external synchronization. Typically the RegionVersionHolder
 * that holds the exception is locked while accessing its RVVExceptions. This is what is done in
 * RegionVersionVector.
 *
 */
abstract class RVVException
    implements Comparable<RVVException>, Cloneable, VersionedDataSerializable {
  private static final long serialVersionUID = 2021977010704105114L;

  protected static boolean UseTreeSetsForTesting = false;

  /**
   * The maximum version span that can be represented by a bitset RVVException. If the span is
   * greater than this we use the version of RVVException that collects "received versions" in a
   * regular collection.
   */
  protected static final long RVV_MAX_BITSET_SPAN = 128 * 8; // 128 bytes gives a span of 1k
                                                             // versions

  long previousVersion;
  long nextVersion;


  static RVVException createException(long previousVersion, long nextVersion) {
    return createException(previousVersion, nextVersion, 0);
  }

  /** Use this method to create a new RVVException */
  static RVVException createException(long previousVersion, long nextVersion,
      long initialExceptionCount) {
    // arbitrary cutoff of 100 bytes to use a treeSet instead of bitSet
    // But if we are deserializing an exception too many received versions use a
    // bitset anyway.
    long delta = nextVersion - previousVersion;
    if (UseTreeSetsForTesting
        || (delta > RVV_MAX_BITSET_SPAN && initialExceptionCount * 512 < delta)) {
      return new RVVExceptionT(previousVersion, nextVersion);
    }
    return new RVVExceptionB(previousVersion, nextVersion);
  }

  RVVException(long previousVersion, long nextVersion) {
    this.previousVersion = previousVersion;
    this.nextVersion = nextVersion;
  }



  /**
   * RegionVersionHolder.fromData() calls this to create an exception
   */
  static RVVException createException(DataInput in) throws IOException {
    long previousVersion = InternalDataSerializer.readUnsignedVL(in);
    int size = (int) InternalDataSerializer.readUnsignedVL(in);
    long last = previousVersion;
    long[] versions = new long[(int) size];
    for (int i = 0; i < size; i++) {
      long delta = InternalDataSerializer.readUnsignedVL(in);
      long value = delta + last;
      versions[i] = value;
      last = value;
    }
    long delta = InternalDataSerializer.readUnsignedVL(in);
    long nextVersion = last + delta;
    RVVException result = createException(previousVersion, nextVersion, size);

    for (int i = 0; i < size; i++) {
      result.addReceived(versions[i]);
    }
    return result;
  }


  /** has the given version been recorded as having been received? */
  public abstract boolean contains(long version);

  /** return false if any revisions have been recorded in the range of this exception */
  public abstract boolean isEmpty();

  /** internal method to add a new version to the received-versions collection */
  protected abstract void addReceived(long version);

  public void toData(DataOutput out) throws IOException {
    InternalDataSerializer.writeUnsignedVL(this.previousVersion, out);
    writeReceived(out);
  }

  /**
   * add a received version
   */
  public abstract void add(long receivedVersion);

  /**
   * returns true if the missing versions that this exception represents have all been received
   */
  public boolean isFilled() {
    return this.previousVersion + 1 >= this.nextVersion;
  }

  public abstract RVVException clone();

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(RVVException o) {
    long thisVal = this.previousVersion;
    long anotherVal = o.previousVersion;
    return (thisVal < anotherVal ? -1 : (thisVal == anotherVal ? 0 : 1));
  }

  /** Test hook - compare two exceptions */
  public boolean sameAs(RVVException other) {
    return (this.previousVersion == other.previousVersion)
        && (this.nextVersion == other.nextVersion);
  }

  @Override
  public boolean equals(Object obj) {
    throw new UnsupportedOperationException("change the test to use sameAs");
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException("this class does not support hashing at this time");
  }

  protected abstract void writeReceived(DataOutput out) throws IOException;

  public abstract ReceivedVersionsReverseIterator receivedVersionsReverseIterator();

  public abstract long getHighestReceivedVersion();

  public abstract class ReceivedVersionsReverseIterator {

    abstract boolean hasNext();

    abstract long next();

    abstract void remove();

  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {}

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  public boolean shouldChangeForm() {
    return false;
  }

  public RVVException changeForm() {
    throw new IllegalStateException("Should not be called");
  }
}
