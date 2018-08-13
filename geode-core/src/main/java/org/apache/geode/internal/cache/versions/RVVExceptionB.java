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

import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;
import java.util.NoSuchElementException;

import org.apache.geode.internal.InternalDataSerializer;

public class RVVExceptionB extends RVVException {

  /**
   * received represents individual received versions that fall within this exception. position 0
   * corresponds to version receivedBaseVersion.
   */
  BitSet received;
  private long receivedBaseVersion;

  public RVVExceptionB(long previousVersion, long nextVersion) {
    super(previousVersion, nextVersion);
  }


  /**
   * add a received version
   */
  public void add(long receivedVersion) {
    // String me = this.toString();
    // long oldv = this.nextVersion;
    if (receivedVersion == this.previousVersion + 1) {
      this.previousVersion = receivedVersion;
      if (this.received != null) {
        addReceived(receivedVersion);
        consumeReceivedVersions();
      }
    } else if (receivedVersion == this.nextVersion - 1) {
      this.nextVersion = receivedVersion;
      if (this.received != null) {
        addReceived(receivedVersion);
        consumeReceivedVersions();
      }
    } else if (this.previousVersion < receivedVersion && receivedVersion < this.nextVersion) {
      addReceived(receivedVersion);
    }
    // if (this.nextVersion == 29 && oldv != 29) {
    // System.out.println("before=" + me + "\nafter=" + this + "\nadded "+receivedVersion);
    // }
  }


  protected void addReceived(long rv) {
    if (this.received == null) {
      this.receivedBaseVersion = this.previousVersion + 1;
      if (this.nextVersion > this.previousVersion) { // next version not known during
                                                     // deserialization
        long size = this.nextVersion - this.previousVersion;
        this.received = new BitSet((int) size);
      } else {
        this.received = new BitSet();
      }
    }
    // Assert.assertTrue(this.receivedBaseVersion > 0, "should not have a base version of zero. rv="
    // + rv + " ex=" + this);
    // Assert.assertTrue(rv >= this.receivedBaseVersion,
    // "attempt to record a version not in this exception. version=" + rv + " exception=" + this);
    this.received.set((int) (rv - this.receivedBaseVersion));
  }

  /**
   * checks to see if any of the received versions can be merged into the start/end version numbers
   */
  private void consumeReceivedVersions() {
    int idx = (int) (this.previousVersion - this.receivedBaseVersion + 1);
    while (this.previousVersion < this.nextVersion && this.received.get(idx)) {
      idx++;
      this.previousVersion++;
    }
    if (this.previousVersion < this.nextVersion) {
      idx = (int) (this.nextVersion - this.receivedBaseVersion) - 1;
      while (this.previousVersion < this.nextVersion && this.received.get(idx)) {
        idx--;
        this.nextVersion--;
      }
    }
  }


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

  @Override
  public RVVExceptionB clone() {
    RVVExceptionB clone = new RVVExceptionB(previousVersion, nextVersion);
    if (this.received != null) {
      clone.received = (BitSet) this.received.clone();
      clone.receivedBaseVersion = this.receivedBaseVersion;
    }
    return clone;
  }

  public void toData(DataOutput out) throws IOException {
    InternalDataSerializer.writeUnsignedVL(this.previousVersion, out);
    writeReceived(out);
  }

  protected void writeReceived(DataOutput out) throws IOException {
    final int size = received == null ? 1 : received.length() + 1;

    int deltaIndex = size - 1;
    long[] deltas = new long[size];
    long last = this.nextVersion;

    // TODO - it would be better just to serialize the longs[] in the BitSet
    // as is, rather than go through this delta encoding.
    for (ReceivedVersionsReverseIterator it = receivedVersionsReverseIterator(); it.hasNext();) {
      Long version = it.next();
      long delta = last - version.longValue();
      deltas[--deltaIndex] = delta;
      last = version.longValue();
    }
    deltas[0] = last - this.previousVersion;
    InternalDataSerializer.writeUnsignedVL(size - 1, out);

    for (long value : deltas) {
      InternalDataSerializer.writeUnsignedVL(value, out);
    }
  }

  @Override
  public String toString() {
    if (this.received != null) {
      StringBuilder sb = new StringBuilder();
      sb.append("e(n=").append(this.nextVersion).append("; p=").append(this.previousVersion);
      if (this.receivedBaseVersion != this.previousVersion + 1) {
        sb.append("; b=").append(this.receivedBaseVersion);
      }
      int lastBit = (int) (this.nextVersion - this.receivedBaseVersion);
      sb.append("; rb=[");
      int i = this.received.nextSetBit((int) (this.previousVersion - this.receivedBaseVersion + 1));
      if (i >= 0) {
        sb.append(i);
        for (i = this.received.nextSetBit(i + 1); (0 < i) && (i < lastBit); i =
            this.received.nextSetBit(i + 1)) {
          sb.append(',').append(i);
        }
      }
      sb.append(']');
      return sb.toString();
    }
    return "e(n=" + this.nextVersion + " p=" + this.previousVersion + "; rb=[])";
  }

  /**
   * For test purposes only. This isn't quite accurate, because I think two RVVs that have
   * effectively same exceptions may represent the exceptions differently. This method is testing
   * for an exact match of exception format.
   */
  @Override
  public boolean sameAs(RVVException ex) {
    if (ex instanceof RVVExceptionT) {
      return ((RVVExceptionT) ex).sameAs(this);
    }
    if (!super.sameAs(ex)) {
      return false;
    }
    RVVExceptionB other = (RVVExceptionB) ex;
    if (this.received == null) {
      if (other.received != null && !other.received.isEmpty()) {
        return false;
      }
    } else if (!this.received.equals(other.received))
      return false;
    return true;
  }

  /** has the given version been recorded as having been received? */
  public boolean contains(long version) {
    if (version <= this.previousVersion) {
      return false;
    }
    return (this.received != null && this.received.get((int) (version - this.receivedBaseVersion)));
  }

  /** return false if any revisions have been recorded in the range of this exception */
  public boolean isEmpty() {
    return (this.received == null) || (this.received.isEmpty());
  }

  public ReceivedVersionsReverseIterator receivedVersionsReverseIterator() {
    return new ReceivedVersionsReverseIteratorB();
  }

  @Override
  public long getHighestReceivedVersion() {
    if (isEmpty()) {
      return this.previousVersion;
    } else {
      // Note, the "length" of the bitset is the highest set bit + 1,
      // see the javadocs. That's why this works to return the highest
      // received version
      return receivedBaseVersion + received.length() - 1;
    }

  }

  /** it's a shame that BitSet has no iterator */
  protected class ReceivedVersionsReverseIteratorB extends ReceivedVersionsReverseIterator {
    int index;
    int nextIndex;

    ReceivedVersionsReverseIteratorB() {
      this.index = -1;
      if (received == null) {
        nextIndex = -1;
      } else {
        this.nextIndex = received.previousSetBit((int) (nextVersion - receivedBaseVersion - 1));
        if (nextIndex + receivedBaseVersion <= previousVersion) {
          nextIndex = -1;
        }
      }
    }

    boolean hasNext() {
      return this.nextIndex >= 0;
    }

    long next() {
      this.index = this.nextIndex;
      if (this.index < 0) {
        throw new NoSuchElementException("no more elements available");
      }
      this.nextIndex = received.previousSetBit(this.index - 1);
      if (nextIndex + receivedBaseVersion <= previousVersion) {
        nextIndex = -1;
      }
      return this.index + receivedBaseVersion;
    }

    void remove() {
      if (this.index < 0) {
        throw new NoSuchElementException("no more elements available");
      }
      received.clear(this.index);
    }
  }
}
