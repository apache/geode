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
import java.util.LinkedList;
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
  @Override
  public void add(long receivedVersion) {
    // String me = this.toString();
    // long oldv = this.nextVersion;
    if (receivedVersion == previousVersion + 1) {
      previousVersion = receivedVersion;
      if (received != null) {
        addReceived(receivedVersion);
        consumeReceivedVersions();
      }
    } else if (receivedVersion == nextVersion - 1) {
      nextVersion = receivedVersion;
      if (received != null) {
        addReceived(receivedVersion);
        consumeReceivedVersions();
      }
    } else if (previousVersion < receivedVersion && receivedVersion < nextVersion) {
      addReceived(receivedVersion);
    }
    // if (this.nextVersion == 29 && oldv != 29) {
    // System.out.println("before=" + me + "\nafter=" + this + "\nadded "+receivedVersion);
    // }
  }


  @Override
  protected void addReceived(long rv) {
    if (received == null) {
      receivedBaseVersion = previousVersion + 1;
      if (nextVersion > previousVersion) { // next version not known during
                                           // deserialization
        long size = nextVersion - previousVersion;
        received = new BitSet((int) size);
      } else {
        received = new BitSet();
      }
    }
    // Assert.assertTrue(this.receivedBaseVersion > 0, "should not have a base version of zero. rv="
    // + rv + " ex=" + this);
    // Assert.assertTrue(rv >= this.receivedBaseVersion,
    // "attempt to record a version not in this exception. version=" + rv + " exception=" + this);
    received.set((int) (rv - receivedBaseVersion));
  }

  /**
   * checks to see if any of the received versions can be merged into the start/end version numbers
   */
  private void consumeReceivedVersions() {
    int idx = (int) (previousVersion - receivedBaseVersion + 1);
    while (previousVersion < nextVersion && received.get(idx)) {
      idx++;
      previousVersion++;
    }
    if (previousVersion < nextVersion) {
      idx = (int) (nextVersion - receivedBaseVersion) - 1;
      while (previousVersion < nextVersion && received.get(idx)) {
        idx--;
        nextVersion--;
      }
    }
  }


  /*
   * (non-Javadoc)
   *
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(RVVException o) {
    long thisVal = previousVersion;
    long anotherVal = o.previousVersion;
    return (thisVal < anotherVal ? -1 : (thisVal == anotherVal ? 0 : 1));
  }

  @Override
  public RVVExceptionB clone() {
    RVVExceptionB clone = new RVVExceptionB(previousVersion, nextVersion);
    if (received != null) {
      clone.received = (BitSet) received.clone();
      clone.receivedBaseVersion = receivedBaseVersion;
    }
    return clone;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    InternalDataSerializer.writeUnsignedVL(previousVersion, out);
    writeReceived(out);
  }

  @Override
  protected void writeReceived(DataOutput out) throws IOException {
    LinkedList<Long> deltas = new LinkedList<>();
    long last = nextVersion;

    // TODO - it would be better just to serialize the longs[] in the BitSet
    // as is, rather than go through this delta encoding.
    for (ReceivedVersionsReverseIterator it = receivedVersionsReverseIterator(); it.hasNext();) {
      long version = it.next();
      deltas.addFirst(last - version);
      last = version;
    }
    InternalDataSerializer.writeUnsignedVL(deltas.size(), out); // Number of received versions

    // Last version is the oldest received version, still need the delta from there to previous
    deltas.addFirst(last - previousVersion);

    for (long value : deltas) {
      InternalDataSerializer.writeUnsignedVL(value, out);
    }
  }

  @Override
  public String toString() {
    if (received != null) {
      StringBuilder sb = new StringBuilder();
      sb.append("e(n=").append(nextVersion).append("; p=").append(previousVersion);
      if (receivedBaseVersion != previousVersion + 1) {
        sb.append("; b=").append(receivedBaseVersion);
      }
      int lastBit = (int) (nextVersion - receivedBaseVersion);
      sb.append("; rb=[");
      int i = received.nextSetBit((int) (previousVersion - receivedBaseVersion + 1));
      if (i >= 0) {
        sb.append(i);
        for (i = received.nextSetBit(i + 1); (0 < i) && (i < lastBit); i =
            received.nextSetBit(i + 1)) {
          sb.append(',').append(i);
        }
      }
      sb.append(']');
      return sb.toString();
    }
    return "e(n=" + nextVersion + " p=" + previousVersion + "; rb=[])";
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
    if (received == null) {
      return other.received == null || other.received.isEmpty();
    } else
      return received.equals(other.received);
  }

  /** has the given version been recorded as having been received? */
  @Override
  public boolean contains(long version) {
    if (version <= previousVersion) {
      return false;
    }
    return (received != null && received.get((int) (version - receivedBaseVersion)));
  }

  /** return false if any revisions have been recorded in the range of this exception */
  @Override
  public boolean isEmpty() {
    return (received == null) || (received.isEmpty());
  }

  @Override
  public ReceivedVersionsReverseIterator receivedVersionsReverseIterator() {
    return new ReceivedVersionsReverseIteratorB();
  }

  @Override
  public long getHighestReceivedVersion() {
    if (isEmpty()) {
      return previousVersion;
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
      index = -1;
      if (received == null) {
        nextIndex = -1;
      } else {
        nextIndex = received.previousSetBit((int) (nextVersion - receivedBaseVersion - 1));
        if (nextIndex + receivedBaseVersion <= previousVersion) {
          nextIndex = -1;
        }
      }
    }

    @Override
    boolean hasNext() {
      return nextIndex >= 0;
    }

    @Override
    long next() {
      index = nextIndex;
      if (index < 0) {
        throw new NoSuchElementException("no more elements available");
      }
      nextIndex = received.previousSetBit(index - 1);
      if (nextIndex + receivedBaseVersion <= previousVersion) {
        nextIndex = -1;
      }
      return index + receivedBaseVersion;
    }

    @Override
    void remove() {
      if (index < 0) {
        throw new NoSuchElementException("no more elements available");
      }
      received.clear(index);
    }
  }
}
