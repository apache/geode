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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import org.apache.geode.internal.InternalDataSerializer;

/**
 * This subclass of RVVException is the original class that uses TreeSets to hold received versions.
 * It is now only used if the exception represents a large gap.
 *
 *
 *
 */
public class RVVExceptionT extends RVVException {

  TreeSet<Long> received;

  RVVExceptionT(long previousVersion, long nextVersion) {
    super(previousVersion, nextVersion);
  }

  @Override
  public void add(long receivedVersion) {
    if (receivedVersion == this.previousVersion + 1) {
      this.previousVersion = receivedVersion;
      if (this.received != null) {
        consumeReceivedVersions();
      }
    } else if (receivedVersion == this.nextVersion - 1) {
      this.nextVersion = receivedVersion;
      if (this.received != null) {
        consumeReceivedVersions();
      }
    } else if (this.previousVersion < receivedVersion && receivedVersion < this.nextVersion) {
      addReceived(receivedVersion);
    }
  }


  @Override
  protected void addReceived(long rv) {
    if (this.received == null) {
      this.received = new TreeSet<Long>();
    }
    this.received.add(rv);
  }

  /**
   * checks to see if any of the received versions can be merged into the start/end version numbers
   */
  private void consumeReceivedVersions() {
    // Iterate in forward order
    for (Iterator<Long> it = this.received.iterator(); it.hasNext();) {
      long v = it.next();
      if (v <= this.previousVersion + 1) {
        // if the received version is less than the previous + 1, remove it.
        it.remove();
        if (v == this.previousVersion + 1) {
          // if the received version is equal to previous +1, also update the previous
          this.previousVersion = v;
        }
      } else {
        // Once we reach received entries greater than the previous, stop.
        break;
      }
    }

    // Iterate in reverse order
    for (Iterator<Long> it = this.received.descendingIterator(); it.hasNext();) {
      long v = it.next();
      if (v >= this.nextVersion - 1) {
        // if the received version is greater than the next - 1, remove it.
        it.remove();
        if (v == this.nextVersion - 1) {
          // if the received version is equal the next - 1, also update next.
          this.nextVersion = v;
        }
      } else {
        break;
      }
    }
  }

  @Override
  public RVVException clone() {
    RVVExceptionT clone = new RVVExceptionT(previousVersion, nextVersion);
    if (this.received != null) {
      clone.received = new TreeSet<Long>(this.received);
    }
    return clone;
  }


  public void writeReceived(DataOutput out) throws IOException {

    int size = this.received == null ? 0 : this.received.size();
    InternalDataSerializer.writeUnsignedVL(size, out);

    // Write each version in the exception as a delta from the previous version
    // this will likely be smaller than the absolute value, so it will
    // be more likely to fit into a byte or a short.
    long last = this.previousVersion;
    if (this.received != null) {
      for (Long version : this.received) {
        long delta = version.longValue() - last;
        InternalDataSerializer.writeUnsignedVL(delta, out);
        last = version.longValue();
      }
    }
    long delta = this.nextVersion - last;
    InternalDataSerializer.writeUnsignedVL(delta, out);
  }

  @Override
  public String toString() {
    if (this.received != null) {
      return "e(n=" + this.nextVersion + " p=" + +this.previousVersion
          + (this.received.size() == 0 ? "" : "; rs=" + this.received) + ")";
    }
    return "et(n=" + this.nextVersion + " p=" + this.previousVersion + "; rs=[])";
  }

  // @Override
  // public int hashCode() {
  // final int prime = 31;
  // int result = 1;
  // result = prime * result + (int) (nextVersion ^ (nextVersion >>> 32));
  // result = prime * result
  // + (int) (previousVersion ^ (previousVersion >>> 32));
  // result = prime * result + ((this.received == null) ? 0 : this.received.hashCode());
  // return result;
  // }

  /**
   * For test purposes only. This isn't quite accurate, because I think two RVVs that have
   * effectively same exceptions may represent the exceptions differently. This method is testing
   * for an exact match of exception format. <br>
   */
  @Override
  public boolean sameAs(RVVException ex) {
    if (!super.sameAs(ex)) {
      return false;
    }
    RVVExceptionT other = (RVVExceptionT) ex;
    if (this.received == null) {
      if (other.received != null && !other.received.isEmpty()) {
        return false;
      }
    } else if (other.received == null || !this.received.equals(other.received)) {
      return false;
    }
    return true;
  }

  protected boolean sameAs(RVVExceptionB ex) {
    if (!super.sameAs(ex)) {
      return false;
    }
    for (ReceivedVersionsReverseIterator it = receivedVersionsReverseIterator(); it.hasNext();) {
      if (!ex.contains(it.next())) {
        return false;
      }
    }
    for (ReceivedVersionsReverseIterator it = ex.receivedVersionsReverseIterator(); it.hasNext();) {
      if (!contains(it.next())) {
        return false;
      }
    }
    return true;
  }

  /** has the given version been recorded as having been received? */
  public boolean contains(long version) {
    if (version <= this.previousVersion) {
      return false;
    }
    return (this.received != null && (this.received.contains(version)));
  }

  /** return false if any revisions have been recorded in the range of this exception */
  public boolean isEmpty() {
    return (this.received == null || this.received.isEmpty());
  }

  public ReceivedVersionsReverseIterator receivedVersionsReverseIterator() {
    return new ReceivedVersionsReverseIteratorT();
  }

  public long getHighestReceivedVersion() {
    if (received == null || received.isEmpty()) {
      return previousVersion;
    } else {
      return received.last();
    }
  }

  @Override
  public boolean shouldChangeForm() {
    // If the received set size * 512 as big as the gap in versions, switch
    // to using bitset instead because that will use less memory.
    // A bit set using 1 bit for each *possible* entry
    // A treeset uses approximately 64 bytes for each *actual* entry
    return this.received != null
        && this.received.size() * 512 > this.nextVersion - this.previousVersion;
  }

  @Override
  public RVVException changeForm() {
    // Convert the exception to a bitset exception
    RVVExceptionB ex = new RVVExceptionB(previousVersion, nextVersion);
    for (ReceivedVersionsReverseIterator it = this.receivedVersionsReverseIterator(); it
        .hasNext();) {
      long next = it.next();
      ex.add(next);
    }
    return ex;
  }

  protected class ReceivedVersionsReverseIteratorT extends ReceivedVersionsReverseIterator {
    boolean noIterator;
    Iterator<Long> treeSetIterator;

    ReceivedVersionsReverseIteratorT() {
      if (received == null) {
        this.noIterator = true;
      } else {
        this.treeSetIterator = received.descendingIterator();
      }
    }

    boolean hasNext() {
      return !noIterator && this.treeSetIterator.hasNext();
    }

    long next() {
      if (!noIterator) {
        return this.treeSetIterator.next().longValue();
      }
      throw new NoSuchElementException("no more elements");
    }

    void remove() {
      if (!noIterator) {
        this.treeSetIterator.remove();
      }
    }
  }
}
