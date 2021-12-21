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
    if (receivedVersion == previousVersion + 1) {
      previousVersion = receivedVersion;
      if (received != null) {
        consumeReceivedVersions();
      }
    } else if (receivedVersion == nextVersion - 1) {
      nextVersion = receivedVersion;
      if (received != null) {
        consumeReceivedVersions();
      }
    } else if (previousVersion < receivedVersion && receivedVersion < nextVersion) {
      addReceived(receivedVersion);
    }
  }


  @Override
  protected void addReceived(long rv) {
    if (received == null) {
      received = new TreeSet<Long>();
    }
    received.add(rv);
  }

  /**
   * checks to see if any of the received versions can be merged into the start/end version numbers
   */
  private void consumeReceivedVersions() {
    // Iterate in forward order
    for (Iterator<Long> it = received.iterator(); it.hasNext();) {
      long v = it.next();
      if (v <= previousVersion + 1) {
        // if the received version is less than the previous + 1, remove it.
        it.remove();
        if (v == previousVersion + 1) {
          // if the received version is equal to previous +1, also update the previous
          previousVersion = v;
        }
      } else {
        // Once we reach received entries greater than the previous, stop.
        break;
      }
    }

    // Iterate in reverse order
    for (Iterator<Long> it = received.descendingIterator(); it.hasNext();) {
      long v = it.next();
      if (v >= nextVersion - 1) {
        // if the received version is greater than the next - 1, remove it.
        it.remove();
        if (v == nextVersion - 1) {
          // if the received version is equal the next - 1, also update next.
          nextVersion = v;
        }
      } else {
        break;
      }
    }
  }

  @Override
  public RVVException clone() {
    RVVExceptionT clone = new RVVExceptionT(previousVersion, nextVersion);
    if (received != null) {
      clone.received = new TreeSet<Long>(received);
    }
    return clone;
  }


  @Override
  public void writeReceived(DataOutput out) throws IOException {

    int size = received == null ? 0 : received.size();
    InternalDataSerializer.writeUnsignedVL(size, out);

    // Write each version in the exception as a delta from the previous version
    // this will likely be smaller than the absolute value, so it will
    // be more likely to fit into a byte or a short.
    long last = previousVersion;
    if (received != null) {
      for (Long version : received) {
        long delta = version.longValue() - last;
        InternalDataSerializer.writeUnsignedVL(delta, out);
        last = version.longValue();
      }
    }
    long delta = nextVersion - last;
    InternalDataSerializer.writeUnsignedVL(delta, out);
  }

  @Override
  public String toString() {
    if (received != null) {
      return "e(n=" + nextVersion + " p=" + +previousVersion
          + (received.size() == 0 ? "" : "; rs=" + received) + ")";
    }
    return "et(n=" + nextVersion + " p=" + previousVersion + "; rs=[])";
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
    if (received == null) {
      return other.received == null || other.received.isEmpty();
    } else
      return other.received != null && received.equals(other.received);
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
  @Override
  public boolean contains(long version) {
    if (version <= previousVersion) {
      return false;
    }
    return (received != null && (received.contains(version)));
  }

  /** return false if any revisions have been recorded in the range of this exception */
  @Override
  public boolean isEmpty() {
    return (received == null || received.isEmpty());
  }

  @Override
  public ReceivedVersionsReverseIterator receivedVersionsReverseIterator() {
    return new ReceivedVersionsReverseIteratorT();
  }

  @Override
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
    return received != null
        && received.size() * 512 > nextVersion - previousVersion;
  }

  @Override
  public RVVException changeForm() {
    // Convert the exception to a bitset exception
    RVVExceptionB ex = new RVVExceptionB(previousVersion, nextVersion);
    for (ReceivedVersionsReverseIterator it = receivedVersionsReverseIterator(); it
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
        noIterator = true;
      } else {
        treeSetIterator = received.descendingIterator();
      }
    }

    @Override
    boolean hasNext() {
      return !noIterator && treeSetIterator.hasNext();
    }

    @Override
    long next() {
      if (!noIterator) {
        return treeSetIterator.next().longValue();
      }
      throw new NoSuchElementException("no more elements");
    }

    @Override
    void remove() {
      if (!noIterator) {
        treeSetIterator.remove();
      }
    }
  }
}
