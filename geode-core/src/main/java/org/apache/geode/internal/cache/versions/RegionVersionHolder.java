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
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializable;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * RegionVersionHolders are part of a RegionVersionVector. A RVH holds the current version for a
 * member and a list of exceptions, which are holes in the list of versions received from that
 * member.
 *
 * RegionVersionHolders should be modified under synchronization on the holder.
 *
 * Starting in 7.0.1 the holder has a BitSet that records the most recent versions. The variable
 * bitSetVersion corresponds to bit zero, and subsequent bits represent bitSetVersion+1, +2, etc.
 * The method mergeBitSet() should be used to dump the BitSet's exceptions into the regular
 * exceptions list prior to performing operations like exceptions- comparisons or dominance checks.
 *
 * Starting in 8.0, the holder introduced a special exception to describe following use case of
 * unfinished operation: Operation R4 and R5 are applied locally, but never distributed to P. So P's
 * RVV for R is still 3. After R GIIed from P, R's RVV becomes R5(3-6), i.e. Exception's nextVersion
 * is currentVersion+1.
 *
 */
public class RegionVersionHolder<T> implements Cloneable, DataSerializable {

  private static final Logger logger = LogService.getLogger();

  private static List<RVVException> EMPTY_EXCEPTIONS = Collections.emptyList();

  long version = -1; // received version
  transient T id;
  private List<RVVException> exceptions;
  boolean isDepartedMember;

  // non final for tests
  public static int BIT_SET_WIDTH = 64 * 16; // should be a multiple of 4 64-bit longs

  private long bitSetVersion = 1;
  private BitSet bitSet;

  /**
   * This contructor should only be used for cloning a RegionVersionHolder or initializing and
   * invalid version holder (with version -1)
   *
   */
  public RegionVersionHolder(long ver) {
    this.version = ver;
  }

  public RegionVersionHolder(T id) {
    this.id = id;
    this.version = 0;
    this.bitSetVersion = 1;
    this.bitSet = new BitSet(RegionVersionHolder.BIT_SET_WIDTH);
  }

  public RegionVersionHolder(DataInput in) throws IOException {
    fromData(in);
  }

  public synchronized long getVersion() {
    RVVException e = null;
    List<RVVException> exs = getExceptions();
    if (!exs.isEmpty()) {
      e = exs.get(0);
    }
    if (isSpecialException(e, this)) {
      return e.getHighestReceivedVersion();
    } else {
      return this.version;
    }
  }

  private synchronized RVVException getSpecialException() {
    RVVException e = null;
    if (this.exceptions != null && !this.exceptions.isEmpty()) {
      e = this.exceptions.get(0);
    }
    if (isSpecialException(e, this)) {
      return e;
    } else {
      return null;
    }
  }

  public long getBitSetVersionForTesting() {
    return this.bitSetVersion;
  }

  private synchronized List<RVVException> getExceptions() {
    mergeBitSet();
    if (this.exceptions != null) {
      return this.exceptions;
    } else {
      return EMPTY_EXCEPTIONS;
    }
  }

  public synchronized List<RVVException> getExceptionForTest() {
    return getExceptions();
  }

  public synchronized int getExceptionCount() {
    return getExceptions().size();
  }

  public synchronized String exceptionsToString() {
    return getExceptions().toString();
  }


  /* test only method */
  public void setVersion(long ver) {
    this.version = ver;
  }

  @Override
  public synchronized RegionVersionHolder<T> clone() {
    RegionVersionHolder<T> clone = new RegionVersionHolder<T>(this.version);
    clone.id = this.id;
    clone.isDepartedMember = this.isDepartedMember;
    if (this.exceptions != null) {
      clone.exceptions = new LinkedList<RVVException>();
      for (RVVException e : this.exceptions) {
        clone.exceptions.add(e.clone());
      }
    }
    if (this.bitSet != null) {
      clone.bitSet = (BitSet) this.bitSet.clone();
      clone.bitSetVersion = this.bitSetVersion;
      clone.mergeBitSet();
    }
    return clone;
  }

  @Override
  public synchronized String toString() {
    // mergeBitSet();
    StringBuilder sb = new StringBuilder();
    sb.append("{rv").append(this.version).append(" bsv").append(this.bitSetVersion).append(" bs=[");
    if (this.bitSet != null) {
      int i = this.bitSet.nextSetBit(0);
      if (i >= 0) {
        sb.append("0");
        for (i = this.bitSet.nextSetBit(1); i > 0; i = this.bitSet.nextSetBit(i + 1)) {
          sb.append(',').append(i);
        }
      }
    }
    sb.append(']');
    if (this.exceptions != null && !this.exceptions.isEmpty()) {
      sb.append(this.exceptions.toString());
    }
    sb.append("}");
    return sb.toString();
  }

  /** add a version that is older than this.bitSetVersion */
  private void addOlderVersion(long missingVersion) {
    // exceptions iterate in reverse order on their previousVersion variable
    if (this.exceptions == null) {
      return;
    }
    int i = 0;
    for (Iterator<RVVException> it = this.exceptions.iterator(); it.hasNext();) {
      RVVException e = it.next();
      if (e.nextVersion <= missingVersion) {
        return; // there is no RVVException for this version
      }
      if (e.previousVersion < missingVersion && missingVersion < e.nextVersion) {
        String fine = null;
        if (logger.isTraceEnabled(LogMarker.RVV_VERBOSE)) {
          fine = e.toString();
        }
        e.add(missingVersion);
        if (e.isFilled()) {
          if (fine != null) {
            logger.trace(LogMarker.RVV_VERBOSE, "Filled exception {}", fine);
          }
          it.remove();
        } else if (e.shouldChangeForm()) {
          this.exceptions.set(i, e.changeForm());
        }
        if (this.exceptions.isEmpty()) {
          this.exceptions = null;
        }
        return;
      }
      i++;
    }
  }

  void flushBitSetDuringRecording(long version) {
    if (this.bitSetVersion + BIT_SET_WIDTH - 1 >= version) {
      return; // it fits in this bitset
    }

    int length = BIT_SET_WIDTH;
    int bitCountToFlush = length * 3 / 4;

    if (logger.isTraceEnabled(LogMarker.RVV_VERBOSE)) {
      logger.trace(LogMarker.RVV_VERBOSE, "flushing RVV bitset bitSetVersion={}; bits={}",
          this.bitSetVersion, this.bitSet);
    }
    // see if we can shift part of the bits so that exceptions in the recent bits can
    // be kept in the bitset and later filled without having to create real exception objects
    if (version >= this.bitSetVersion + length + bitCountToFlush) {
      // nope - flush the whole bitset
      addBitSetExceptions(length, version);
    } else {
      // yes - flush the lower part. We can only flush up to the last set bit because
      // the exceptions list includes a "next version" that indicates a received version.
      addBitSetExceptions(bitCountToFlush, this.bitSetVersion + bitCountToFlush);
    }
    if (logger.isTraceEnabled(LogMarker.RVV_VERBOSE)) {
      logger.trace(LogMarker.RVV_VERBOSE, "After flushing bitSetVersion={}; bits={}",
          this.bitSetVersion, this.bitSet);
    }
  }


  /** merge bit-set exceptions into the regular exceptions list */
  private synchronized void mergeBitSet() {
    if (this.bitSet != null && this.bitSetVersion < this.version) {
      addBitSetExceptions((int) (this.version - this.bitSetVersion), this.version);
    }
  }

  /**
   * Add exceptions from the BitSet array to the exceptions list. Assumes that the BitSet[0]
   * corresponds to this.bitSetVersion. This scans the bitset looking for gaps that are recorded as
   * RVV exceptions. The scan terminates at numBits or when the last set bit is found. The bitSet is
   * adjusted and a new bitSetVersion is established.
   *
   * @param newVersion the desired new bitSetVersion, which may be > the max representable in the
   *        bitset
   * @param numBits the desired number of bits to flush from the bitset
   */
  private void addBitSetExceptions(int numBits, long newVersion) {
    final boolean isDebugEnabled_RVV = logger.isTraceEnabled(LogMarker.RVV_VERBOSE);
    int lastSetIndex = -1;

    if (isDebugEnabled_RVV) {
      logger.trace(LogMarker.RVV_VERBOSE, "addBitSetExceptions({},{})", numBits, newVersion);
    }

    for (int idx = 0; idx < numBits;) {
      int nextMissingIndex = this.bitSet.nextClearBit(idx);
      if (nextMissingIndex < 0) {
        break;
      }

      lastSetIndex = nextMissingIndex - 1;

      int nextReceivedIndex = this.bitSet.nextSetBit(nextMissingIndex + 1);
      long nextReceivedVersion = -1;
      if (nextReceivedIndex > 0) {
        lastSetIndex = nextReceivedIndex;
        nextReceivedVersion = (long) (nextReceivedIndex) + this.bitSetVersion;
        idx = nextReceivedIndex + 1;
        if (isDebugEnabled_RVV) {
          logger.trace(LogMarker.RVV_VERBOSE,
              "found gap in bitSet: missing bit at index={}; next set index={}", nextMissingIndex,
              nextReceivedIndex);
        }
      } else {
        // We can't flush any more bits from the bit set because there
        // are no more received versions
        if (isDebugEnabled_RVV) {
          logger.trace(LogMarker.RVV_VERBOSE,
              "terminating flush at bit {} because of missing entries", lastSetIndex);
        }
        this.bitSetVersion += lastSetIndex;
        this.bitSet.clear();
        if (lastSetIndex != -1) {
          this.bitSet.set(0);
        }
        return;
      }
      long nextMissingVersion = Math.max(1, nextMissingIndex + this.bitSetVersion);
      if (nextReceivedVersion > nextMissingVersion) {
        addException(nextMissingVersion - 1, nextReceivedVersion);
        if (isDebugEnabled_RVV) {
          logger.trace(LogMarker.RVV_VERBOSE, "Added rvv exception e<rv{} - rv{}>",
              (nextMissingVersion - 1), nextReceivedVersion);
        }
      }
    }
    this.bitSet = this.bitSet.get(lastSetIndex, Math.max(lastSetIndex + 1, bitSet.size()));
    if (lastSetIndex > 0) {
      this.bitSetVersion = this.bitSetVersion + (long) lastSetIndex;
    }
  }

  synchronized void recordVersion(long version) {
    if (this.bitSet != null) {
      recordVersionWithBitSet(version);
    } else {
      recordVersionWithoutBitSet(version);
    }
  }

  private void recordVersionWithoutBitSet(long version) {
    if ((version - this.version) > 1) {
      this.addException(this.version, version);
      logRecordVersion(version);
      this.version = version;
      return;
    }
    if (this.version > version) {
      this.addOlderVersion(version);
    }
    this.version = Math.max(this.version, version);
  }

  private void recordVersionWithBitSet(long version) {
    if (this.version == version) {
      if (version >= this.bitSetVersion) {
        setVersionInBitSet(version);
      }
      this.addOlderVersion(version);
      return;
    }

    flushBitSetDuringRecording(version);

    if (version >= this.bitSetVersion) {
      if (this.getSpecialException() != null) {
        this.addOlderVersion(version);
      }
      setVersionInBitSet(version);
      this.version = Math.max(this.version, version);
      return;
    }
    this.addOlderVersion(version);
    this.version = Math.max(this.version, version);
  }

  private void setVersionInBitSet(long version) {
    this.bitSet.set((int) (version - this.bitSetVersion));
  }

  private void logRecordVersion(long version) {
    if (logger.isTraceEnabled(LogMarker.RVV_VERBOSE)) {
      logger.trace(LogMarker.RVV_VERBOSE, "Added rvv exception e<rv{} - rv{}>", this.version,
          version);
    }
  }


  /**
   * Add an exception that is older than this.bitSetVersion.
   */
  protected synchronized void addException(long previousVersion, long nextVersion) {
    if (this.exceptions == null) {
      this.exceptions = new LinkedList<RVVException>();
    }
    int i = 0;
    for (Iterator<RVVException> it = this.exceptions.iterator(); it.hasNext(); i++) {
      RVVException e = it.next();
      if (previousVersion >= e.nextVersion) {
        RVVException except = RVVException.createException(previousVersion, nextVersion);
        this.exceptions.add(i, except);
        return;
      }
    }
    this.exceptions.add(RVVException.createException(previousVersion, nextVersion));
  }

  synchronized void removeExceptionsOlderThan(long v) {
    mergeBitSet();
    if (this.exceptions != null) {
      for (Iterator<RVVException> it = this.exceptions.iterator(); it.hasNext();) {
        RVVException e = it.next();
        if (e.nextVersion <= v) {
          it.remove();
        }
      }
      if (this.exceptions.isEmpty()) {
        this.exceptions = null;
      }
    }
  }

  /**
   * Initialize this version holder from another version holder This is called during GII.
   *
   * It's more likely that the other holder has seen most of the versions, and this version holder
   * only has a few updates that happened since the GII started. So we apply our seen versions to
   * the other version holder and then initialize this version holder from the other version holder.
   */
  public synchronized void initializeFrom(RegionVersionHolder<T> source) {
    // Make sure the bitsets are merged in both the source
    // and this vector
    mergeBitSet();

    RegionVersionHolder<T> other = source.clone();
    other.mergeBitSet();
    // Get a copy of the local version and exceptions
    long myVersion = this.version;

    // initialize our version and exceptions to match the others
    this.exceptions = other.exceptions;
    this.version = other.version;

    // Initialize the bit set to be empty. Merge bit set should
    // have already done this, but just to be sure.
    if (this.bitSet != null) {
      this.bitSetVersion = this.version;
      // Make sure the bit set is empty except for the first, bit, indicating
      // that the version has been received.
      this.bitSet.set(0);
    }

    // Now if this.version/exceptions overlap with myVersion/myExceptions, use this'
    // The only case needs special handling is: if myVersion is newer than this.version,
    // should create an exception (this.version+1, myversion) and set this.version=myversion
    if (myVersion > this.version) {
      RVVException e = RVVException.createException(this.version, myVersion + 1);
      // add special exception
      if (this.exceptions == null) {
        this.exceptions = new LinkedList<RVVException>();
      }
      int i = 0;
      for (RVVException exception : this.exceptions) {
        if (e.compareTo(exception) >= 0) {
          break;
        }
        i++;
      }
      this.exceptions.add(i, e);
      this.version = myVersion;
    }
  }

  /**
   * initialize a holder that was cloned from another holder so it is ready for use in a live vector
   */
  void makeReadyForRecording() {
    if (this.bitSet == null) {
      this.bitSet = new BitSet(BIT_SET_WIDTH);
      this.bitSetVersion = this.version;
      this.bitSet.set(0);
    }
  }


  /**
   * returns true if this version holder has seen the given version number
   */
  synchronized boolean contains(long v) {
    if (v > getVersion()) {
      return false;
    } else {
      if (this.bitSet != null && v >= this.bitSetVersion) {
        return this.bitSet.get((int) (v - this.bitSetVersion));
      }
      if (this.exceptions == null) {
        return true;
      }
      for (Iterator<RVVException> it = this.exceptions.iterator(); it.hasNext();) {
        RVVException e = it.next();
        if (e.nextVersion <= v) {
          return true; // there is no RVVException for this version
        }
        if (e.previousVersion < v && v < e.nextVersion) {
          return e.contains(v);
        }
      }
      return true;
    }
  }

  /**
   * Returns true if this version hold has an exception in the exception list for the given version
   * number.
   *
   * This differs from contains because it returns true if v is greater than the last seen version
   * for this holder.
   */
  synchronized boolean hasExceptionFor(long v) {
    if (this.bitSet != null && v >= this.bitSetVersion) {
      if (v > this.bitSetVersion + this.bitSet.length()) {
        return false;
      }
      return this.bitSet.get((int) (v - this.bitSetVersion));
    }
    if (this.exceptions == null) {
      return false;
    }
    for (Iterator<RVVException> it = this.exceptions.iterator(); it.hasNext();) {
      RVVException e = it.next();
      if (e.nextVersion <= v) {
        return false; // there is no RVVException for this version
      }
      if (e.previousVersion < v && v < e.nextVersion) {
        return !e.contains(v);
      }
    }
    return false;
  }

  public boolean dominates(RegionVersionHolder<T> other) {
    return !other.isNewerThanOrCanFillExceptionsFor(this);
  }

  public boolean isSpecialException(RVVException e, RegionVersionHolder holder) {
    // deltaGII introduced a special exception, i.e. the hone is not in the middle, but at the end
    // For example, P was at P3, operation P4 is on-going and identified as unfinished operation.
    // The next operation from P should be P5, but P's currentVersion() should be 3. In holder,
    // it's described as P3(2-4), i.e. exception.nextVersion == holder.version + 1
    return (e != null && e.nextVersion == holder.version + 1);
  }

  /** returns true if this holder has seen versions that the other holder hasn't */
  public synchronized boolean isNewerThanOrCanFillExceptionsFor(RegionVersionHolder<T> source) {
    if (source == null || getVersion() > source.getVersion()) {
      return true;
    }

    // Prevent synhronization issues if other is a live version vector.
    RegionVersionHolder<T> other = source.clone();

    // since the exception sets are sorted with most recent ones first
    // we can make one pass over both sets to see if there are overlapping
    // exceptions or exceptions I don't have that the other does
    mergeBitSet(); // dump the bit-set exceptions into the regular exceptions list
    other.mergeBitSet();
    List<RVVException> mine = canonicalExceptions(this.exceptions);
    Iterator<RVVException> myIterator = mine.iterator();
    List<RVVException> his = canonicalExceptions(other.exceptions);
    Iterator<RVVException> otherIterator = his.iterator();
    // System.out.println("comparing " + mine + " with " + his);
    RVVException myException = myIterator.hasNext() ? myIterator.next() : null;
    RVVException otherException = otherIterator.hasNext() ? otherIterator.next() : null;
    // I can't fill exceptions that are newer than anything I've seen, so skip them
    while ((otherException != null && otherException.previousVersion > this.version)
        || isSpecialException(otherException, other)) {
      otherException = otherIterator.hasNext() ? otherIterator.next() : null;
    }
    while (otherException != null) {
      // System.out.println("comparing " + myException + " with " + otherException);
      if (myException == null) {
        return true;
      }
      if (isSpecialException(myException, this)) {
        // skip special exception
        myException = myIterator.hasNext() ? myIterator.next() : null;
        continue;
      }
      if (isSpecialException(otherException, other)) {
        // skip special exception
        otherException = otherIterator.hasNext() ? otherIterator.next() : null;
        continue;
      }
      if (myException.previousVersion >= otherException.nextVersion) {
        // |____| my exception
        // |____| other exception
        // my exception is newer than the other exception, so get the next one in the sorted list
        myException = myIterator.hasNext() ? myIterator.next() : null;
        continue;
      }
      if (otherException.previousVersion >= myException.nextVersion) {
        // |____| my exception
        // |____| other exception
        // my exception is older than the other exception, so I have seen changes
        // it has not
        return true;
      }
      if ((myException.previousVersion == otherException.previousVersion)
          && (myException.nextVersion == otherException.nextVersion)) {
        // |____| my exception
        // |____| -- other exception
        // If the exceptions are identical we can skip both of them and
        // go to the next pair
        myException = myIterator.hasNext() ? myIterator.next() : null;
        otherException = otherIterator.hasNext() ? otherIterator.next() : null;
        continue;
      }
      // There is some overlap between my exception and the other exception.
      //
      // |_________________| my exception
      // |____| \
      // |____|* \ the other exception is one of
      // |____| / these
      // |_____________________| /
      //
      // Unless my exception completely contains the other exception (*)
      // I have seen changes the other hasn't
      if ((otherException.previousVersion < myException.previousVersion)
          || (myException.nextVersion < otherException.nextVersion)) {
        return true;
      }
      // My exception completely contains the other exception and I have not
      // received any thing within its exception's range that it has not also seen
      otherException = otherIterator.hasNext() ? otherIterator.next() : null;
    }
    // System.out.println("Done iterating and returning false");
    return false;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.DataSerializable#toData(java.io.DataOutput)
   *
   * Version Holders serialized to disk, so if the serialization format of version holder changes,
   * we need to upgrade our persistence format.
   */
  public synchronized void toData(DataOutput out) throws IOException {
    mergeBitSet();
    InternalDataSerializer.writeUnsignedVL(this.version, out);
    int size = (this.exceptions == null) ? 0 : this.exceptions.size();
    InternalDataSerializer.writeUnsignedVL(size, out);
    out.writeBoolean(this.isDepartedMember);
    if (size > 0) {
      for (RVVException e : this.exceptions) {
        InternalDataSerializer.invokeToData(e, out);
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.DataSerializable#fromData(java.io.DataInput)
   */
  public void fromData(DataInput in) throws IOException {
    this.version = InternalDataSerializer.readUnsignedVL(in);
    int size = (int) InternalDataSerializer.readUnsignedVL(in);
    this.isDepartedMember = in.readBoolean();
    if (size > 0) {
      this.exceptions = new LinkedList<RVVException>();
      for (int i = 0; i < size; i++) {
        RVVException e = RVVException.createException(in);
        this.exceptions.add(e);
      }
    }
  }



  /*
   * Warning: this hashcode uses mutable state and is only good for as long as the holder is not
   * modified. It was added for unit testing.
   *
   * (non-Javadoc)
   *
   * @see java.lang.Object#hashCode()
   */
  public synchronized int hashCode() {
    mergeBitSet();
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) version;
    result = prime * result + (int) (version >> 32);
    result = prime * result + canonicalExceptions(exceptions).hashCode();
    return result;
  }

  // special exception will be kept in clone, but sometime we need to remove it for comparing
  // 2 RegionVersionHolders are actually the same
  void removeSpecialException() {
    if (this.exceptions != null && !this.exceptions.isEmpty()) {
      for (Iterator<RVVException> it = this.exceptions.iterator(); it.hasNext();) {
        RVVException e = it.next();
        if (isSpecialException(e, this)) {
          it.remove();
        }
      }
      if (this.exceptions.isEmpty()) {
        this.exceptions = null;
      }
    }
  }

  /**
   * For test purposes only. Two RVVs that have effectively same exceptions may represent the
   * exceptions differently. This method will test to see if the exception lists are effectively the
   * same, regardless of representation.
   */
  public synchronized boolean sameAs(RegionVersionHolder<T> other) {
    mergeBitSet();
    if (getVersion() != other.getVersion()) {
      return false;
    }
    RegionVersionHolder<T> vh1 = this.clone();
    RegionVersionHolder<T> vh2 = other.clone();
    vh1.removeSpecialException();
    vh2.removeSpecialException();
    if (vh1.exceptions == null || vh1.exceptions.isEmpty()) {
      if (vh2.exceptions != null && !vh2.exceptions.isEmpty()) {
        return false;
      }
    } else {
      List<RVVException> e1 = canonicalExceptions(vh1.exceptions);
      List<RVVException> e2 = canonicalExceptions(vh2.exceptions);
      Iterator<RVVException> it1 = e1.iterator();
      Iterator<RVVException> it2 = e2.iterator();
      while (it1.hasNext() && it2.hasNext()) {
        if (!it1.next().sameAs(it2.next())) {
          return false;
        }
      }
      return (!it1.hasNext() && !it2.hasNext());
    }

    return true;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof RegionVersionHolder)) {
      return false;
    }
    return sameAs((RegionVersionHolder) obj);
  }

  /**
   * Canonicalize an ordered set of exceptions. In the canonical form, none of the RVVExceptions
   * have any received versions.
   *
   * @return The canonicalized set of exceptions.
   */
  protected static List<RVVException> canonicalExceptions(List<RVVException> exceptions) {
    LinkedList<RVVException> canon = new LinkedList<RVVException>();
    if (exceptions != null) {
      // Iterate through the set of exceptions
      for (RVVException exception : exceptions) {
        if (exception.isEmpty()) {
          canon.add(exception);
        } else {
          long previous = exception.nextVersion;
          // Iterate through the set of received versions for this exception
          for (RVVException.ReceivedVersionsReverseIterator it =
              exception.receivedVersionsReverseIterator(); it.hasNext();) {
            Long received = it.next();
            // If we find a gap between the previous received version and the
            // next received version, add an exception.
            if (received != previous - 1) {
              canon.add(RVVException.createException(received, previous));
            }
            // move the previous reference
            previous = received;
          }

          // if there is a gap between the first received version and the previous
          // version, add an exception
          // this also handles the case where the RVV has no received versions,
          // because previous==exception.nextVersion in that case.
          if (exception.previousVersion != previous - 1) {
            canon.add(RVVException.createException(exception.previousVersion, previous));
          }
        }
      }
    }
    return canon;
  }

}
