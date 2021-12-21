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
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.logging.internal.log4j.api.LogService;

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

  @Immutable
  private static final List<RVVException> EMPTY_EXCEPTIONS = Collections.emptyList();

  long version = -1; // received version
  transient T id;
  private List<RVVException> exceptions;
  boolean isDepartedMember;

  // This flag is used to to determine if region sync is needed when receiving region sync requests
  // from other members hosting the region.
  //
  // It is set to true when region sync is being scheduled when triggered by the
  // member departed event, if the lost member is the holding member by this holder.
  // It can also be set to true, if region sync is not scheduled (this member joins the
  // cluster after the lost member departed event has occurred) but this member receives
  // request for region sync from other existing members. If this is the case, this member
  // will send region sync request to other existing members hosting the region.
  //
  // The flag will be always set to true once region sync is scheduled or done for the holding
  // member. If the holding member by the holder is lost multiples times and this member is
  // never lost, the timed task would schedule region sync for the lost member (bypassing this
  // condition check). If this member is also lost, when this member is restarted this
  // flag will be initialized as false.
  private transient boolean regionSynchronizeScheduledOrDone;

  // non final for tests
  @MutableForTesting
  public static int BIT_SET_WIDTH = 64 * 16; // should be a multiple of 4 64-bit longs

  private long bitSetVersion = 1;
  private BitSet bitSet;

  /**
   * This contructor should only be used for cloning a RegionVersionHolder or initializing and
   * invalid version holder (with version -1)
   *
   */
  public RegionVersionHolder(long ver) {
    version = ver;
  }

  public RegionVersionHolder(T id) {
    this.id = id;
    version = 0;
    bitSetVersion = 1;
    bitSet = new BitSet(RegionVersionHolder.BIT_SET_WIDTH);
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
      return version;
    }
  }

  private synchronized RVVException getSpecialException() {
    RVVException e = null;
    if (exceptions != null && !exceptions.isEmpty()) {
      e = exceptions.get(0);
    }
    if (isSpecialException(e, this)) {
      return e;
    } else {
      return null;
    }
  }

  public long getBitSetVersionForTesting() {
    return bitSetVersion;
  }

  public BitSet getBitSetForTesting() {
    return bitSet;
  }

  private synchronized List<RVVException> getExceptions() {
    mergeBitSet();
    if (exceptions != null) {
      return exceptions;
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

  /**
   * Should only be called as part of cloning a RegionVersionHolder
   */
  void setVersion(long ver) {
    version = ver;
  }

  @Override
  public synchronized RegionVersionHolder<T> clone() {
    RegionVersionHolder<T> clone = new RegionVersionHolder<>(version);
    clone.id = id;
    clone.isDepartedMember = isDepartedMember;
    if (exceptions != null) {
      clone.exceptions = new LinkedList<>();
      for (RVVException e : exceptions) {
        clone.exceptions.add(e.clone());
      }
    }
    if (bitSet != null) {
      clone.bitSet = (BitSet) bitSet.clone();
      clone.bitSetVersion = bitSetVersion;
      clone.mergeBitSet();
    }
    return clone;
  }

  @Override
  public synchronized String toString() {
    // mergeBitSet();
    StringBuilder sb = new StringBuilder();
    sb.append("{rv").append(version).append(" bsv").append(bitSetVersion).append(" bs=[");
    if (bitSet != null) {
      int i = bitSet.nextSetBit(0);
      if (i >= 0) {
        sb.append("0");
        for (i = bitSet.nextSetBit(1); i > 0; i = bitSet.nextSetBit(i + 1)) {
          sb.append(',').append(i);
        }
      }
    }
    sb.append(']');
    if (exceptions != null && !exceptions.isEmpty()) {
      sb.append(exceptions);
    }
    sb.append("}");
    return sb.toString();
  }

  /** add a version that is older than this.bitSetVersion */
  private void addOlderVersion(long missingVersion) {
    // exceptions iterate in reverse order on their previousVersion variable
    if (exceptions == null) {
      return;
    }
    int i = 0;
    for (Iterator<RVVException> it = exceptions.iterator(); it.hasNext();) {
      RVVException e = it.next();
      if (e.nextVersion <= missingVersion) {
        return; // there is no RVVException for this version
      }
      if (e.previousVersion < missingVersion) {
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
          exceptions.set(i, e.changeForm());
        }
        if (exceptions.isEmpty()) {
          exceptions = null;
        }
        return;
      }
      i++;
    }
  }

  void flushBitSetDuringRecording(long version) {
    if (bitSetVersion + BIT_SET_WIDTH - 1 >= version) {
      return; // it fits in this bitset
    }

    int bitCountToFlush = BIT_SET_WIDTH * 3 / 4;

    // We can only flush up to the last set bit because
    // the exceptions list includes a "next version" that indicates a received version.
    bitCountToFlush = bitSet.previousSetBit(bitCountToFlush);

    if (logger.isTraceEnabled(LogMarker.RVV_VERBOSE)) {
      logger.trace(LogMarker.RVV_VERBOSE, "flushing RVV bitset bitSetVersion={}; bits={}",
          bitSetVersion, bitSet);
    }
    // see if we can shift part of the bits so that exceptions in the recent bits can
    // be kept in the bitset and later filled without having to create real exception objects
    if (bitCountToFlush == -1 || version >= bitSetVersion + BIT_SET_WIDTH + bitCountToFlush) {
      // nope - flush the whole bitset
      addBitSetExceptions(version);
    } else {
      // yes - flush the lower part. We can only flush up to the last set bit because
      // the exceptions list includes a "next version" that indicates a received version.
      addBitSetExceptions(bitSetVersion + bitCountToFlush);
    }
    if (logger.isTraceEnabled(LogMarker.RVV_VERBOSE)) {
      logger.trace(LogMarker.RVV_VERBOSE, "After flushing bitSetVersion={}; bits={}",
          bitSetVersion, bitSet);
    }
  }


  /** merge bit-set exceptions into the regular exceptions list */
  private synchronized void mergeBitSet() {
    if (bitSet != null && bitSetVersion < version) {
      addBitSetExceptions(version);
    }
  }

  /**
   * Add exceptions from the BitSet array to the exceptions list. Assumes that the BitSet[0]
   * corresponds to this.bitSetVersion. This scans the bitset looking for gaps that are recorded as
   * RVV exceptions. The scan terminates at numBits or when the last set bit is found. The bitSet is
   * adjusted and a new bitSetVersion is established.
   *
   * @param newVersion the desired new bitSetVersion, which may be > the max representable in the
   *        bitset. This should *always* be a version that has been received, because this
   *        method may need to create an exception up to this version, and the existance of an
   *        exception implies that the final version was received.
   *
   *
   */
  private void addBitSetExceptions(long newVersion) {
    if (newVersion <= bitSetVersion) {
      return;
    }

    // Add all of the exceptions that should be flushed from the bitset as real exceptions
    Iterator<RVVException> exceptionIterator =
        new BitSetExceptionIterator(bitSet, bitSetVersion, newVersion);
    while (exceptionIterator.hasNext()) {
      addException(exceptionIterator.next());
    }

    // Move the data in the bitset forward to reflect the new version
    if (newVersion > bitSetVersion + bitSet.size()) {
      // Optimization - if the new version is past the end of the bitset, just clear the bitset
      bitSet.clear();
    } else {
      // Otherwise slide the bitset over to the new offset
      int offsetIncrease = (int) (newVersion - bitSetVersion);
      bitSet = bitSet.get(offsetIncrease, bitSet.size());
    }

    // Move the bitset version
    bitSetVersion = newVersion;
  }

  synchronized void recordVersion(long version) {
    if (bitSet != null) {
      recordVersionWithBitSet(version);
    } else {
      recordVersionWithoutBitSet(version);
    }
  }

  private void recordVersionWithoutBitSet(long version) {
    if ((version - this.version) > 1) {
      addException(this.version, version);
      logRecordVersion(version);
      this.version = version;
      return;
    }
    if (this.version > version) {
      addOlderVersion(version);
    }
    this.version = Math.max(this.version, version);
  }

  private void recordVersionWithBitSet(long version) {

    flushBitSetDuringRecording(version);

    if (this.version == version) {
      if (version >= bitSetVersion) {
        setVersionInBitSet(version);
      }
      addOlderVersion(version);
      return;
    }


    if (version >= bitSetVersion) {
      if (getSpecialException() != null) {
        addOlderVersion(version);
      }
      setVersionInBitSet(version);
      this.version = Math.max(this.version, version);
      return;
    }
    addOlderVersion(version);
    this.version = Math.max(this.version, version);
  }

  private void setVersionInBitSet(long version) {
    long bitToSet = version - bitSetVersion;
    if (bitToSet > BIT_SET_WIDTH) {
      Assert.fail("Trying to set a bit larger than the size of the bitset " + bitToSet);
    }
    bitSet.set(Math.toIntExact(bitToSet));
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
  synchronized void addException(final long previousVersion, final long nextVersion) {
    RVVException newException = RVVException.createException(previousVersion, nextVersion);
    addException(newException);
  }

  private void addException(RVVException newException) {
    if (exceptions == null) {
      exceptions = new LinkedList<>();
    }
    int i = 0;
    for (Iterator<RVVException> it = exceptions.iterator(); it.hasNext(); i++) {
      RVVException e = it.next();
      if (newException.previousVersion >= e.nextVersion) {
        RVVException except = newException;
        exceptions.add(i, except);
        return;
      }
    }
    exceptions.add(newException);
  }

  synchronized void removeExceptionsOlderThan(long v) {
    mergeBitSet();
    if (exceptions != null) {
      for (Iterator<RVVException> it = exceptions.iterator(); it.hasNext();) {
        RVVException e = it.next();
        if (e.nextVersion <= v) {
          it.remove();
        }
      }
      if (exceptions.isEmpty()) {
        exceptions = null;
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
    long myVersion = version;

    // initialize our version and exceptions to match the others
    exceptions = other.exceptions;
    version = other.version;



    // Now if this.version/exceptions overlap with myVersion/myExceptions, use this'
    // The only case needs special handling is: if myVersion is newer than this.version,
    // should create an exception (this.version+1, myversion) and set this.version=myversion
    if (myVersion > version) {
      RVVException e = RVVException.createException(version, myVersion + 1);
      // add special exception
      if (exceptions == null) {
        exceptions = new LinkedList<>();
      }
      int i = 0;
      for (RVVException exception : exceptions) {
        if (e.compareTo(exception) >= 0) {
          break;
        }
        i++;
      }
      exceptions.add(i, e);
      version = myVersion;
    }

    // Initialize the bit set to be empty. Merge bit set should
    // have already done this, but just to be sure.
    if (bitSet != null) {
      bitSetVersion = version;
      // Make sure the bit set is empty except for the first, bit, indicating
      // that the version has been received.
      bitSet.set(0);
    }
  }

  /**
   * initialize a holder that was cloned from another holder so it is ready for use in a live vector
   */
  void makeReadyForRecording() {
    if (bitSet == null) {
      bitSet = new BitSet(BIT_SET_WIDTH);
      bitSetVersion = version;
      bitSet.set(0);
    }
  }


  /**
   * returns true if this version holder has seen the given version number
   */
  synchronized boolean contains(long v) {
    if (v > getVersion()) {
      return false;
    } else {
      if (bitSet != null && v >= bitSetVersion) {
        return bitSet.get((int) (v - bitSetVersion));
      }
      if (exceptions == null) {
        return true;
      }
      for (Iterator<RVVException> it = exceptions.iterator(); it.hasNext();) {
        RVVException e = it.next();
        if (e.nextVersion <= v) {
          return true; // there is no RVVException for this version
        }
        if (e.previousVersion < v) {
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
    if (bitSet != null && v >= bitSetVersion) {
      if (v > bitSetVersion + bitSet.length()) {
        return false;
      }
      return bitSet.get((int) (v - bitSetVersion));
    }
    if (exceptions == null) {
      return false;
    }
    for (Iterator<RVVException> it = exceptions.iterator(); it.hasNext();) {
      RVVException e = it.next();
      if (e.nextVersion <= v) {
        return false; // there is no RVVException for this version
      }
      if (e.previousVersion < v) {
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
    List<RVVException> mine = canonicalExceptions(exceptions);
    Iterator<RVVException> myIterator = mine.iterator();
    List<RVVException> others = canonicalExceptions(other.exceptions);
    Iterator<RVVException> otherIterator = others.iterator();
    // System.out.println("comparing " + mine + " with " + others);
    RVVException myException = myIterator.hasNext() ? myIterator.next() : null;
    RVVException otherException = otherIterator.hasNext() ? otherIterator.next() : null;
    // I can't fill exceptions that are newer than anything I've seen, so skip them
    while ((otherException != null && otherException.previousVersion > version)
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
  @Override
  public synchronized void toData(DataOutput out) throws IOException {
    mergeBitSet();
    InternalDataSerializer.writeUnsignedVL(version, out);
    int size = (exceptions == null) ? 0 : exceptions.size();
    InternalDataSerializer.writeUnsignedVL(size, out);
    out.writeBoolean(isDepartedMember);
    if (size > 0) {
      for (RVVException e : exceptions) {
        InternalDataSerializer.invokeToData(e, out);
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.DataSerializable#fromData(java.io.DataInput)
   */
  @Override
  public void fromData(DataInput in) throws IOException {
    version = InternalDataSerializer.readUnsignedVL(in);
    int size = (int) InternalDataSerializer.readUnsignedVL(in);
    isDepartedMember = in.readBoolean();
    if (size > 0) {
      exceptions = new LinkedList<>();
      for (int i = 0; i < size; i++) {
        RVVException e = RVVException.createException(in);
        exceptions.add(e);
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
    if (exceptions != null && !exceptions.isEmpty()) {
      for (Iterator<RVVException> it = exceptions.iterator(); it.hasNext();) {
        RVVException e = it.next();
        if (isSpecialException(e, this)) {
          it.remove();
        }
      }
      if (exceptions.isEmpty()) {
        exceptions = null;
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
    RegionVersionHolder<T> vh1 = clone();
    RegionVersionHolder<T> vh2 = other.clone();
    vh1.removeSpecialException();
    vh2.removeSpecialException();
    if (vh1.exceptions == null || vh1.exceptions.isEmpty()) {
      return vh2.exceptions == null || vh2.exceptions.isEmpty();
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
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof RegionVersionHolder)) {
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
    LinkedList<RVVException> canon = new LinkedList<>();
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
            long received = it.next();
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

  private synchronized boolean isRegionSynchronizeScheduledOrDone() {
    return regionSynchronizeScheduledOrDone;
  }

  public synchronized void setRegionSynchronizeScheduled() {
    regionSynchronizeScheduledOrDone = true;
  }


  /**
   * Check to see if regionSynchronizeScheduledOrDone is set to true. If it is not,
   * the regionSynchronizeScheduledOrDone variable is set to true and returns true.
   * If it is already set to true, do nothing and returns false.
   */
  public synchronized boolean setRegionSynchronizeScheduledOrDoneIfNot() {
    if (!isRegionSynchronizeScheduledOrDone()) {
      regionSynchronizeScheduledOrDone = true;
      return true;
    }
    return false;
  }
}
