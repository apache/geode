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
package org.apache.geode.distributed.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.GemFireIOException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.UpdateAttributesProcessor;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.util.ArrayUtils;

/**
 * Provides advice on sending distribution messages. For a given operation, this advisor will
 * provide a list of recipients that a message should be sent to, and other information depending on
 * the operation. Each distributed entity that can have remote counterparts maintains an instance of
 * <code>DistributionAdvisor</code> and maintains it by giving it a <code>Profile</code> for each of
 * its remote counterparts, and telling it to delete a profile when that counterpart no longer
 * exists.
 * <p>
 * Provides <code>advise</code> methods for each type of operation that requires specialized
 * decision making based on the profiles. For all other operations that do not require specialized
 * decision making, the {@link #adviseGeneric} method is provided.
 * <p>
 * A primary design goal of this class is scalability: the footprint must be kept to a minimum as
 * the number of instances grows across a growing number of members in the distributed system.
 *
 * @since GemFire 3.0
 */
public class DistributionAdvisor {

  private static final Logger logger = LogService.getLogger();

  /**
   * Specifies the starting version number for the profileVersionSequencer.
   */
  public static final int START_VERSION_NUMBER = Integer
      .getInteger(DistributionConfig.GEMFIRE_PREFIX + "DistributionAdvisor.startVersionNumber", 1)
      .intValue();

  /**
   * Specifies the starting serial number for the serialNumberSequencer.
   */
  public static final int START_SERIAL_NUMBER =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "Cache.startSerialNumber", 1 // Integer.MAX_VALUE-10
      ).intValue();

  /**
   * Incrementing serial number used to identify order of resource creation
   */
  private static final AtomicInteger serialNumberSequencer = new AtomicInteger(START_SERIAL_NUMBER);

  /**
   * This serial number indicates a "missing" serial number.
   */
  public static final int ILLEGAL_SERIAL = -1;

  /**
   * Used to compare profile versioning numbers against {@link Integer#MAX_VALUE} and
   * {@link Integer#MIN_VALUE} to determine if a rollover has occurred.
   */
  public static final int ROLLOVER_THRESHOLD = Integer
      .getInteger(DistributionConfig.GEMFIRE_PREFIX + "CacheDistributionAdvisor.rolloverThreshold",
          1000)
      .intValue();

  /**
   * {@link Integer#MAX_VALUE} minus {@link #ROLLOVER_THRESHOLD} determines the upper threshold for
   * rollover comparison.
   */
  private static final int ROLLOVER_THRESHOLD_UPPER = Integer.MAX_VALUE - ROLLOVER_THRESHOLD;

  /**
   * {@link Integer#MIN_VALUE} plus {@link #ROLLOVER_THRESHOLD} determines the lower threshold for
   * rollover comparison.
   */
  private static final int ROLLOVER_THRESHOLD_LOWER = Integer.MIN_VALUE + ROLLOVER_THRESHOLD;

  /**
   * Incrementing serial number used to identify order of region creation
   *
   * @see Profile#getVersion()
   */
  private final AtomicInteger profileVersionSequencer = new AtomicInteger(START_VERSION_NUMBER);

  /**
   * This system property is not supported and disabling intelligent messaging is currently
   * problematic
   */
  protected static final boolean disabled = Boolean.getBoolean("disable-intelligent-msg");



  /**
   * Indicates whether this advisor is has been initialized. This will be false when a shared region
   * is mapped into the cache but there has been no distributed operations done on it yet.
   */
  private volatile boolean initialized = false;

  /**
   * Synchronization lock used for controlling access to initialization. We do not synchronize on
   * this advisor itself because we use that synchronization for putProfile and we can not lock out
   * putProfile while we are doing initialization
   */
  private final Object initializeLock = new Object();

  /**
   * the version of the profile set
   *
   * @since GemFire 5.1
   */
  private long membershipVersion;

  /**
   * whether membership ops are closed (because the DA's been closed). Access under synchronization
   * on (this)
   */
  private boolean membershipClosed;

  /**
   * opCountLock guards access to previousVersionOpCount and currentVersionOpCount
   */
  private final Object opCountLock = new Object();

  /**
   * the number of operations in-progress for previous versions of the profile set. Guarded by
   * opCountLock
   */
  private long previousVersionOpCount;

  /**
   * the number of operations in-progress for the current version of the profile set. Guarded by
   * opCountLock
   */
  private long currentVersionOpCount;

  /**
   * Hold onto removed profiles to compare to late-processed profiles. Fix for bug 36881. Protected
   * by synchronizing on this DistributionAdvisor. guarded.By this DistributionAdvisor
   */
  private final Map removedProfiles = new HashMap();

  /**
   * My database of Profiles
   */
  protected volatile Profile[] profiles = new Profile[0];

  /**
   * Number of active profiles
   */
  private int numActiveProfiles = 0;

  /**
   * A collection of MembershipListeners that want to be notified when a profile is added to or
   * removed from this DistributionAdvisor. The keys are membership listeners and the values are
   * Boolean.TRUE.
   */
  protected ConcurrentMap membershipListeners = new ConcurrentHashMap();

  /**
   * A collection of listeners for changes to profiles. These listeners are notified if a profile is
   * added, removed, or updated.
   */
  protected ConcurrentMap profileListeners = new ConcurrentHashMap();

  /**
   * The resource getting advise from this.
   */
  private final DistributionAdvisee advisee;
  /**
   * The membership listener registered with the dm.
   */
  private final MembershipListener ml;

  protected DistributionAdvisor(DistributionAdvisee advisee) {
    this.advisee = advisee;
    this.ml = new MembershipListener() {

      public void memberJoined(DistributionManager distributionManager,
          InternalDistributedMember id) {
        // Ignore
      }

      public void quorumLost(DistributionManager distributionManager,
          Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}

      @SuppressWarnings("synthetic-access")
      public void memberDeparted(DistributionManager distributionManager,
          final InternalDistributedMember id, boolean crashed) {
        boolean shouldSync = crashed && shouldSyncForCrashedMember(id);
        final Profile profile = getProfile(id);
        boolean removed =
            removeId(id, crashed, false/* destroyed */, true/* fromMembershipListener */);
        // if concurrency checks are enabled and this was a crash we may need to
        // sync with other members in case an update was lost. We do this in the
        // waiting thread pool so as not to block other membership listeners
        if (removed && shouldSync) {
          syncForCrashedMember(id, profile);
        }
      }

      public void memberSuspect(DistributionManager distributionManager,
          InternalDistributedMember id, InternalDistributedMember whoSuspected, String reason) {}

    };
  }

  public static DistributionAdvisor createDistributionAdvisor(DistributionAdvisee advisee) {
    DistributionAdvisor advisor = new DistributionAdvisor(advisee);
    advisor.initialize();
    return advisor;
  }

  protected void initialize() {
    subInit();
    getDistributionManager().addMembershipListener(this.ml);
  }

  protected void subInit() {
    // override for any additional initialization specific to subclass
  }

  /**
   * determine whether a delta-gii synchronization should be performed for this lost member
   *
   * @return true if a delta-gii should be performed
   */
  public boolean shouldSyncForCrashedMember(InternalDistributedMember id) {
    return (this.advisee instanceof DistributedRegion)
        && ((DistributedRegion) this.advisee).shouldSyncForCrashedMember(id);
  }

  /** perform a delta-GII for the given lost member */
  public void syncForCrashedMember(final InternalDistributedMember id, final Profile profile) {
    final DistributedRegion dr = getRegionForDeltaGII();
    if (dr == null) {
      return;
    }

    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("da.syncForCrashedMember will sync region in waiting thread pool: {}", dr);
    }
    dr.getDistributionManager().getWaitingThreadPool().execute(new Runnable() {
      // bug #49601 - don't synchronize until GII has been performed
      public void run() {
        while (!dr.isInitialized()) {
          if (dr.isDestroyed()) {
            return;
          } else {
            try {
              if (isDebugEnabled) {
                logger.debug(
                    "da.syncForCrashedMember waiting for region to finish initializing: {}", dr);
              }
              Thread.sleep(100);
            } catch (InterruptedException e) {
              return;
            }
          }
        }
        CacheProfile cp = (CacheProfile) profile;
        PersistentMemberID persistentId = cp.persistentID;
        if (dr.getDataPolicy().withPersistence() && persistentId == null) {
          // Fix for 46704. The lost member may be a replicate
          // or an empty accessor. We don't need to to a synchronization
          // in that case, because those members send their writes to
          // a persistent member.
          if (isDebugEnabled) {
            logger.debug(
                "da.syncForCrashedMember skipping sync because crashed member is not persistent: {}",
                id);
          }
          return;
        }


        VersionSource lostVersionID;
        if (persistentId != null) {
          lostVersionID = persistentId.getVersionMember();
        } else {
          lostVersionID = id;
        }
        dr.synchronizeForLostMember(id, lostVersionID);
      }
    });
  }

  /** find the region for a delta-gii operation (synch) */
  public DistributedRegion getRegionForDeltaGII() {
    if (this.advisee instanceof DistributedRegion) {
      return (DistributedRegion) this.advisee;
    }
    return null;
  }

  public String toStringWithProfiles() {
    final StringBuffer sb = new StringBuffer(toString());
    sb.append(" with profiles=(");
    Profile[] profs = this.profiles; // volatile read
    for (int i = 0; i < profs.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(profs[i]);
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * Increment and get next profile version from {@link #profileVersionSequencer}.
   *
   * @return next profile version number
   */
  protected int incrementAndGetVersion() {
    // NOTE: int should rollover if value is Integer.MAX_VALUE
    return this.profileVersionSequencer.incrementAndGet();
  }

  /**
   * Generates a serial number for identifying a logical resource. Later instances of the same
   * logical resource will have a greater serial number than earlier instances. This number
   * increments statically throughout the life of this JVM. Rollover to negative is allowed.
   *
   * @see #ILLEGAL_SERIAL
   * @return the new serial number
   */
  public static int createSerialNumber() {
    for (;;) {
      // NOTE: AtomicInteger should rollover if value is Integer.MAX_VALUE
      int result = serialNumberSequencer.incrementAndGet();
      if (result != ILLEGAL_SERIAL) {
        return result;
      }
    }
  }

  public DistributionManager getDistributionManager() {
    return getAdvisee().getDistributionManager();
  }

  /**
   * Like getDistributionManager but does not check
   * that the DistributedSystem is still connected
   */
  private DistributionManager getDistributionManagerWithNoCheck() {
    return getAdvisee().getSystem().getDM();
  }

  public DistributionAdvisee getAdvisee() {
    return this.advisee;
  }

  /**
   * Free up resources used by this advisor once it is no longer being used.
   *
   * @since GemFire 3.5
   */
  public void close() {
    try {
      synchronized (this) {
        this.membershipClosed = true;
        synchronized (this.opCountLock) {
          this.previousVersionOpCount = 0;
          this.currentVersionOpCount = 0;
        }
      }
      getDistributionManager().removeMembershipListener(this.ml);
    } catch (CancelException e) {
      // if distribution has stopped, above is a no-op.
    } catch (IllegalArgumentException ignore) {
      // this is thrown if the listener is no longer registered
    }
  }

  /**
   * Atomically add listener to the list to receive notification when a *new* profile is added or a
   * profile is removed, and return adviseGeneric(). This ensures that no membership listener calls
   * are missed, but there is no guarantee that there won't be redundant listener calls.
   */
  public Set addMembershipListenerAndAdviseGeneric(MembershipListener listener) {
    initializationGate(); // exchange profiles before acquiring lock on membershipListeners
    membershipListeners.putIfAbsent(listener, Boolean.TRUE);
    return adviseGeneric();
  }


  /**
   * Add listener to the list to receive notification when a profile is added or removed. Note that
   * there is no guarantee that the listener will not get redundant calls, but the listener is
   * guaranteed to get a call.
   */
  public void addMembershipListener(MembershipListener listener) {
    membershipListeners.putIfAbsent(listener, Boolean.TRUE);
  }

  public boolean addProfileChangeListener(ProfileListener listener) {
    return null == profileListeners.putIfAbsent(listener, Boolean.TRUE);
  }

  public boolean removeProfileChangeListener(ProfileListener listener) {
    return profileListeners.remove(listener) != null;
  }

  /**
   * Remove listener from the list to receive notification when a provile is added or removed.
   *
   * @return true if listener was in the list
   */
  public boolean removeMembershipListener(MembershipListener listener) {
    return membershipListeners.remove(listener) != null;
  }

  /** Called by CreateRegionProcessor after it does its own profile exchange */
  public void setInitialized() {
    synchronized (this.initializeLock) {
      this.initialized = true;
    }
  }

  /** Return true if exchanged profiles */
  public boolean initializationGate() {
    if (this.initialized) {
      return false;
    }
    synchronized (this.initializeLock) {
      if (!this.initialized) {
        exchangeProfiles();
        return true;
      }
    }
    return false;
  }

  // wait for pending profile exchange to complete before returning
  public boolean isInitialized() {
    synchronized (this.initializeLock) {
      return this.initialized;
    }
  }

  /**
   * Polls the isInitialized state. Unlike {@link #isInitialized} it will not wait for it to become
   * initialized if it is in the middle of being initialized.
   *
   * @since GemFire 5.7
   */
  public boolean pollIsInitialized() {
    return this.initialized;
  }

  /**
   * Dumps out all profiles in this advisor.
   *
   * @param infoMsg prefix message to log
   */

  public void dumpProfiles(String infoMsg) {
    Profile[] profs = this.profiles;
    final StringBuffer buf = new StringBuffer(2000);
    if (infoMsg != null) {
      buf.append(infoMsg);
      buf.append(": ");
    }
    buf.append("FYI, DUMPING PROFILES IN ");
    buf.append(toString());
    buf.append(":\n");

    buf.append("My Profile=");
    buf.append(getAdvisee().getProfile());
    buf.append("\nOther Profiles:\n");

    for (int i = 0; i < profs.length; i++) {
      buf.append("\t");
      buf.append(profs[i].toString());
      buf.append("\n");
    }
    if (logger.isDebugEnabled()) {
      logger.debug(buf.toString());
    }
  }

  /**
   * Create or update a profile for a remote counterpart.
   *
   * @param profile the profile, referenced by this advisor after this method returns.
   */
  public boolean putProfile(Profile profile) {
    return putProfile(profile, false);
  }

  public synchronized boolean putProfile(Profile newProfile, boolean forceProfile) {
    try {
      return doPutProfile(newProfile, forceProfile);
    } finally {
      if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
        logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE, "putProfile exiting {}",
            toStringWithProfiles());
      }
    }
  }

  /**
   * Return true if the memberId on the specified Profile is a current member of the distributed
   * system.
   *
   * @since GemFire 5.7
   */
  protected boolean isCurrentMember(Profile p) {
    return getDistributionManager().isCurrentMember(p.getDistributedMember());
  }

  /**
   * Update the Advisor with profiles describing remote instances of the
   * {@link DistributionAdvisor#getAdvisee()}. Profile information is versioned via
   * {@link Profile#getVersion()} and may be ignored if an older version is received after newer
   * versions.
   *
   * @param newProfile the profile to add
   * @param forceProfile true will force profile to be added even if member is not in distributed
   *        view (should only ever be true for tests that need to inject a bad profile)
   *
   * @return true if the profile was applied, false if the profile was ignored
   */
  private synchronized boolean doPutProfile(Profile newProfile, boolean forceProfile) {
    assert newProfile != null;
    // prevent putting of profile that is gone from the view
    if (!forceProfile) {
      // ensure member is in distributed system view
      if (!isCurrentMember(newProfile)) {
        if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
          logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
              "putProfile: ignoring {}; not in current view for {}",
              newProfile.getDistributedMember(), getAdvisee().getFullPath());
        }

        // member is no longer in system so do nothing
        return false;
      }
    }

    // prevent putting of profile for which we already received removal msg
    Integer removedSerialNumber = (Integer) this.removedProfiles.get(newProfile.getId());
    if (removedSerialNumber != null
        && !isNewerSerialNumber(newProfile.getSerialNumber(), removedSerialNumber.intValue())) {
      // removedProfile exists and newProfile is NOT newer so do nothing
      if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
        logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
            "putProfile: Skipping putProfile: {} is not newer than serialNumber {} for {}",
            newProfile, removedSerialNumber, getAdvisee().getFullPath());
      }
      return false;
    }

    // compare newProfile to oldProfile if one is found
    Profile oldProfile = getProfile(newProfile.getId());
    final boolean isTraceEnabled_DistributionAdvisor =
        logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE);
    if (isTraceEnabled_DistributionAdvisor) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
          "putProfile: Updating existing profile: {} with new profile: {} for {}", oldProfile,
          newProfile, getAdvisee().getFullPath());
    }
    if (oldProfile != null && !isNewerProfile(newProfile, oldProfile)) {
      // oldProfile exists and newProfile is NOT newer so do nothing
      if (isTraceEnabled_DistributionAdvisor) {
        logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
            "putProfile: Ignoring {} because it's older than or same as {} for {}", newProfile,
            oldProfile, getAdvisee().getFullPath());
      }
      return false;
    }

    // handle membershipVersion for state flush
    if (newProfile.initialMembershipVersion == 0) {
      if (oldProfile != null) {
        newProfile.initialMembershipVersion = oldProfile.initialMembershipVersion;
      } else {
        if (!membershipClosed) {
          membershipVersion++;
          if (logger.isTraceEnabled(LogMarker.STATE_FLUSH_OP_VERBOSE)) {
            logger.trace(LogMarker.STATE_FLUSH_OP_VERBOSE,
                "StateFlush incremented membership version: {}", membershipVersion);
          }
          newProfile.initialMembershipVersion = membershipVersion;
          synchronized (this.opCountLock) {
            previousVersionOpCount += currentVersionOpCount;
            currentVersionOpCount = 0;
          }
        }
      }
    } else {
      forceNewMembershipVersion();
    }

    if (isTraceEnabled_DistributionAdvisor) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
          "DistributionAdvisor ({}) putProfile: {}", this, newProfile);
    }
    boolean doAddOrUpdate = evaluateProfiles(newProfile, oldProfile);
    if (!doAddOrUpdate) {
      return false;
    }
    if (basicAddProfile(newProfile)) {
      profileCreated(newProfile);
      notifyListenersProfileAdded(newProfile);
      notifyListenersMemberAdded(newProfile.getDistributedMember());
    } else {
      notifyListenersProfileUpdated(newProfile);
      profileUpdated(newProfile);
    }
    return true;
  }

  /**
   * A callback to sub-classes for extra validation logic
   *
   * @return true if the change from old to new is valid
   */
  protected boolean evaluateProfiles(Profile newProfile, Profile oldProfile) {
    return true;
  }

  /**
   * Returns true if newProfile is newer than oldProfile. This is determined by comparing
   * {@link Profile#getSerialNumber()} and {@link Profile#getVersion()}. If the old versioning
   * number being compared is above {@link #ROLLOVER_THRESHOLD_UPPER} and the new versioning number
   * is below {@link #ROLLOVER_THRESHOLD_LOWER} then a rollover is assumed to have occurred, which
   * means the new versioning number is newer.
   *
   * @param newProfile the newer profile
   * @param oldProfile the older profile
   * @return true if newProfile is newer than oldProfile
   */
  protected boolean isNewerProfile(Profile newProfile, Profile oldProfile) {
    Assert.assertHoldsLock(this, true);
    boolean isNewer = true;

    // force version comparison
    int oldSerial = oldProfile.getSerialNumber();
    int newSerial = newProfile.getSerialNumber();
    // boolean serialRolled = oldSerial > 0 && newSerial < 0;
    boolean serialRolled =
        oldSerial > ROLLOVER_THRESHOLD_UPPER && newSerial < ROLLOVER_THRESHOLD_LOWER;

    int oldVersion = oldProfile.getVersion();
    int newVersion = newProfile.getVersion();
    // boolean versionRolled = oldVersion > 0 && newVersion < 0;
    boolean versionRolled =
        oldVersion > ROLLOVER_THRESHOLD_UPPER && newVersion < ROLLOVER_THRESHOLD_LOWER;

    boolean newIsNewer = false;
    if (oldSerial == newSerial) {
      // if region serial is same, compare versions
      newIsNewer = versionRolled || oldVersion < newVersion;
    } else {
      // compare region serial
      newIsNewer = serialRolled || oldSerial < newSerial;
    }
    if (!newIsNewer) {
      isNewer = false;
    }
    return isNewer;
  }

  /**
   * Compare two serial numbers
   *
   * @return return true if the first serial number (newSerialNumber) is more recent
   */
  public static boolean isNewerSerialNumber(int newSerialNumber, int oldSerialNumber) {
    boolean serialRolled =
        oldSerialNumber > ROLLOVER_THRESHOLD_UPPER && newSerialNumber < ROLLOVER_THRESHOLD_LOWER;
    return serialRolled || oldSerialNumber < newSerialNumber;
  }

  /**
   * Create a new version of the membership profile set. This is used in flushing state out of the
   * VM for previous versions of the set.
   *
   * @since GemFire 5.1
   */
  public synchronized void forceNewMembershipVersion() {
    if (!membershipClosed) {
      membershipVersion++;
      if (logger.isTraceEnabled(LogMarker.STATE_FLUSH_OP_VERBOSE)) {
        logger.trace(LogMarker.STATE_FLUSH_OP_VERBOSE,
            "StateFlush forced new membership version: {}", membershipVersion);
      }
      synchronized (this.opCountLock) {
        previousVersionOpCount += currentVersionOpCount;
        currentVersionOpCount = 0;
        if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_STATE_FLUSH_VERBOSE)) {
          logger.trace(LogMarker.DISTRIBUTION_STATE_FLUSH_VERBOSE,
              "advisor for {} forced new membership version to {} previousOpCount={}", getAdvisee(),
              membershipVersion, previousVersionOpCount);
        }
      }
    }
  }

  /**
   * this method must be invoked at the start of every operation that can modify the state of
   * resource. The return value must be recorded and sent to the advisor in an endOperation message
   * when messages for the operation have been put in the DistributionManager's outgoing "queue".
   *
   * @return the current membership version for this advisor
   * @since GemFire 5.1
   */
  public synchronized long startOperation() {
    if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_STATE_FLUSH_VERBOSE)) {
      logger.trace(LogMarker.DISTRIBUTION_STATE_FLUSH_VERBOSE,
          "startOperation() op count is now {} in view version {}", currentVersionOpCount + 1,
          membershipVersion);
    }
    synchronized (this.opCountLock) {
      currentVersionOpCount++;
      if (logger.isTraceEnabled(LogMarker.STATE_FLUSH_OP_VERBOSE)) {
        logger.trace(LogMarker.STATE_FLUSH_OP_VERBOSE, "StateFlush current opcount incremented: {}",
            currentVersionOpCount);
      }
    }
    return membershipVersion;
  }

  /**
   * This method must be invoked when messages for an operation have been put in the
   * DistributionManager's outgoing queue.
   *
   * @param version The membership version returned by startOperation
   * @since GemFire 5.1
   */
  public synchronized long endOperation(long version) {
    synchronized (this.opCountLock) {
      if (version == membershipVersion) {
        currentVersionOpCount--;
        if (logger.isTraceEnabled(LogMarker.STATE_FLUSH_OP_VERBOSE)) {
          logger.trace(LogMarker.STATE_FLUSH_OP_VERBOSE,
              "StateFlush current opcount deccremented: {}", currentVersionOpCount);
        }
      } else {
        previousVersionOpCount--;
        if (logger.isTraceEnabled(LogMarker.STATE_FLUSH_OP_VERBOSE)) {
          logger.trace(LogMarker.STATE_FLUSH_OP_VERBOSE,
              "StateFlush previous opcount incremented: {}", previousVersionOpCount);
        }
      }
    }
    return membershipVersion;
  }

  public void waitForCurrentOperations() {
    long timeout =
        1000L * this.getDistributionManager().getSystem().getConfig().getAckWaitThreshold();
    waitForCurrentOperations(timeout);
  }

  /**
   * wait for the current operations being sent on views prior to the joining of the given member to
   * be placed on communication channels before returning
   *
   * @since GemFire 5.1
   */
  public void waitForCurrentOperations(long timeout) {
    // CacheProfile profile = (CacheProfile)getProfile(member);
    // long targetVersion = profile.initialMembershipVersion - 1;

    // this may wait longer than it should if the membership version changes, dumping
    // more operations into the previousVersionOpCount
    long startTime = System.currentTimeMillis();
    long warnTime = startTime + timeout;
    long quitTime = warnTime + timeout - 1000L;
    boolean warned = false;
    final boolean isDebugEnabled_STATE_FLUSH_OP =
        logger.isTraceEnabled(LogMarker.STATE_FLUSH_OP_VERBOSE);
    while (true) {
      long opCount;
      synchronized (this.opCountLock) {
        opCount = this.previousVersionOpCount;
      }
      if (opCount <= 0) {
        break;
      }
      // The advisor's close() method will set the pVOC to zero. This loop
      // must not terminate due to cache closure until that happens.
      // See bug 34361 comment 79
      if (isDebugEnabled_STATE_FLUSH_OP) {
        logger.trace(LogMarker.STATE_FLUSH_OP_VERBOSE,
            "Waiting for current operations to finish({})", opCount);
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        throw new GemFireIOException("State flush interrupted");
      }
      long now = System.currentTimeMillis();
      if ((!warned) && System.currentTimeMillis() >= warnTime) {
        warned = true;
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.DistributionAdvisor_0_SEC_HAVE_ELAPSED_WHILE_WAITING_FOR_CURRENT_OPERATIONS_TO_DISTRIBUTE,
            Long.toString((warnTime - startTime) / 1000L)));
      } else if (warned && (now >= quitTime)) {
        // OSProcess.printStacks(0);
        throw new GemFireIOException(
            "Current operations did not distribute within " + (now - startTime) + " milliseconds");
      }
    }
    if (this.membershipClosed) {
      if (isDebugEnabled_STATE_FLUSH_OP) {
        logger.trace(LogMarker.STATE_FLUSH_OP_VERBOSE,
            "State Flush stopped waiting for operations to distribute because advisor has been closed");
      }
    }
  }

  /**
   * Bypass the distribution manager and ask the membership manager directly if a given member is
   * still in the view.
   *
   * We need this because we're asking membership questions from within listeners, and we don't know
   * whether the DM's membership listener fires before or after our own.
   *
   * @param id member we are asking about
   * @return true if we are still in the JGroups view (must return false if id == null)
   */
  protected boolean stillInView(ProfileId id) {
    if (id instanceof InternalDistributedMember) {
      InternalDistributedMember memberId = (InternalDistributedMember) id;
      return this.getDistributionManager().getViewMembers().contains(memberId);
    } else {
      // if id is not a InternalDistributedMember then return false
      return false;
    }
  }

  /**
   * Given member is no longer pertinent to this advisor; remove it.
   *
   * This is often overridden in subclasses, but they need to defer to their superclass at some
   * point in their re-implementation.
   *
   * @param memberId the member to remove
   * @param crashed true if the member did not leave normally
   * @return true if it was being tracked
   */
  private boolean basicRemoveId(ProfileId memberId, boolean crashed, boolean destroyed) {
    final boolean isDebugEnabled = logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE);
    if (isDebugEnabled) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE, "DistributionAdvisor ({}) removeId {}",
          this, memberId);
    }

    Profile profileRemoved = basicRemoveMemberId(memberId);
    if (profileRemoved == null) {
      if (isDebugEnabled) {
        logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
            "DistributionAdvisor.removeId: no profile to remove for {}", memberId);
      }
      return false;
    }
    if (isDebugEnabled) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
          "DistributionAdvisor.removeId: removed profile for {}", memberId);
    }
    profileRemoved(profileRemoved);
    notifyListenersProfileRemoved(profileRemoved, destroyed);
    notifyListenersMemberRemoved(profileRemoved.getDistributedMember(), crashed);
    profileRemoved.cleanUp();
    return true;
  }

  /**
   * Removes the specified profile if it is registered with this advisor.
   *
   * @return true if it was registered; false if not.
   * @since GemFire 5.7
   */
  public boolean removeProfile(Profile profile, boolean destroyed) {
    return removeId(profile.getId(), false, destroyed, false/* fromMembershipListener */);
  }

  /**
   * Removes the profile for the given member. This method is meant to be overriden by subclasses.
   *
   * @param memberId the member whose profile should be removed
   * @param crashed true if the member crashed
   * @param fromMembershipListener true if this call is a result of MembershipEvent invocation
   *        (fixes #42000)
   * @return true when the profile was removed, false otherwise
   */
  public boolean removeId(final ProfileId memberId, boolean crashed, boolean destroyed,
      boolean fromMembershipListener) {
    boolean result;
    try {
      result = doRemoveId(memberId, crashed, destroyed, fromMembershipListener);
    } finally {
      if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
        logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE, "removeId {} exiting {}", memberId,
            toStringWithProfiles());
      }
    }
    return result;
  }

  private boolean doRemoveId(ProfileId memberId, boolean crashed, boolean destroyed,
      boolean fromMembershipListener) {
    final boolean isDebugEnabled_DA = logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE);
    if (isDebugEnabled_DA) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
          "removeId: removing member {} from resource {}", memberId, getAdvisee().getFullPath());
    }
    synchronized (this) {
      // If the member has disappeared, completely remove
      if (!fromMembershipListener) {
        boolean result = false;
        // Assert.assertTrue(!crashed); // should not get here :-)

        // Is there an existing profile? If so, add it to list of those removed.
        Profile profileToRemove = getProfile(memberId);
        while (profileToRemove != null) {
          result = true;
          if (isDebugEnabled_DA) {
            logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE, "removeId: tracking removal of {}",
                profileToRemove);
          }
          this.removedProfiles.put(profileToRemove.getDistributedMember(),
              Integer.valueOf(profileToRemove.getSerialNumber()));
          basicRemoveId(profileToRemove.getId(), crashed, destroyed);
          profileToRemove = getProfile(memberId);
        }
        return result;
      } else {
        // Garbage collect; this profile is no longer pertinent
        this.removedProfiles.remove(memberId);
        boolean result = basicRemoveId(memberId, crashed, destroyed);
        while (basicRemoveId(memberId, crashed, destroyed)) {
          // keep removing profiles that match until we have no more
        }
        return result;
      }
    }
  }

  /**
   * Removes the profile for the specified member and serial number
   *
   * @param memberId the member to remove the profile for
   * @param serialNum specific serial number to remove
   * @return true if a matching profile for the member was found
   */
  public boolean removeIdWithSerial(InternalDistributedMember memberId, int serialNum,
      boolean regionDestroyed) {
    if (logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE)) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
          "removeIdWithSerial: removing member {} with serial {} from resource {}", memberId,
          serialNum, getAdvisee().getName());
    }
    Assert.assertTrue(serialNum != ILLEGAL_SERIAL);
    return updateRemovedProfiles(memberId, serialNum, regionDestroyed);
  }

  /**
   * Update the list of removed profiles based on given serial number, and ensure that the given
   * member is no longer in the list of bucket owners.
   *
   * @param memberId member to remove
   * @param serialNum serial number
   * @return true if this member was an owner
   */
  private synchronized boolean updateRemovedProfiles(InternalDistributedMember memberId,
      int serialNum, boolean regionDestroyed) {
    final boolean isDebugEnabled_DA = logger.isTraceEnabled(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE);
    if (isDebugEnabled_DA) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
          "updateRemovedProfiles: ensure member {} with serial {} is removed from region {}",
          memberId, serialNum, getAdvisee().getFullPath());
    }

    boolean removedId = false;
    if (stillInView(memberId)) {
      boolean isNews = true;

      // If existing profile is newer, just return
      Profile profileToRemove = getProfile(memberId);
      if (profileToRemove != null) {
        if (isNewerSerialNumber(profileToRemove.serialNumber, serialNum)) {
          if (isDebugEnabled_DA) {
            logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
                "updateRemovedProfiles: member {} has profile {} which is newer than serial {}",
                memberId, profileToRemove, serialNum);
          }
          // We have a current profile for this member, but its serial number
          // is more recent than the removal that was requested.

          // Do not remove our existing profile, and do not update
          // removedProfiles.
          isNews = false;
        }
      }

      if (isNews) {
        // Is this a more recent removal than we have recorded?
        // If not, do not remove any existing profile, and do not
        // update removedProfiles
        Integer oldSerial = (Integer) this.removedProfiles.get(memberId);
        if (oldSerial != null && isNewerSerialNumber(oldSerial.intValue(), serialNum)) {
          if (isDebugEnabled_DA) {
            logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
                "updateRemovedProfiles: member {} sent removal of serial {} but we hae already removed {}",
                memberId, serialNum, oldSerial);
          }
          isNews = false;
        }
      }

      if (isNews) {
        if (isDebugEnabled_DA) {
          logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
              "updateRemovedProfiles: adding serial {} for member {} to removedProfiles", serialNum,
              memberId);
        }
        // The member is still in the system, and the removal message is
        // a new one. Remember this removal, and ensure that its profile
        // is removed from this bucket.
        this.removedProfiles.put(memberId, Integer.valueOf(serialNum));

        // Only remove profile if this removal is more recent than our
        // current state
        removedId = basicRemoveId(memberId, false, regionDestroyed);
      }
    } // isCurrentMember
    else {
      // If the member has disappeared, completely remove (garbage collect)
      if (isDebugEnabled_DA) {
        logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE,
            "updateRemovedProfiles: garbage collecting member {}", memberId);
      }
      this.removedProfiles.remove(memberId);

      // Always make sure that this member is removed from the advisor
      removedId = basicRemoveId(memberId, false, regionDestroyed);
    }

    if (isDebugEnabled_DA) {
      logger.trace(LogMarker.DISTRIBUTION_ADVISOR_VERBOSE, "updateRemovedProfiles: removedId = {}",
          removedId);
    }

    return removedId;
  }

  /**
   * Indicate whether given member is being tracked
   *
   * @param memberId the member
   * @return true if the member was being tracked
   */
  public synchronized boolean containsId(InternalDistributedMember memberId) {
    return (indexOfMemberId(memberId) > -1);
  }


  public synchronized int getNumProfiles() {
    return this.numActiveProfiles;
  }

  /**
   * Caller must be synchronized on this. Overridden in BucketAdvisor.
   */
  protected void setNumActiveProfiles(int newValue) {
    this.numActiveProfiles = newValue;
  }

  public Profile getProfile(ProfileId id) {
    Profile[] allProfiles = this.profiles; // volatile read
    boolean isIDM = (id instanceof InternalDistributedMember);
    for (int i = 0; i < allProfiles.length; i++) {
      if (isIDM) {
        if (allProfiles[i].getDistributedMember().equals(id)) {
          return allProfiles[i];
        }
      } else {
        if (allProfiles[i].getId().equals(id)) {
          return allProfiles[i];
        }
      }
    }
    return null;
  }

  /** exchange profiles to initialize this advisor */
  public void exchangeProfiles() {
    Assert.assertHoldsLock(this, false); // causes deadlock
    Assert.assertHoldsLock(this.initializeLock, true);
    new UpdateAttributesProcessor(getAdvisee()).distribute(true);
    setInitialized();
  }

  /** Creates the current distribution profile for this member */
  public Profile createProfile() {
    Profile newProfile =
        instantiateProfile(getDistributionManager().getId(), incrementAndGetVersion());
    getAdvisee().fillInProfile(newProfile);
    return newProfile;
  }

  /** Instantiate new distribution profile for this member */
  protected Profile instantiateProfile(InternalDistributedMember memberId, int version) {
    return new Profile(memberId, version);
  }

  /**
   * Provide recipient information for any other operation. Returns the set of members that have
   * remote counterparts.
   *
   * @return Set of Serializable members; no reference to Set kept by advisor so caller is free to
   *         modify it
   */
  public Set<InternalDistributedMember> adviseGeneric() {
    return adviseFilter(null);
  }

  /**
   * Provide recipients for profile exchange, called by UpdateAttributesProcessor and
   * CreateRegionProcessor. Can not be initialized at this point because it is only called in the
   * following scenarios: 1) We're doing a lazy initialization and synchronization on initializeLock
   * prevents other threads from causing initialization on this advisor. 2) We're creating a new
   * region and doing profile exchange as part of region initialization, in which case no other
   * threads have access to the region or this advisor.
   */
  public Set adviseProfileExchange() {
    // Get the list of recipients from the nearest initialized advisor
    // in the parent chain
    Assert.assertTrue(!isInitialized());
    DistributionAdvisor advisor;
    DistributionAdvisee advisee = getAdvisee();
    do {
      advisee = advisee.getParentAdvisee();
      if (advisee == null)
        return getDefaultDistributionMembers();
      advisor = advisee.getDistributionAdvisor();
    } while (!advisor.isInitialized());
    // do not call adviseGeneric because we don't want to trigger another
    // profile exchange on the parent
    return advisor.adviseFilter(null);
  }

  /**
   * Returns a set of the members this advisor should distribute to by default
   *
   * @since GemFire 5.7
   */
  @SuppressWarnings("unchecked")
  protected Set<InternalDistributedMember> getDefaultDistributionMembers() {
    if (!useAdminMembersForDefault()) {
      return getDistributionManager().getOtherDistributionManagerIds();
    } else {
      return getDistributionManager().getAllOtherMembers();
    }
  }

  /**
   * Returns true if all members including ADMIN are required for distribution of update attributes
   * message by {@link #getDefaultDistributionMembers()}.
   */
  public boolean useAdminMembersForDefault() {
    return false;
  }

  private void notifyListenersMemberAdded(InternalDistributedMember member) {
    Iterator it = membershipListeners.keySet().iterator();
    while (it.hasNext()) {
      try {
        ((MembershipListener) it.next()).memberJoined(getDistributionManagerWithNoCheck(), member);
      } catch (Exception e) {
        logger.warn("Ignoring exception during member joined listener notification", e);
      }
    }
  }

  private void notifyListenersMemberRemoved(InternalDistributedMember member, boolean crashed) {
    Iterator it = membershipListeners.keySet().iterator();
    while (it.hasNext()) {
      try {
        ((MembershipListener) it.next()).memberDeparted(getDistributionManagerWithNoCheck(), member,
            crashed);
      } catch (Exception e) {
        logger.warn("Ignoring exception during member departed listener notification", e);
      }
    }
  }

  private void notifyListenersProfileRemoved(Profile profile, boolean destroyed) {
    Iterator it = profileListeners.keySet().iterator();
    while (it.hasNext()) {
      ((ProfileListener) it.next()).profileRemoved(profile, destroyed);
    }
  }

  private void notifyListenersProfileAdded(Profile profile) {
    Iterator it = profileListeners.keySet().iterator();
    while (it.hasNext()) {
      ((ProfileListener) it.next()).profileCreated(profile);
    }
  }

  private void notifyListenersProfileUpdated(Profile profile) {
    Iterator it = profileListeners.keySet().iterator();
    while (it.hasNext()) {
      ((ProfileListener) it.next()).profileUpdated(profile);
    }
  }

  /**
   * Template method for sub-classes to override. Method is invoked after a new profile is
   * created/added to profiles.
   *
   * @param profile the created profile
   */
  protected void profileCreated(Profile profile) {}

  /**
   * Template method for sub-classes to override. Method is invoked after a profile is updated in
   * profiles.
   *
   * @param profile the updated profile
   */
  protected void profileUpdated(Profile profile) {}

  /**
   * Template method for sub-classes to override. Method is invoked after a profile is removed from
   * profiles.
   *
   * @param profile the removed profile
   */
  protected void profileRemoved(Profile profile) {}

  /** All advise methods go through this method */
  protected Set<InternalDistributedMember> adviseFilter(Filter f) {
    initializationGate();
    if (disabled) {
      if (logger.isDebugEnabled()) {
        logger.debug("Intelligent Messaging Disabled");
      }
      return getDefaultDistributionMembers();
    }
    Set<InternalDistributedMember> recipients = null;
    Profile[] locProfiles = this.profiles; // grab current profiles
    for (int i = 0; i < locProfiles.length; i++) {
      Profile profile = locProfiles[i];
      if (f == null || f.include(profile)) {
        if (recipients == null) {
          recipients = new HashSet<InternalDistributedMember>();
        }
        recipients.add(profile.getDistributedMember());
      }
    }
    if (recipients == null) {
      return Collections.emptySet();
    } else {
      return recipients;
    }
  }

  /**
   * This method calls filter->include on every profile until include returns true.
   *
   * @return false if all filter->include calls returns false; otherwise true.
   **/
  protected boolean satisfiesFilter(Filter f) {
    initializationGate();
    if (disabled) {
      if (logger.isDebugEnabled()) {
        logger.debug("Intelligent Messaging Disabled");
      }
      return !getDefaultDistributionMembers().isEmpty();
    }
    Profile[] locProfiles = this.profiles; // grab current profiles
    for (Profile p : locProfiles) {
      if (f.include(p)) {
        return true;
      }
    }
    return false;
  }

  /**
   * A visitor interface for all the available profiles used by
   * {@link DistributionAdvisor#accept(ProfileVisitor, Object)}. Unlike the {@link Filter} class
   * this does not assume of two state visit of inclusion or exclusion rather allows manipulation of
   * an arbitrary aggregator that has been passed to the {@link #visit} method. In addition this is
   * public for use by other classes.
   */
  public interface ProfileVisitor<T> {

    /**
     * Visit a given {@link Profile} accumulating the results in the given aggregate. Returns false
     * when the visit has to be terminated.
     *
     * @param advisor the DistributionAdvisor that invoked this visitor
     * @param profile the profile being visited
     * @param profileIndex the index of current profile
     * @param numProfiles the total number of profiles being visited
     * @param aggregate result aggregated so far, if any
     *
     * @return false if the visit has to be terminated immediately and false otherwise
     */
    boolean visit(DistributionAdvisor advisor, Profile profile, int profileIndex, int numProfiles,
        T aggregate);
  }

  /**
   * Invoke the given {@link ProfileVisitor} on all the {@link Profile}s exiting when the
   * {@link ProfileVisitor#visit} method returns false. Unlike the {@link #adviseFilter(Filter)}
   * method this does assume the return type to be a Set of qualifying members rather allows for
   * population of an arbitrary aggregator passed as the argument to this method.
   *
   * @param <T> the type of object used for aggregation of results
   * @param visitor the {@link ProfileVisitor} to use for the visit
   * @param aggregate an aggregate object that will be used to for aggregation of results by the
   *        {@link ProfileVisitor#visit} method; this allows the {@link ProfileVisitor} to not
   *        maintain any state so that in many situations a global static object encapsulating the
   *        required behaviour will work
   *
   * @return true if all the profiles were visited and false if the {@link ProfileVisitor#visit} cut
   *         it short by returning false
   */
  public <T> boolean accept(ProfileVisitor<T> visitor, T aggregate) {
    initializationGate();
    final Profile[] locProfiles = this.profiles; // grab current profiles
    final int numProfiles = locProfiles.length;
    Profile p;
    for (int index = 0; index < numProfiles; ++index) {
      p = locProfiles[index];
      if (!visitor.visit(this, p, index, numProfiles, aggregate)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get an unmodifiable list of the <code>Profile</code>s that match the given <code>Filter</code>.
   *
   * @since GemFire 5.7
   */
  protected List/* <Profile> */ fetchProfiles(Filter f) {
    initializationGate();
    List result = null;
    Profile[] locProfiles = this.profiles; // grab current profiles
    for (int i = 0; i < locProfiles.length; i++) {
      Profile profile = locProfiles[i];
      if (f == null || f.include(profile)) {
        if (result == null) {
          result = new ArrayList(locProfiles.length);
        }
        result.add(profile);
      }
    }
    if (result == null) {
      result = Collections.EMPTY_LIST;
    } else {
      result = Collections.unmodifiableList(result);
    }
    return result;
  }

  /** Provide recipients for profile update. */
  public Set adviseProfileUpdate() {
    return adviseGeneric();
  }

  /**
   * Provide recipients for profile remove.
   *
   * @since GemFire 5.7
   */
  public Set adviseProfileRemove() {
    return adviseGeneric();
  }

  /**
   * @return true if new profile added, false if already had profile (but profile is still replaced
   *         with new one)
   */
  // must synchronize when modifying profile array
  protected synchronized boolean basicAddProfile(Profile p) {
    // don't add more than once, but replace existing profile
    // try {

    int index = indexOfMemberId(p.getId());
    if (index >= 0) {
      Profile[] oldProfiles = this.profiles; // volatile read
      oldProfiles[index] = p;
      this.profiles = oldProfiles; // volatile write
      return false;
    }

    // minimize volatile reads by copying ref to local var
    Profile[] snap = this.profiles; // volatile read
    Profile[] newProfiles = (Profile[]) ArrayUtils.insert(snap, snap.length, p);
    Assert.assertTrue(newProfiles != null);

    // System.out.println("newprofiles = " + newProfiles.length);
    // for (int i = 0; i < newProfiles.length; i ++)
    // System.out.println("profile " + i + ": " + newProfiles[i].getId().toString());

    this.profiles = newProfiles; // volatile write
    setNumActiveProfiles(newProfiles.length);

    return true;
    // }
    // finally {
    // Assert.assertTrue(indexOfMemberId(p.getId()) >= 0);
    // boolean containsOne = false;
    // for (int i = 0; i < this.profiles.length; i++) {
    // if (this.profiles[i].getId() == p.getId()) {
    // Assert.assertTrue(!containsOne);
    // containsOne = true;
    // }
    // }
    // Assert.assertTrue(containsOne);
    // }
  }


  // must synchronize when modifying profile array
  /**
   * Perform work of removing the given member from this advisor.
   */
  private synchronized Profile basicRemoveMemberId(ProfileId id) {
    // try {
    int i = indexOfMemberId(id);
    if (i >= 0) {
      Profile profileRemoved = this.profiles[i];
      basicRemoveIndex(i);
      return profileRemoved;
    } else
      return null;
    // } finally {
    // Assert.assertTrue(-1 == indexOfMemberId(id));
    // }
  }

  protected int indexOfMemberId(ProfileId id) {
    Assert.assertHoldsLock(this, true);
    Profile[] profs = this.profiles; // volatile read
    for (int i = 0; i < profs.length; i++) {
      Profile p = profs[i];
      if (id instanceof InternalDistributedMember) {
        if (p.getDistributedMember().equals(id))
          return i;
      } else {
        if (p.getId().equals(id))
          return i;
      }
    }
    return -1;
  }

  private void basicRemoveIndex(int index) {
    Assert.assertHoldsLock(this, true);
    // minimize volatile reads by copying ref to local var
    Profile[] oldProfiles = this.profiles; // volatile read
    Profile[] newProfiles = new Profile[oldProfiles.length - 1];
    System.arraycopy(oldProfiles, 0, newProfiles, 0, index);
    System.arraycopy(oldProfiles, index + 1, newProfiles, index, newProfiles.length - index);
    this.profiles = newProfiles; // volatile write
    if (this.numActiveProfiles > 0) {
      this.numActiveProfiles--;
    }
  }


  /** Filter interface */
  protected interface Filter {
    boolean include(Profile profile);
  }

  /**
   * Marker interface to designate on object that serves and the unique id that identifies a
   * Profile.
   */
  public interface ProfileId {
  }
  /**
   * Profile information for a remote counterpart.
   */
  public static class Profile implements DataSerializableFixedID {
    /** Member for whom this profile represents */
    public InternalDistributedMember peerMemberId;
    /** Serial number incremented every time profile is updated by memberId */
    public int version;
    public int serialNumber = ILLEGAL_SERIAL;
    /**
     * The DistributionAdvisor's membership version where this member was added
     *
     * @since GemFire 5.1
     */
    public transient long initialMembershipVersion;

    /** for internal use, required for DataSerializable.Helper.readObject */
    public Profile() {}

    public Profile(InternalDistributedMember memberId, int version) {
      if (memberId == null) {
        throw new IllegalArgumentException(
            LocalizedStrings.DistributionAdvisor_MEMBERID_CANNOT_BE_NULL.toLocalizedString());
      }
      this.peerMemberId = memberId;
      this.version = version;
    }

    /**
     * Return object that uniquely identifies this profile.
     *
     * @since GemFire 5.7
     */
    public ProfileId getId() {
      return this.peerMemberId;
    }

    public int getVersion() {
      return this.version;
    }

    public int getSerialNumber() {
      return this.serialNumber;
    }

    @Override
    public int hashCode() {
      return this.getId().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this)
        return true;
      if (obj == null)
        return false;
      if (!this.getClass().equals(obj.getClass()))
        return false;
      return getId().equals(((Profile) obj).getId());
    }

    /**
     * Return the DistributedMember associated with this profile
     *
     * @since GemFire 5.0
     */
    public InternalDistributedMember getDistributedMember() {
      return this.peerMemberId;
    }

    public int getDSFID() {
      return DA_PROFILE;
    }

    public void toData(DataOutput out) throws IOException {
      InternalDataSerializer.invokeToData(this.peerMemberId, out);
      out.writeInt(this.version);
      out.writeInt(this.serialNumber);
    }

    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.peerMemberId = new InternalDistributedMember();
      InternalDataSerializer.invokeFromData(this.peerMemberId, in);
      this.version = in.readInt();
      this.serialNumber = in.readInt();
    }

    /**
     * Process add/remove/update of an incoming profile.
     */
    public void processIncoming(ClusterDistributionManager dm, String adviseePath,
        boolean removeProfile, boolean exchangeProfiles, final List<Profile> replyProfiles) {
      // nothing by default; just log that nothing was done
      if (logger.isDebugEnabled()) {
        logger.debug("While processing UpdateAttributes message ignored incoming profile: {}",
            this);
      }
    }

    /**
     * Attempts to process this message with the specified {@link DistributionAdvisee}. Also if
     * exchange profiles then add the profile from {@link DistributionAdvisee} to reply.
     *
     * @param advisee the CacheDistributionAdvisee to apply this profile to
     * @param removeProfile true to remove profile else add profile
     * @param exchangeProfiles true to add the profile to reply
     */
    protected void handleDistributionAdvisee(DistributionAdvisee advisee, boolean removeProfile,
        boolean exchangeProfiles, final List<Profile> replyProfiles) {
      final DistributionAdvisor da;
      if (advisee != null && (da = advisee.getDistributionAdvisor()) != null) {
        if (removeProfile) {
          da.removeProfile(this, false);
        } else {
          da.putProfile(this);
        }
        if (exchangeProfiles) {
          // assume non-null replyProfiles when exchangeProfiles is true
          replyProfiles.add(advisee.getProfile());
        }
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = getToStringHeader();
      sb.append("@").append(System.identityHashCode(this)).append("(");
      fillInToString(sb);
      sb.append(")");
      return sb.toString();
    }

    /**
     * This will be get called when profile will be removed from advisor Do local cleanup in this
     * thread, otherwsie spawn another thread to do cleanup
     */
    public void cleanUp() {

    }

    public StringBuilder getToStringHeader() {
      return new StringBuilder("Profile");
    }

    public void fillInToString(StringBuilder sb) {
      sb.append("memberId=" + this.peerMemberId);
      sb.append("; version=" + this.version);
      sb.append("; serialNumber=" + this.serialNumber);
      sb.append("; initialMembershipVersion=" + this.initialMembershipVersion);
    }

    @Override
    public Version[] getSerializationVersions() {
      return null;
    }
  }
}
