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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.deadlock.MessageDependencyMonitor;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.serialization.DSFIDNotFoundException;
import org.apache.geode.internal.serialization.UnsupportedSerializationVersionException;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.util.Breadcrumbs;
import org.apache.geode.internal.util.concurrent.StoppableCountDownLatch;

/**
 * This class processes responses to {@link DistributionMessage}s. It handles a the generic case of
 * simply waiting for responses from all members. It is intended to be subclassed for special cases.
 *
 * <P>
 *
 * Note that, unlike the reply processors in versions previous to GemFire 2.1, this reply processor
 * is kept entirely in JOM.
 *
 * <p>
 *
 * Recommended usage pattern in subclass...
 *
 * <pre>
 *
 * public void process(DistributionMessage msg) {
 *   try {
 *     ...custom code for subclass goes here...
 *   }
 *   finally {
 *     super.process(msg);
 *   }
 * }
 *
 * </pre>
 *
 * The above usage pattern causes the waitForReplies latch to not be released until after the
 * message has been processed. In addition, it is guaranteed to be released even if the custom
 * subclass code throws a runtime exception.
 *
 * <p>
 *
 * @see MessageWithReply
 * @since GemFire 2.1
 */
public class ReplyProcessor21 implements MembershipListener {

  private static final Logger logger = LogService.getLogger();

  public static final boolean THROW_EXCEPTION_ON_TIMEOUT =
      Boolean.getBoolean("ack-threshold-exception");

  /**
   * the ratio by which ack-severe-alert-threshold is lowered when waiting for a BucketRegion
   * operation
   */
  public static final double PR_SEVERE_ALERT_RATIO;

  /** All live reply processors in this VM */
  @MakeNotStatic
  protected static final ProcessorKeeper21 keeper = new ProcessorKeeper21();

  //////////////////// Instance Methods ////////////////////

  /**
   * The members that haven't replied yet
   *
   * Concurrency: protected by synchronization of itself
   */
  protected final InternalDistributedMember[] members;

  /**
   * Set to true in preWait, set to false in postWait. Used to avoid removing membership listener in
   * Runnable in postWait if we've called waitForReplies again.
   */
  protected volatile boolean waiting = false;

  /**
   * An <code>Exception</code> that occurred when processing a reply
   * <p>
   * Since this is set by the executor and read by the initiating thread, this is a volatile.
   *
   * @see ReplyMessage#getException
   */
  protected volatile ReplyException exception;

  /** Have we heard back from everyone? */
  private volatile boolean done;

  private boolean keeperCleanedUp;

  /** Have we been aborted due to shutdown? */
  protected volatile boolean shutdown;

  /** Semaphore used for wait/notify */
  private final StoppableCountDownLatch latch;

  /** The id of this processor */
  protected int processorId;

  /**
   * Used to get the ack wait threshold (which might change at runtime), etc.
   */
  protected final InternalDistributedSystem system;

  /** the distribution manager - if null, get the manager from the system */
  protected final DistributionManager dmgr;

  /** Start time for replyWait stat, in nanos */
  long statStart;

  /** Start time for ack-wait-threshold, in millis */
  private long initTime;

  /**
   * whether this reply processor should perform severe-alert processing for the message being ack'd
   */
  private boolean severeAlertEnabled;

  /**
   * whether the severe-alert timeout has been reset. This can happen if a member we're waiting for
   * is waiting on a suspect member, for instance.
   */
  private volatile boolean severeAlertTimerReset;

  /**
   * whether this reply processor should shorten severe-alert processing due to another vm waiting
   * on this one. This is a thread-local so that lower level comm layers can tell that the interval
   * should be shortened
   */
  private static final ThreadLocal<Boolean> severeAlertShorten =
      ThreadLocal.withInitial(() -> Boolean.FALSE);

  /**
   * whether the next replyProcessor for the current thread should perform severe-alert processing
   */
  private static final ThreadLocal<Boolean> forceSevereAlertProcessing =
      ThreadLocal.withInitial(() -> Boolean.FALSE);

  ////////////////////// Static Methods /////////////////////

  static {
    String str = System
        .getProperty(DistributionConfig.GEMFIRE_PREFIX + "ack-severe-alert-reduction-ratio", ".80");
    double ratio;
    try {
      ratio = Double.parseDouble(str);
    } catch (NumberFormatException e) {
      System.err.println(
          "Unable to parse gemfire.ack-severe-alert-reduction-ratio setting of \"" + str + "\"");
      ratio = 0.80;
    }
    PR_SEVERE_ALERT_RATIO = ratio;
  }

  /**
   * Returns the <code>ReplyProcessor</code> with the given id, or <code>null</code> if it no longer
   * exists.
   *
   * @param processorId The id of the processor to get
   *
   * @see #getProcessorId
   */
  public static ReplyProcessor21 getProcessor(int processorId) {
    return (ReplyProcessor21) keeper.retrieve(processorId);
  }

  /////////////////////// Constructors //////////////////////

  /**
   * Creates a new <code>ReplyProcessor</code> that wants replies from a single member of a
   * distributed system.
   *
   * @param system the DistributedSystem connection
   * @param member the member this processor wants a reply from
   */
  public ReplyProcessor21(InternalDistributedSystem system, InternalDistributedMember member) {
    this(system, Collections.singleton(member));
  }

  /**
   * Creates a new <code>ReplyProcessor</code> that wants replies from a single member of a
   * distributed system.
   *
   * @param system the DistributedSystem connection
   * @param member the member this processor wants a reply from
   * @param cancelCriterion optional CancelCriterion to use; will use the DistributionManager if
   *        null
   */
  public ReplyProcessor21(InternalDistributedSystem system, InternalDistributedMember member,
      CancelCriterion cancelCriterion) {
    this(system, Collections.singleton(member), cancelCriterion);
  }

  /**
   * Creates a new <code>ReplyProcessor</code> that wants replies from a single member of a
   * distributed system.
   *
   * @param dm the DistributionManager to use for messaging and membership
   * @param member the member this processor wants a reply from
   */
  public ReplyProcessor21(DistributionManager dm, InternalDistributedMember member) {
    this(dm, Collections.singleton(member));
  }

  /**
   * Creates a new <code>ReplyProcessor</code> that wants replies from some number of members of a
   * distributed system. Call this method with
   * {@link ClusterDistributionManager#getDistributionManagerIds} if you want replies from all DMs
   * including the one hosted in this VM.
   *
   * @param dm the DistributionManager to use for messaging and membership
   * @param initMembers the Set of members this processor wants replies from
   */
  public ReplyProcessor21(DistributionManager dm, Collection initMembers) {
    this(dm, initMembers, null);
  }

  /**
   * Creates a new <code>ReplyProcessor</code> that wants replies from some number of members of a
   * distributed system. Call this method with
   * {@link ClusterDistributionManager#getDistributionManagerIds} if you want replies from all DMs
   * including the one hosted in this VM.
   *
   * @param dm the DistributionManager to use for messaging and membership
   * @param initMembers the Set of members this processor wants replies from
   * @param cancelCriterion optional CancelCriterion to use; will use the dm if null
   */
  public ReplyProcessor21(DistributionManager dm, Collection initMembers,
      CancelCriterion cancelCriterion) {
    this(dm, dm.getSystem(), initMembers, cancelCriterion);
  }

  /**
   * Creates a new <code>ReplyProcessor</code> that wants replies from some number of members of a
   * distributed system. Call this method with
   * {@link ClusterDistributionManager#getDistributionManagerIds} if you want replies from all DMs
   * including the one hosted in this VM.
   *
   * @param system the DistributedSystem connection
   * @param initMembers the Set of members this processor wants replies from
   */
  public ReplyProcessor21(InternalDistributedSystem system, Collection initMembers) {
    this(system.getDistributionManager(), system, initMembers, null);
  }

  /**
   * Creates a new <code>ReplyProcessor</code> that wants replies from some number of members of a
   * distributed system. Call this method with
   * {@link ClusterDistributionManager#getDistributionManagerIds} if you want replies from all DMs
   * including the one hosted in this VM.
   *
   * @param system the DistributedSystem connection
   * @param initMembers the Set of members this processor wants replies from
   * @param cancelCriterion optional CancelCriterion to use; will use the DistributedSystem's
   *        DistributionManager if null
   */
  public ReplyProcessor21(InternalDistributedSystem system, Collection initMembers,
      CancelCriterion cancelCriterion) {
    this(system.getDistributionManager(), system, initMembers, cancelCriterion);
  }

  /**
   * Construct new ReplyProcessor21.
   *
   * @param dm the DistributionManager to use for messaging and membership
   * @param system the DistributedSystem connection
   * @param initMembers the collection of members this processor wants replies from
   * @param cancelCriterion optional CancelCriterion to use; will use the dm if null
   */
  private ReplyProcessor21(DistributionManager dm, InternalDistributedSystem system,
      Collection initMembers, CancelCriterion cancelCriterion) {

    this(dm, system, initMembers, cancelCriterion, true);
  }

  /**
   * Construct new ReplyProcessor21.
   *
   * @param dm the DistributionManager to use for messaging and membership
   * @param system the DistributedSystem connection
   * @param initMembers the collection of members this processor wants replies from
   * @param cancelCriterion optional CancelCriterion to use; will use the dm if null
   */
  protected ReplyProcessor21(DistributionManager dm, InternalDistributedSystem system,
      Collection initMembers, CancelCriterion cancelCriterion, boolean register) {
    if (!allowReplyFromSender()) {
      Assert.assertTrue(initMembers != null, "null initMembers");
      Assert.assertTrue(system != null, "null system");
      if (dm != null) {
        Assert.assertTrue(!initMembers.contains(dm.getId()),
            "dm present in initMembers but reply from sender is not allowed");
      }
    }
    this.system = system;
    this.dmgr = dm;
    if (cancelCriterion == null) {
      cancelCriterion = dm.getCancelCriterion();
    }
    this.latch = new StoppableCountDownLatch(cancelCriterion, 1);
    int sz = initMembers.size();
    this.members = new InternalDistributedMember[sz];
    if (sz > 0) {
      int i = 0;
      for (Iterator it = initMembers.iterator(); it.hasNext(); i++) {
        this.members[i] = (InternalDistributedMember) it.next();
      }
    }
    this.done = false;
    this.shutdown = false;
    this.exception = null;
    if (register) {
      register();
    }
    this.keeperCleanedUp = false;
    this.initTime = System.currentTimeMillis();
  }

  protected int register() {
    this.processorId = keeper.put(this);
    return this.processorId;
  }

  ///////////////////// Instance Methods /////////////////////

  /**
   * get the distribution manager for this processor. If the distributed system has a distribution
   * manager, it is used. Otherwise, we expect a distribution manager has been set with
   * setDistributionManager and we'll use that
   */
  public DistributionManager getDistributionManager() {
    try {
      DistributionManager result = this.system.getDistributionManager();
      if (result == null) {
        result = this.dmgr;
        Assert.assertTrue(result != null, "null DistributionManager");
      }
      return result;
    } catch (IllegalStateException ex) {
      // fix for bug 35000
      this.system.getCancelCriterion().checkCancelInProgress(null);
      throw new DistributedSystemDisconnectedException(ex.getMessage());
    }
  }


  /**
   * Override and return true if processor should wait for reply from sender.
   * <p>
   * NOTE: the communication layer does not reliably support sending a message to oneself, so other
   * means must be used to execute the message in this VM. Typically you would set the sender of the
   * message and then invoke its process() method in another thread.
   */
  protected boolean allowReplyFromSender() {
    return false;
  }

  /**
   * The first time a reply is received that contains an exception, the ReplyProcessor will save
   * that exception to be passed along to the waiting client thread. By default, any exception
   * encountered after that will be logged. Subclasses can reimplement this method to disable
   * logging of multiple exceptions.
   */
  protected boolean logMultipleExceptions() {
    return true;
  }


  /**
   * Makes note of a reply from a member. If all members have replied, the waiting thread is
   * signaled. This method can be overridden to provide customized functionality, however the
   * overriden method should always invoke <code>super.process()</code>.
   */
  public void process(DistributionMessage msg) {
    process(msg, true);
  }

  protected void process(DistributionMessage msg, boolean warn) {
    if (logger.isDebugEnabled()) {
      logger.debug("{} got process({}) from {}", this, msg, msg.getSender());
    }
    if (msg instanceof ReplyMessage) {
      ReplyException ex = ((ReplyMessage) msg).getException();
      if (ex != null) {
        if (ex.getCause() instanceof DSFIDNotFoundException) {
          processException(msg, (DSFIDNotFoundException) ex.getCause());
        } else {
          processException(msg, ex);
        }
      }
    }

    final InternalDistributedMember sender = msg.getSender();
    if (!removeMember(sender, false) && warn) {
      // if the member hasn't left the system, something is wrong
      final DistributionManager dm = getDistributionManager(); // fix for bug 33253
      List ids = getDistributionManagerIds();
      if (ids == null || ids.contains(sender)) {
        List viewMembers = dm.getViewMembers();
        if (system.getConfig().getMcastPort() == 0 // could be using multicast & will get responses
                                                   // from everyone
            && (viewMembers == null || viewMembers.contains(sender))) {
          logger.warn(
              "Received reply from member {} but was not expecting one. More than one reply may have been received. The reply that was not expected is: {}",
              new Object[] {sender, msg});
        }
      }
    }
    checkIfDone();
  }


  protected synchronized void processException(DistributionMessage msg, ReplyException ex) {
    processException(ex);
  }

  protected synchronized void processException(ReplyException ex) {
    if (this.exception == null) { // only keep first exception
      this.exception = ex;

    } else if (logMultipleExceptions()) {
      if (!(ex.getCause() instanceof ConcurrentCacheModificationException)) {
        logger.fatal(
            "Exception received in ReplyMessage. Only one exception is passed back to caller. This exception is logged only.",
            ex);
      }
    }
  }

  /**
   * Handle a {@link DSFIDNotFoundException} indicating a message type is not implemented on another
   * server (for example due to different product version). Default implementation logs the
   * exception as severe and moves on.
   *
   * Rationale for default handling: New operations can have caused changes to other newer versioned
   * GFE JVMs that cannot be reverted. So ignoring exceptions is a conservative way considering such
   * scenarios. It will be upto individual messages to handle differently by overriding the above
   * method.
   */
  protected synchronized void processException(DistributionMessage msg, DSFIDNotFoundException ex) {
    final short versionOrdinal = ex.getProductVersionOrdinal();
    String versionStr = null;
    try {
      Version version = Version.fromOrdinal(versionOrdinal);
      versionStr = version.toString();
    } catch (UnsupportedSerializationVersionException e) {
    }
    if (versionStr == null) {
      versionStr = "Ordinal=" + versionOrdinal;
    }
    logger.fatal(String.format(
        "Exception received due to missing DSFID %s on remote node %s running version %s.",
        new Object[] {ex.getUnknownDSFID(), msg.getSender(), versionStr}), ex);
  }

  @Override
  public void memberJoined(DistributionManager distributionManager, InternalDistributedMember id) {
    // Nothing to do - member wasn't sent the operation, anyway.
  }

  @Override
  public void quorumLost(DistributionManager distributionManager,
      Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}

  @Override
  public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {
    if (isSevereAlertProcessingEnabled()) {
      // if we're waiting for the member that initiated suspicion, we don't
      // want to be hasty about kicking it out of the distributed system
      synchronized (this.members) {
        int cells = this.members.length;
        for (int i = 0; i < cells; i++) {
          InternalDistributedMember e = this.members[i];
          if (e != null && e.equals(whoSuspected)) {
            this.severeAlertTimerReset = true;
          }
        }
      } // synchronized
    }
  }

  @Override
  public void memberDeparted(DistributionManager distributionManager,
      final InternalDistributedMember id, final boolean crashed) {
    removeMember(id, true);
    checkIfDone();
  }

  /**
   * Wait for all expected acks to be returned or an exception to come in. This method will return
   * whether a) we have received replies from all desired members or b) the necessary conditions
   * have been met so that we don't need to wait anymore.
   *
   * @throws InternalGemFireException if ack-threshold was exceeded and system property
   *         "ack-threshold-exception" is set to true
   * @throws InterruptedException thrown if the wait is interrupted
   * @see #canStopWaiting()
   */
  public void waitForReplies() throws InterruptedException, ReplyException {
    boolean result = waitForReplies(0);
    Assert.assertTrue(result, "failed but no exception thrown");
  }


  /**
   * Registers this processor as a membership listener and returns a set of the current members.
   *
   * @return a Set of the current members
   * @since GemFire 5.7
   */
  protected List addListenerAndGetMembers() {
    return getDistributionManager().addMembershipListenerAndGetDistributionManagerIds(this);
  }

  /**
   * Unregisters this processor as a membership listener
   *
   * @since GemFire 5.7
   */
  protected void removeListener() {
    try {
      getDistributionManager().removeMembershipListener(this);
    } catch (DistributedSystemDisconnectedException e) {
      // ignore
    }
  }

  /**
   * Returns the set of members that this processor should care about.
   *
   * @return a Set of the current members
   * @since GemFire 5.7
   */
  protected List getDistributionManagerIds() {
    return getDistributionManager().getDistributionManagerIds();
  }

  protected void preWait() {
    waiting = true;
    DistributionManager mgr = getDistributionManager();
    statStart = mgr.getStats().startReplyWait();
    synchronized (this.members) {
      List activeMembers = addListenerAndGetMembers();
      processActiveMembers(activeMembers);
    }
  }

  /**
   * perform initial membership processing while under synchronization of this.members
   *
   * @param activeMembers the DM's current membership set
   */
  protected void processActiveMembers(List activeMembers) {
    for (int i = 0; i < this.members.length; i++) {
      if (this.members[i] != null) {
        if (!activeMembers.contains(this.members[i])) {
          memberDeparted(getDistributionManager(), this.members[i], false);
        }
      }
    }
  }

  private void postWait() {
    waiting = false;
    removeListener();
    final DistributionManager mgr = getDistributionManager();
    mgr.getStats().endReplyWait(this.statStart, this.initTime);
    mgr.getCancelCriterion().checkCancelInProgress(null);
  }


  /**
   * Wait a given number of milliseconds for the expected acks to be received. If <code>msecs</code>
   * milliseconds pass before all acknowlegdements are received, <code>false</code> is returned.
   *
   * @param msecs the number of milliseconds to wait for replies
   * @throws InterruptedException if interrupted while waiting on latch
   * @throws ReplyException an exception passed back in reply
   * @throws InternalGemFireException if ack-threshold was exceeded and system property
   *         "ack-threshold-exception" is set to true
   * @throws IllegalStateException if the processor is not registered to receive messages
   *
   * @return Whether or not we received all of the replies in the given amount of time.
   */
  public boolean waitForReplies(long msecs) throws InterruptedException, ReplyException {
    return waitForReplies(msecs, getLatch(), true);
  }

  public boolean waitForReplies(long msecs, StoppableCountDownLatch latch, boolean doCleanUp)
      throws InterruptedException, ReplyException {
    if (this.keeperCleanedUp) {
      throw new IllegalStateException(
          "This reply processor has already been removed from the processor keeper");
    }
    boolean result = true;
    boolean interrupted = Thread.interrupted();
    MessageDependencyMonitor.waitingForReply(this);
    try {
      // do the interrupted check inside the try so that cleanup is called
      if (interrupted)
        throw new InterruptedException();
      if (stillWaiting()) {
        preWait();
        try {
          result = basicWait(msecs, latch);
        } catch (InterruptedException e) {
          interrupted = true;
        } finally {
          if (doCleanUp) {
            postWait();
          }
        }
      }
      if (this.exception != null) {
        throw this.exception;
      }
    } finally {
      if (doCleanUp) {
        try {
          cleanup();
        } finally {
          if (interrupted)
            throw new InterruptedException();
        }
      }
      MessageDependencyMonitor.doneWaiting(this);
    }
    return result;
  }

  /**
   * basicWait occurs after preWait and before postWait. Attempts to acquire the latch are made.
   *
   * @param msecs the number of milliseconds to wait for replies
   * @return whether or not we received all of the replies in the given amount of time
   */
  private boolean basicWait(long msecs, StoppableCountDownLatch latch)
      throws InterruptedException, ReplyException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }

    if (stillWaiting()) {
      long timeout = getAckWaitThreshold() * 1000L;
      long timeSoFar = System.currentTimeMillis() - this.initTime;
      final long severeAlertTimeout = getAckSevereAlertThresholdMS();
      // only start SUSPECT processing if severe alerts are enabled
      final boolean doSuspectProcessing =
          isSevereAlertProcessingEnabled() && (severeAlertTimeout > 0);
      if (timeout <= 0) {
        timeout = Long.MAX_VALUE;
      }
      if (msecs == 0) {
        boolean timedOut = false;
        if (timeout <= timeSoFar + 1) {
          timedOut = !latch.await(10);
        }
        if (timedOut || !latch.await(timeout - timeSoFar - 1)) {
          this.dmgr.getCancelCriterion().checkCancelInProgress(null);

          timeout(doSuspectProcessing, false);

          // If ack-severe-alert-threshold has been set, we now
          // wait for that period of time and then force the non-responding
          // members from the system. Then we wait indefinitely
          if (doSuspectProcessing) {
            boolean wasNotUnlatched;
            do {
              this.severeAlertTimerReset = false; // retry if this gets set by suspect processing
                                                  // (splitbrain requirement)
              wasNotUnlatched = !latch.await(severeAlertTimeout);
            } while (wasNotUnlatched && this.severeAlertTimerReset);
            if (wasNotUnlatched) {
              this.dmgr.getCancelCriterion().checkCancelInProgress(null);
              timeout(false, true);

              long suspectProcessingErrorAlertTimeout = severeAlertTimeout * 3;
              if (!latch.await(suspectProcessingErrorAlertTimeout)) {
                long now = System.currentTimeMillis();
                long totalTimeElapsed = now - this.initTime;

                String waitingOnMembers;
                synchronized (members) {
                  waitingOnMembers = Arrays.toString(members);
                }
                logger.fatal("An additional " + suspectProcessingErrorAlertTimeout
                    + " milliseconds have elapsed while waiting for replies. Total of "
                    + totalTimeElapsed + " milliseconds elapsed (init time:" + this.initTime
                    + ", now: " + now + ") Waiting for members: " + waitingOnMembers);

                // for consistency, we must now wait indefinitely for a membership view
                // that ejects the removed members
                latch.await();
              }
            }
          } else {
            latch.await();
          }
          // Give an info message since timeout gave a warning.
          logger.info("{} wait for replies completed", shortName());
        }
      } else if (msecs > timeout) {
        if (!latch.await(timeout)) {
          timeout(doSuspectProcessing, false);
          // after timeout alert, wait remaining time
          if (!latch.await(msecs - timeout)) {
            logger.info("wait for replies timing out after {} seconds",
                Long.valueOf(msecs / 1000));
            return false;
          }
          // Give an info message since timeout gave a warning.
          logger.info("{} wait for replies completed", shortName());
        }
      } else {
        if (!latch.await(msecs)) {
          return false;
        }
      }
    }
    Assert.assertTrue(latch != this.latch || !stillWaiting(), this);
    if (stopBecauseOfExceptions()) {
      throw this.exception;
    }
    return true;
  }

  /**
   * Wait a given number of milliseconds for the expected acks to be received. If <code>msecs</code>
   * milliseconds pass before all acknowlegdements are received, <code>false</code> is returned.
   * <p>
   * Thread interruptions will be ignored while waiting. If interruption occurred while in this
   * method, the current thread's interrupt flag will be true, but InterruptedException will not be
   * thrown.
   *
   * @param p_msecs the number of milliseconds to wait for replies, zero will be interpreted as
   *        Long.MAX_VALUE
   *
   * @throws ReplyException an exception passed back in reply
   *
   * @throws InternalGemFireException if ack-threshold was exceeded and system property
   *         "ack-threshold-exception" is set to true
   * @throws IllegalStateException if the processor is not registered to receive replies
   */
  public boolean waitForRepliesUninterruptibly(long p_msecs) throws ReplyException {
    return waitForRepliesUninterruptibly(p_msecs, getLatch(), true);
  }

  public boolean waitForRepliesUninterruptibly(long p_msecs, StoppableCountDownLatch latch,
      boolean doCleanUp) throws ReplyException {
    if (this.keeperCleanedUp) {
      throw new IllegalStateException(
          "This reply processor has already been removed from the processor keeper");
    }
    long msecs = p_msecs; // don't overwrite parameter
    boolean result = true;
    MessageDependencyMonitor.waitingForReply(this);
    try {
      if (stillWaiting()) {
        preWait();
        try {
          while (true) {
            // cancellation check
            this.dmgr.getCancelCriterion().checkCancelInProgress(null);

            long startWaitTime = System.currentTimeMillis();
            boolean interrupted = Thread.interrupted();
            try {
              result = basicWait(msecs, latch);
              break;
            } catch (InterruptedException e) {
              interrupted = true; // keep looping
              this.dmgr.getCancelCriterion().checkCancelInProgress(e);
              if (msecs > 0) {
                final long interruptTime = System.currentTimeMillis();
                msecs -= interruptTime - startWaitTime;
                if (msecs <= 0) {
                  msecs = 1;
                  result = false;
                  break;
                }
              }
            } finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
          } // while
        } finally {
          if (doCleanUp) {
            postWait();
          }
        }
      } // stillWaiting
      if (this.exception != null) {
        throw this.exception;
      }
    } finally {
      if (doCleanUp) {
        cleanup();
      }
      MessageDependencyMonitor.doneWaiting(this);
    }
    return result;
  }

  /**
   * Used to cleanup resources allocated by the processor after we are done using it.
   *
   * @since GemFire 5.1
   */
  public void cleanup() {
    if (!this.keeperCleanedUp) {
      this.keeperCleanedUp = true;
      keeper.remove(getProcessorId());
    }
  }

  /**
   * Wait for the expected acks to be received.
   * <p>
   * Thread interruptions will be ignored while waiting. If interruption occurred while in this
   * method, the current thread's interrupt flag will be true, but InterruptedException will not be
   * thrown.
   *
   * @throws ReplyException an exception passed back in reply
   *
   * @throws InternalGemFireException if ack-threshold was exceeded and system property
   *         "ack-threshold-exception" is set to true
   */
  public void waitForRepliesUninterruptibly() throws ReplyException {
    waitForRepliesUninterruptibly(0);
  }

  /**
   * Returns the id of this reply processor. This id is often sent in messages, so that reply
   * messages know which processor to notify.
   *
   * @see #getProcessor
   */
  public int getProcessorId() {
    return processorId;
  }

  /**
   * Returns whether or not this reply processor can stop waiting for replies. This method can be
   * overridden to allow the waiter to be notified before all responses have been received. This is
   * useful when we are waiting for a value from any member, or if we can stop waiting if a remote
   * exception occurred. By default, this method returns <code>false</code>.
   */
  protected boolean canStopWaiting() {
    return false;
  }

  /**
   * We're still waiting if there is any member still left in the set and an exception hasn't been
   * returned from anyone yet.
   *
   * @return true if we are still waiting for a response
   */
  protected boolean stillWaiting() {
    if (shutdown) {
      // Create the exception here, so that the call stack reflects the
      // failed computation. If you set the exception in onShutdown,
      // the resulting stack is not of interest.
      ReplyException re = new ReplyException(new DistributedSystemDisconnectedException(
          "aborted due to shutdown"));
      this.exception = re;
      return false;
    }

    // Optional override by subclass
    if (canStopWaiting()) {
      return false;
    }

    // If an error has occurred, we're done.
    if (stopBecauseOfExceptions()) {
      return false;
    }

    // All else is good, keep waiting if we have members to wait on.
    return numMembers() > 0;
  }

  /**
   * Control of reply processor waiting behavior in the face of exceptions.
   *
   * @since GemFire 5.7
   * @return true to stop waiting when exceptions are present
   */
  protected boolean stopBecauseOfExceptions() {
    return exception != null;
  }

  /**
   * If this processor is not waiting for any more replies, then the waiting thread will be
   * notified.
   */
  protected void checkIfDone() {
    boolean finished = !stillWaiting();
    if (finished) {
      finished();
    }
  }

  /** do processing required when finished */
  protected void finished() {
    boolean isDone = false;
    synchronized (this) {
      if (!this.done) { // make sure only called once
        this.done = true;
        isDone = true;
        // getSync().release(); // notifies threads in waitForReplies
        getLatch().countDown();
      }
    } // synchronized

    // ensure that postFinish is invoked only once
    if (isDone) {
      postFinish();
    }
  }

  /**
   * Override to execute custom code after {@link #finished}. This will be invoked only once for
   * each ReplyProcessor21.
   */
  protected void postFinish() {}

  protected String shortName() {
    String base = this.getClass().getName();
    int dot = base.lastIndexOf('.');
    if (dot == -1) {
      return base;
    }
    return base.substring(dot + 1);
  }

  @Override
  public String toString() {
    return "<" + shortName() + " " + this.getProcessorId() + " waiting for " + numMembers()
        + " replies" + (exception == null ? "" : (" exception: " + exception)) + " from "
        + membersToString() + ">";
  }

  /**
   *
   * @param m the member to be removed
   * @param departed true if it is removed due to membership
   * @return true if it was in our list of members
   */
  protected boolean removeMember(InternalDistributedMember m, boolean departed) {
    boolean removed = false;
    synchronized (this.members) {
      int cells = this.members.length;
      for (int i = 0; i < cells; i++) {
        InternalDistributedMember e = this.members[i];
        if (e != null && e.equals(m)) {
          this.members[i] = null;
          // we may be expecting more than one response from a member. so,
          // unless the member left, we only scrub the first occurrence of
          // the member id from the responder list
          if (!departed) {
            return true;
          }
          removed = true;
        }
      }
    } // synchronized
    return removed;
  }

  protected int numMembers() {
    int sz = 0;
    synchronized (this.members) {
      int cells = this.members.length;
      for (int i = 0; i < cells; i++) {
        if (this.members[i] != null) {
          sz++;
        }
      }
    } // synchronized
    return sz;
  }

  protected boolean waitingOnMember(InternalDistributedMember id) {
    synchronized (this.members) {
      int cells = this.members.length;
      for (int i = 0; i < cells; i++) {
        if (id.equals(this.members[i])) {
          return true;
        }
      }
      return false;
    } // synchronized
  }


  /**
   * Return the time in sec to wait before sending an alert while waiting for ack replies. Note that
   * the ack wait threshold may change at runtime, so we have to consult the system every time.
   */
  protected int getAckWaitThreshold() {
    return this.system.getConfig().getAckWaitThreshold();
  }

  /**
   * Return the time in sec to wait before removing unresponsive members from the distributed
   * system. This period starts after the ack-wait-threshold period has elapsed
   */
  protected int getSevereAlertThreshold() {
    return this.system.getConfig().getAckSevereAlertThreshold();
  }

  protected boolean processTimeout() {
    return true;
  }

  /**
   * process a wait-timeout. Usually suspectThem would be used in the first timeout, followed by a
   * subsequent use of disconnectThem
   *
   * @param suspectThem whether to ask the membership manager to suspect the unresponsive members
   * @param severeAlert whether to ask the membership manager to disconnect the unresponseive
   *        members
   */
  private void timeout(boolean suspectThem, boolean severeAlert) {

    if (!this.processTimeout())
      return;

    List activeMembers = getDistributionManagerIds();

    // an alert that will show up in the console
    long timeout = getAckWaitThreshold();
    final Object[] msgArgs =
        new Object[] {Long.valueOf(timeout + (severeAlert ? getSevereAlertThreshold() : 0)), this,
            getDistributionManager().getId(), activeMembers};
    final String msg =
        "%s seconds have elapsed while waiting for replies: %s on %s whose current membership list is: [%s]";
    if (severeAlert) {
      logger.fatal(String.format(msg, msgArgs));
    } else {
      logger.warn(String.format(msg, msgArgs));
    }
    msgArgs[3] = "(omitted)";
    Breadcrumbs.setProblem(msg, msgArgs);

    // Increment the stat
    getDistributionManager().getStats().incReplyTimeouts();

    final Set<InternalDistributedMember> suspectMembers;
    if (suspectThem || severeAlert) {
      suspectMembers = new HashSet();
    } else {
      suspectMembers = null;
    }

    synchronized (this.members) {
      for (int i = 0; i < this.members.length; i++) {
        if (this.members[i] != null) {
          if (!activeMembers.contains(this.members[i])) {
            logger.warn(
                "View no longer has {} as an active member, so we will no longer wait for it.",
                this.members[i]);
            memberDeparted(getDistributionManager(), this.members[i], false);
          } else {
            if (suspectMembers != null) {
              suspectMembers.add(this.members[i]);
            }
          }
        }
      }
    }

    if (THROW_EXCEPTION_ON_TIMEOUT) {
      // init the cause to be a TimeoutException so catchers can determine cause
      TimeoutException cause =
          new TimeoutException("Timed out waiting for ACKS.");
      throw new InternalGemFireException(
          String.format(
              "%s seconds have elapsed while waiting for replies: %s on %s whose current membership list is: [%s]",
              msgArgs),
          cause);
    } else if (suspectThem) {
      if (suspectMembers != null && suspectMembers.size() > 0) {
        getDistributionManager().getMembershipManager().suspectMembers(
            (Set<DistributedMember>) (Set<?>) suspectMembers,
            "Failed to respond within ack-wait-threshold");
      }
    }
  }


  protected String membersToString() {
    StringBuffer sb = new StringBuffer("[");
    boolean first = true;
    synchronized (this.members) {
      for (int i = 0; i < this.members.length; i++) {
        InternalDistributedMember member = this.members[i];
        if (member != null) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          sb.append(member);
        }
      }
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * You must be synchronize on the result of this function in order to examine its contents.
   *
   * @return the members that have not replied
   */
  protected InternalDistributedMember[] getMembers() {
    return this.members;
  }

  private StoppableCountDownLatch getLatch() {
    return this.latch;
  }

  /**
   * Enables severe alert processing in this reply processor, if it has also been enabled in the
   * distribution config. Severe alerts are issued if acks are not received within
   * ack-wait-threshold plus ack-severe-alert-threshold seconds.
   */
  public void enableSevereAlertProcessing() {
    this.severeAlertEnabled = true;
  }

  /**
   * Set a shorter ack-severe-alert-threshold than normal
   *
   * @param flag whether to shorten the time or not
   */
  public static void setShortSevereAlertProcessing(boolean flag) {
    severeAlertShorten.set(flag);
  }

  public static boolean getShortSevereAlertProcessing() {
    return severeAlertShorten.get();
  }

  /**
   * Force reply-waits in the current thread to perform severe-alert processing
   */
  public static void forceSevereAlertProcessing() {
    forceSevereAlertProcessing.set(Boolean.TRUE);
  }

  /**
   * Reset the forcing of severe-alert processing for the current thread
   */
  public static void unforceSevereAlertProcessing() {
    forceSevereAlertProcessing.set(Boolean.FALSE);
  }

  /**
   * Returns true if forceSevereAlertProcessing has been used to force the next reply-wait in the
   * current thread to perform severe-alert processing.
   */
  public static boolean isSevereAlertProcessingForced() {
    return forceSevereAlertProcessing.get();
  }


  /**
   * Get the ack-severe-alert-threshold, in milliseconds, with shortening applied
   */
  public long getAckSevereAlertThresholdMS() {
    long disconnectTimeout = getSevereAlertThreshold() * 1000L;
    if (disconnectTimeout > 0 && severeAlertShorten.get()) {
      disconnectTimeout = (long) (disconnectTimeout * PR_SEVERE_ALERT_RATIO);
    }
    return disconnectTimeout;
  }

  public boolean isSevereAlertProcessingEnabled() {
    return this.severeAlertEnabled || isSevereAlertProcessingForced();
  }


  private static final ThreadLocal<Integer> messageId = new ThreadLocal<>();

  private static final Integer VOID_RPID = 0;

  /**
   * Used by messages to store the id for the current message into a thread local. This allows the
   * comms layer to still send replies even when it can't deserialize a message.
   */
  public static void setMessageRPId(int id) {
    messageId.set(id);
  }

  public static void initMessageRPId() {
    messageId.set(VOID_RPID);
  }

  public static void clearMessageRPId() {
    messageId.set(VOID_RPID);
    // messageId.remove(); change to use remove when we drop 1.4 support
  }

  /**
   * Returns the reply processor id for the message currently being read. Returns 0 if no id exists.
   */
  public static int getMessageRPId() {
    int result = 0;
    Object v = messageId.get();
    if (v != null) {
      result = (Integer) v;
    }
    return result;
  }

  /**
   * To fix the hang of 42951 make sure that everything waiting on this processor are told to quit
   * waiting
   * and tell them why.
   *
   * @param ex the reason the reply processor is being canceled
   */
  public void cancel(InternalDistributedMember sender, RuntimeException ex) {
    processException(new ReplyException("Unexpected exception while processing reply message", ex));
    removeMember(sender, false);
    checkIfDone();
  }
}
