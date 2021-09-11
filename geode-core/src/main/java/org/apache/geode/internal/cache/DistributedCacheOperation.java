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
package org.apache.geode.internal.cache;

import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.ANY_INIT;
import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.BEFORE_INITIAL_IMAGE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.InvalidVersionException;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.ServerCQ;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.SerialDistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.CacheOperationMessageMarker;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.CopyOnWriteHashSet;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;
import org.apache.geode.internal.cache.EntryEventImpl.OldValueImporter;
import org.apache.geode.internal.cache.FilterRoutingInfo.FilterInfo;
import org.apache.geode.internal.cache.LocalRegion.InitializationLevel;
import org.apache.geode.internal.cache.UpdateOperation.UpdateMessage;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.cache.partitioned.PartitionMessage;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tx.RemoteOperationMessage;
import org.apache.geode.internal.cache.versions.DiskVersionTag;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.Releasable;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.sequencelog.EntryLogger;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.util.DelayedAction;
import org.apache.geode.logging.internal.log4j.api.LogService;

public abstract class DistributedCacheOperation {

  private static final Logger logger = LogService.getLogger();

  @MutableForTesting
  public static double LOSS_SIMULATION_RATIO = 0; // test hook

  @MutableForTesting
  public static Random LOSS_SIMULATION_GENERATOR;

  @MutableForTesting
  public static long SLOW_DISTRIBUTION_MS = 0; // test hook

  // constants used in subclasses and distribution messages
  // should use enum in source level 1.5+

  /**
   * Deserialization policy: do not deserialize (for byte array, null or cases where the value
   * should stay serialized)
   *
   * @since GemFire 5.7
   */
  public static final byte DESERIALIZATION_POLICY_NONE = (byte) 0;

  /**
   * Deserialization policy: deserialize lazily (for all other objects)
   *
   * @since GemFire 5.7
   */
  public static final byte DESERIALIZATION_POLICY_LAZY = (byte) 2;

  /**
   * @param deserializationPolicy must be one of the following: DESERIALIZATION_POLICY_NONE,
   *        DESERIALIZATION_POLICY_LAZY.
   */
  public static void writeValue(final byte deserializationPolicy, final Object vObj,
      final byte[] vBytes, final DataOutput out) throws IOException {
    if (vObj != null) {
      if (deserializationPolicy == DESERIALIZATION_POLICY_NONE) {
        // We only have NONE with a vObj when vObj is off-heap and not serialized.
        StoredObject so = (StoredObject) vObj;
        assert !so.isSerialized();
        so.sendAsByteArray(out);
      } else { // LAZY
        DataSerializer.writeObjectAsByteArray(vObj, out);
      }
    } else {
      DataSerializer.writeByteArray(vBytes, out);
    }
  }

  /**
   * Given a VALUE_IS_* constant convert and return the corresponding DESERIALIZATION_POLICY_*.
   */
  public static byte valueIsToDeserializationPolicy(boolean oldValueIsSerialized) {
    if (!oldValueIsSerialized) {
      return DESERIALIZATION_POLICY_NONE;
    }
    return DESERIALIZATION_POLICY_LAZY;
  }


  public static final byte DESERIALIZATION_POLICY_NUMBITS =
      DistributionMessage.getNumBits(DESERIALIZATION_POLICY_LAZY);

  public static final short DESERIALIZATION_POLICY_END =
      (short) (1 << DESERIALIZATION_POLICY_NUMBITS);

  public static final short DESERIALIZATION_POLICY_MASK = (short) (DESERIALIZATION_POLICY_END - 1);

  @MutableForTesting
  public static boolean testSendingOldValues;

  protected InternalCacheEvent event;

  protected CacheOperationReplyProcessor processor = null;

  protected Set<InternalDistributedMember> departedMembers;

  protected Set<InternalDistributedMember> originalRecipients;

  @MutableForTesting
  static Runnable internalBeforePutOutgoing;

  public static String deserializationPolicyToString(byte policy) {
    switch (policy) {
      case DESERIALIZATION_POLICY_NONE:
        return "NONE";
      case DESERIALIZATION_POLICY_LAZY:
        return "LAZY";
      default:
        throw new AssertionError("unknown deserialization policy");
    }
  }

  /** Creates a new instance of DistributedCacheOperation */
  public DistributedCacheOperation(CacheEvent<?, ?> event) {
    this.event = (InternalCacheEvent) event;
  }

  /**
   * Return true if this operation needs to check for reliable delivery. Return false if not.
   * Currently the only case it doesn't need to be is a DestroyRegionOperation doing a "local"
   * destroy.
   *
   * @since GemFire 5.0
   */
  boolean isOperationReliable() {
    Operation op = event.getOperation();
    if (!op.isRegionDestroy()) {
      return true;
    }
    // must be a region destroy that is "local" which means
    // Region.localDestroyRegion or Region.close or Cache.clsoe
    // none of these should do reliability checks
    return op.isDistributed();
  }

  public boolean supportsDirectAck() {
    // force use of shared connection if we're already in a secondary
    // thread-owned reader thread. See Connection#processNIOBuffer
    return true;
  }

  /**
   * returns true if multicast can be used for this operation. The default is true.
   */
  public boolean supportsMulticast() {
    return true;
  }

  /** returns true if the receiver can distribute during cache closure */
  public boolean canBeSentDuringShutdown() {
    return getRegion().isUsedForPartitionedRegionAdmin();
  }

  /**
   * returns true if adjunct messaging (piggybacking) is allowed for this operation. Region-oriented
   * operations typically do not allow adjunct messaging, while Entry-oriented operations do. The
   * default implementation returns true.
   */
  protected boolean supportsAdjunctMessaging() {
    return true;
  }

  /**
   * returns true if this operation supports propagation of delta values instead of full changes
   */
  protected boolean supportsDeltaPropagation() {
    return false;
  }

  /** does this operation change region content? */
  public boolean containsRegionContentChange() {
    return true;
  }

  @MutableForTesting
  public static volatile DelayedAction test_InvalidVersionAction;

  /**
   * region's distribution advisor marked that a distribution is about to start, then distribute. It
   * returns a token, which is view version. Return -1 means the method did not succeed. This method
   * must be invoked before toDistribute(). This method should pair with endOperation() in
   * try/finally block.
   */
  public long startOperation() {
    DistributedRegion region = getRegion();
    long viewVersion = -1;
    try {
      if (containsRegionContentChange()) {
        viewVersion = region.getDistributionAdvisor().startOperation();
      }
      if (logger.isTraceEnabled(LogMarker.STATE_FLUSH_OP_VERBOSE)) {
        logger.trace(LogMarker.STATE_FLUSH_OP_VERBOSE, "dispatching operation in view version {}",
            viewVersion);
      }
      try {
        _distribute();
      } catch (InvalidVersionException e) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE,
              "PutAll failed since versions were missing; retrying", e);
        }

        if (test_InvalidVersionAction != null) {
          test_InvalidVersionAction.run();
        }
        _distribute();
      }
    } catch (RuntimeException | Error e) {
      endOperation(viewVersion);
      throw e;
    }
    return viewVersion;
  }

  /**
   * region's distribution advisor marked that a distribution is ended. This method should pair with
   * startOperation in try/finally block.
   */
  public void endOperation(long viewVersion) {
    DistributedRegion region = getRegion();
    if (viewVersion != -1) {
      region.getDistributionAdvisor().endOperation(viewVersion);
      if (logger.isTraceEnabled()) {
        logger.trace(LogMarker.STATE_FLUSH_OP_VERBOSE,
            "done dispatching operation in view version {}", viewVersion);
      }
    }
  }

  /**
   * Distribute a cache operation to other members of the distributed system. This method determines
   * who the recipients are and handles careful delivery of the operation to those members.
   */
  public void distribute() {
    long token = -1;
    try {
      token = startOperation();
    } finally {
      endOperation(token);
    }
  }

  /**
   * About to distribute a cache operation to other members of the distributed system. This method
   * determines who the recipients are and handles careful delivery of the operation to those
   * members. This method should wrapped by startOperation() and endOperation() in try/finally
   * block.
   */
  protected void _distribute() {
    DistributedRegion region = getRegion();
    DistributionManager mgr = region.getDistributionManager();
    boolean reliableOp = isOperationReliable() && region.requiresReliabilityCheck();

    if (SLOW_DISTRIBUTION_MS > 0) { // test hook
      try {
        Thread.sleep(SLOW_DISTRIBUTION_MS);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
      SLOW_DISTRIBUTION_MS = 0;
    }

    boolean isPutAll = (this instanceof DistributedPutAllOperation);
    boolean isRemoveAll = (this instanceof DistributedRemoveAllOperation);

    try {
      // Recipients with CacheOp
      Set<InternalDistributedMember> recipients = new HashSet<>(getRecipients());
      Map<InternalDistributedMember, PersistentMemberID> persistentIds = null;
      if (region.getDataPolicy().withPersistence()) {
        persistentIds = region.getDistributionAdvisor().adviseInitializedPersistentMembers();
      }

      // some members requiring old value are also in the cache op recipients set
      Set<InternalDistributedMember> needsOldValueInCacheOp = Collections.emptySet();

      // set client routing information into the event
      boolean routingComputed = false;
      FilterRoutingInfo filterRouting = null;
      // recipients that will get a cacheop msg and also a PR message
      Set<InternalDistributedMember> twoMessages = Collections.emptySet();
      if (region.isUsedForPartitionedRegionBucket()) {
        twoMessages = ((Bucket) region).getBucketAdvisor().adviseRequiresTwoMessages();
        routingComputed = true;
        filterRouting = getRecipientFilterRouting(recipients);
        if (filterRouting != null) {
          if (logger.isDebugEnabled()) {
            logger.debug("Computed this filter routing: {}", filterRouting);
          }
        }
      }

      // some members need PR notification of the change for client/wan
      // notification
      Set<InternalDistributedMember> adjunctRecipients = Collections.emptySet();

      // Partitioned region listener notification messages piggyback on this
      // operation's replyprocessor and need to be sent at the same time as
      // the operation's message
      if (supportsAdjunctMessaging() && region.isUsedForPartitionedRegionBucket()) {
        BucketRegion br = (BucketRegion) region;
        adjunctRecipients = getAdjunctReceivers(br, recipients, twoMessages, filterRouting);
      }

      EntryEventImpl entryEvent = event.getOperation().isEntry() ? getEvent() : null;

      if (entryEvent != null && entryEvent.hasOldValue()) {
        if (testSendingOldValues) {
          needsOldValueInCacheOp = new HashSet<>(recipients);
        } else {
          needsOldValueInCacheOp =
              region.getCacheDistributionAdvisor().adviseRequiresOldValueInCacheOp();
        }
        recipients.removeAll(needsOldValueInCacheOp);
      }

      Set<InternalDistributedMember> cachelessNodes = Collections.emptySet();
      Set<InternalDistributedMember> adviseCacheServers;
      Set<InternalDistributedMember> cachelessNodesWithNoCacheServer = Collections.emptySet();
      if (region.getDistributionConfig().getDeltaPropagation() && supportsDeltaPropagation()) {
        cachelessNodes = region.getCacheDistributionAdvisor().adviseEmptys();
        if (!cachelessNodes.isEmpty()) {
          List<InternalDistributedMember> list = new ArrayList<>(cachelessNodes);
          for (InternalDistributedMember member : cachelessNodes) {
            if (!recipients.contains(member) || adjunctRecipients.contains(member)) {
              // Don't include those originally excluded.
              list.remove(member);
            }
          }
          cachelessNodes.clear();
          recipients.removeAll(list);
          cachelessNodes.addAll(list);
        }
        if (!cachelessNodes.isEmpty()) {
          cachelessNodesWithNoCacheServer = new HashSet<>(cachelessNodes);
          adviseCacheServers = region.getCacheDistributionAdvisor().adviseCacheServers();
          cachelessNodesWithNoCacheServer.removeAll(adviseCacheServers);
        }
      }

      if (recipients.isEmpty() && adjunctRecipients.isEmpty() && needsOldValueInCacheOp.isEmpty()
          && cachelessNodes.isEmpty()) {
        if (region.isInternalRegion()) {
          if (logger.isDebugEnabled() || logger.isTraceEnabled()) {
            if (mgr.getNormalDistributionManagerIds().size() > 1) {
              // suppress this msg if we are the only member.
              if (logger.isTraceEnabled()) {
                logger.trace("<No Recipients> {}", this);
              }
            } else {
              // suppress this msg if we are the only member.
              if (logger.isDebugEnabled()) {
                logger.debug("<No Recipients> {}", this);
              }
            }
          }
        }
        if (reliableOp && !region.isNoDistributionOk()) {
          region.handleReliableDistribution(Collections.emptySet());
        }

        // compute local client routing before waiting for an ack only for a bucket
        if (region.isUsedForPartitionedRegionBucket()) {
          FilterInfo filterInfo = getLocalFilterRouting(filterRouting);
          event.setLocalFilterInfo(filterInfo);
        }

      } else {
        boolean directAck = false;
        boolean useMulticast = region.getMulticastEnabled()
            && region.getSystem().getConfig().getMcastPort() != 0 && supportsMulticast();
        boolean shouldAck = shouldAck();

        if (shouldAck) {
          if (supportsDirectAck() && adjunctRecipients.isEmpty()) {
            if (region.getSystem().threadOwnsResources()) {
              directAck = true;
            }
          }
        }
        // don't send to the sender of a remote-operation-message. Those messages send
        // their own response. fixes bug #45973
        if (entryEvent != null) {
          RemoteOperationMessage rmsg = entryEvent.getRemoteOperationMessage();
          if (rmsg != null) {
            recipients.remove(rmsg.getSender());
            useMulticast = false; // bug #45106: can't mcast or the sender of the one-hop op will
                                  // get it
          }
        }

        if (logger.isDebugEnabled()) {
          logger.debug("recipients for {}: {} with adjunct messages to: {}", this, recipients,
              adjunctRecipients);
        }

        if (shouldAck) {
          // adjunct messages are sent using the same reply processor, so
          // add them to the processor's membership set
          final Collection<InternalDistributedMember> waitForMembers;
          if (recipients.size() > 0 && adjunctRecipients.size() == 0 && cachelessNodes.isEmpty()) {
            // the common case
            waitForMembers = recipients;
          } else if (!cachelessNodes.isEmpty()) {
            waitForMembers = new HashSet<>(recipients);
            waitForMembers.addAll(cachelessNodes);
          } else {
            // note that we use a List instead of a Set for the responders
            // collection because partitioned regions sometimes send both a regular cache
            // operation and a partitioned-region notification message to the same recipient
            waitForMembers = new ArrayList<>(recipients);
            waitForMembers.addAll(adjunctRecipients);
            waitForMembers.addAll(needsOldValueInCacheOp);
            waitForMembers.addAll(cachelessNodes);
          }
          if (DistributedCacheOperation.LOSS_SIMULATION_RATIO != 0.0) {
            if (LOSS_SIMULATION_GENERATOR == null) {
              LOSS_SIMULATION_GENERATOR = new Random(hashCode());
            }
            if ((LOSS_SIMULATION_GENERATOR.nextInt(100) * 1.0 / 100.0) < LOSS_SIMULATION_RATIO) {
              if (logger.isDebugEnabled()) {
                logger.debug("loss simulation is inhibiting message transmission to {}",
                    recipients);
              }
              waitForMembers.removeAll(recipients);
              recipients = Collections.emptySet();
            }
          }
          if (reliableOp) {
            departedMembers = new HashSet<>();
            processor = new ReliableCacheReplyProcessor(region.getSystem(), waitForMembers,
                departedMembers);
          } else {
            processor = new CacheOperationReplyProcessor(region.getSystem(), waitForMembers);
          }
        }

        CacheOperationMessage msg = createMessage();
        initMessage(msg, processor);

        if (DistributedCacheOperation.internalBeforePutOutgoing != null) {
          DistributedCacheOperation.internalBeforePutOutgoing.run();
        }

        if (processor != null && msg.isSevereAlertCompatible()) {
          processor.enableSevereAlertProcessing();
          // if this message is distributing for a partitioned region message,
          // we can't wait as long as the full ack-severe-alert-threshold or
          // the sender might kick us out of the system before we can get an ack
          // back
          DistributedRegion r = getRegion();
          if (r.isUsedForPartitionedRegionBucket() && event.getOperation().isEntry()) {
            PartitionMessage pm = ((EntryEventImpl) event).getPartitionMessage();
            if (pm != null && pm.getSender() != null
                && !pm.getSender().equals(r.getDistributionManager().getDistributionManagerId())) {
              // PR message sent by another member
              ReplyProcessor21.setShortSevereAlertProcessing(true);
            }
          }
        }

        msg.setMulticast(useMulticast);
        msg.directAck = directAck;
        if (region.isUsedForPartitionedRegionBucket()) {
          if (!isPutAll && !isRemoveAll && filterRouting != null
              && filterRouting.hasMemberWithFilterInfo()) {
            if (logger.isDebugEnabled()) {
              logger.debug("Setting filter information for message to {}", filterRouting);
            }
            msg.filterRouting = filterRouting;
          }
        } else if (!routingComputed) {
          msg.needsRouting = true;
        }

        initProcessor(processor, msg);

        if (region.cache.isClosed() && !canBeSentDuringShutdown()) {
          throw region.cache.getCacheClosedException(
              "The cache has been closed",
              null);
        }

        msg.setRecipients(recipients);
        Set<InternalDistributedMember> failures = mgr.putOutgoing(msg);

        // distribute to members needing the old value now
        if (needsOldValueInCacheOp.size() > 0) {
          msg.appendOldValueToMessage((EntryEventImpl) event);
          msg.resetRecipients();
          msg.setRecipients(needsOldValueInCacheOp);
          Set<InternalDistributedMember> newFailures = mgr.putOutgoing(msg);
          if (newFailures != null) {
            if (logger.isDebugEnabled()) {
              logger.debug("Failed sending ({}) to {}", msg, newFailures);
            }
            if (failures != null && failures.size() > 0) {
              failures.addAll(newFailures);
            } else {
              failures = newFailures;
            }
          }
        }

        if (cachelessNodes.size() > 0) {
          cachelessNodes.removeAll(cachelessNodesWithNoCacheServer);
          if (cachelessNodes.size() > 0) {
            msg.resetRecipients();
            msg.setRecipients(cachelessNodes);
            msg.setSendDelta(false);
            Set<InternalDistributedMember> newFailures = mgr.putOutgoing(msg);
            if (newFailures != null) {
              if (failures != null && failures.size() > 0) {
                failures.addAll(newFailures);
              } else {
                failures = newFailures;
              }
            }
          }

          if (!cachelessNodesWithNoCacheServer.isEmpty()) {
            msg.resetRecipients();
            msg.setRecipients(cachelessNodesWithNoCacheServer);
            msg.setSendDelta(false);
            ((UpdateMessage) msg).setSendDeltaWithFullValue(false);
            Set<InternalDistributedMember> newFailures = mgr.putOutgoing(msg);
            if (newFailures != null) {
              if (failures != null && failures.size() > 0) {
                failures.addAll(newFailures);
              } else {
                failures = newFailures;
              }
            }
            // Add it back for size calculation ahead
            cachelessNodes.addAll(cachelessNodesWithNoCacheServer);
          }
        }

        if (failures != null && !failures.isEmpty() && logger.isDebugEnabled()) {
          logger.debug("Failed sending ({}) to {} while processing event:{}", msg, failures, event);
        }

        // send partitioned region listener notification messages now
        if (!adjunctRecipients.isEmpty()) {
          if (cachelessNodes.size() > 0) {
            // add non-delta recipients back into the set for adjunct
            // calculations
            if (recipients.isEmpty()) {
              recipients = cachelessNodes;
            } else {
              recipients.addAll(cachelessNodes);
            }
          }

          final Set<InternalDistributedMember> adjunctRecipientsWithNoCacheServer =
              new HashSet<>(adjunctRecipients);
          adviseCacheServers = ((Bucket) region).getPartitionedRegion()
              .getCacheDistributionAdvisor().adviseCacheServers();
          adjunctRecipientsWithNoCacheServer.removeAll(adviseCacheServers);

          if (isPutAll) {
            ((BucketRegion) region).performPutAllAdjunctMessaging((DistributedPutAllOperation) this,
                recipients, adjunctRecipients, filterRouting, processor);
          } else if (isRemoveAll) {
            ((BucketRegion) region).performRemoveAllAdjunctMessaging(
                (DistributedRemoveAllOperation) this, recipients, adjunctRecipients, filterRouting,
                processor);
          } else {
            boolean calculateDelta =
                adjunctRecipientsWithNoCacheServer.size() < adjunctRecipients.size();
            adjunctRecipients.removeAll(adjunctRecipientsWithNoCacheServer);
            if (!adjunctRecipients.isEmpty()) {
              ((BucketRegion) region).performAdjunctMessaging(getEvent(), recipients,
                  adjunctRecipients, filterRouting, processor, calculateDelta, true);
            }
            if (!adjunctRecipientsWithNoCacheServer.isEmpty()) {
              ((BucketRegion) region).performAdjunctMessaging(getEvent(), recipients,
                  adjunctRecipientsWithNoCacheServer, filterRouting, processor, calculateDelta,
                  false);
            }
          }
        }

        // compute local client routing before waiting for an ack only for a bucket
        if (region.isUsedForPartitionedRegionBucket()) {
          FilterInfo filterInfo = getLocalFilterRouting(filterRouting);
          event.setLocalFilterInfo(filterInfo);
        }

        waitForAckIfNeeded(persistentIds);

        if (/* msg != null && */reliableOp) {
          Set<InternalDistributedMember> successfulRecips = new HashSet<>(recipients);
          successfulRecips.addAll(cachelessNodes);
          successfulRecips.addAll(needsOldValueInCacheOp);
          if (failures != null && !failures.isEmpty()) {
            successfulRecips.removeAll(failures);
          }
          if (departedMembers != null) {
            successfulRecips.removeAll(departedMembers);
          }
          region.handleReliableDistribution(successfulRecips);
        }
      }

      if (region.isUsedForPartitionedRegionBucket() && filterRouting != null) {
        removeDestroyTokensFromCqResultKeys(filterRouting);
      }

    } catch (CancelException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("distribution of message aborted by shutdown: {}", this);
      }
      throw e;
    } catch (RuntimeException e) {
      logger.info(String.format("Exception occurred while processing %s", this),
          e);
      throw e;
    } finally {
      ReplyProcessor21.setShortSevereAlertProcessing(false);
    }
  }

  /**
   * Cleanup destroyed events in CQ result cache for remote CQs. While maintaining the CQ results
   * key caching. the destroy event keys are marked as destroyed instead of removing them, this is
   * to take care, arrival of duplicate events. The key marked as destroyed are removed after the
   * event is placed in clients HAQueue or distributed to the peers.
   *
   * This is similar to CacheClientNotifier.removeDestroyTokensFromCqResultKeys() where the
   * destroyed events for local CQs are handled.
   */
  private void removeDestroyTokensFromCqResultKeys(FilterRoutingInfo filterRouting) {
    for (InternalDistributedMember m : filterRouting.getMembers()) {
      FilterInfo filterInfo = filterRouting.getFilterInfo(m);
      if (filterInfo == null || filterInfo.getCQs() == null) {
        continue;
      }

      CacheProfile cf = (CacheProfile) ((Bucket) getRegion()).getPartitionedRegion()
          .getCacheDistributionAdvisor().getProfile(m);

      if (cf == null || cf.filterProfile == null || cf.filterProfile.isLocalProfile()
          || cf.filterProfile.getCqMap().isEmpty()) {
        continue;
      }

      for (Object value : cf.filterProfile.getCqMap().values()) {
        ServerCQ cq = (ServerCQ) value;

        doRemoveDestroyTokensFromCqResultKeys(filterInfo, cq);
      }
    }
  }

  void doRemoveDestroyTokensFromCqResultKeys(FilterInfo filterInfo, ServerCQ cq) {
    for (Map.Entry<Long, Integer> e : filterInfo.getCQs().entrySet()) {
      Long cqID = e.getKey();
      // For the CQs satisfying the event with destroy CQEvent, remove
      // the entry form CQ cache.
      if (cq != null && cq.getFilterID() != null && cq.getFilterID().equals(cqID)
          && e.getValue() != null && e.getValue().equals(MessageType.LOCAL_DESTROY)
          && ((EntryOperation<?, ?>) event).getKey() != null) {
        cq.removeFromCqResultKeys(((EntryOperation<?, ?>) event).getKey(), true);
      }
    }
  }

  /**
   * Get the adjunct receivers for a partitioned region operation
   *
   * @param br the PR bucket
   * @param cacheOpReceivers the receivers of the CacheOperationMessage for this op
   * @param twoMessages PR members that are creating the bucket and need both cache op and adjunct
   *        messages
   * @param routing client routing information
   */
  Set<InternalDistributedMember> getAdjunctReceivers(BucketRegion br,
      Set<InternalDistributedMember> cacheOpReceivers, Set<InternalDistributedMember> twoMessages,
      FilterRoutingInfo routing) {
    return br.getAdjunctReceivers(getEvent(), cacheOpReceivers, twoMessages, routing);
  }

  /**
   * perform any operation-specific initialization on the given reply processor
   */
  protected void initProcessor(CacheOperationReplyProcessor p, CacheOperationMessage msg) {
    // nothing to do here - see UpdateMessage
  }

  protected void waitForAckIfNeeded(
      Map<InternalDistributedMember, PersistentMemberID> persistentIds) {
    if (processor == null) {
      return;
    }
    try {
      // keep waiting even if interrupted
      try {
        processor.waitForRepliesUninterruptibly();
        Set<InternalDistributedMember> closedMembers = processor.closedMembers.getSnapshot();
        handleClosedMembers(closedMembers, persistentIds);
      } catch (ReplyException e) {
        if (this instanceof DestroyRegionOperation) {
          logger.fatal("waitForAckIfNeeded: exception", e);
        }
        e.handleCause();
      }
    } finally {
      processor = null;
    }
  }

  private void handleClosedMembers(Set<InternalDistributedMember> closedMembers,
      Map<InternalDistributedMember, PersistentMemberID> persistentIds) {
    if (persistentIds == null) {
      return;
    }

    for (InternalDistributedMember member : closedMembers) {
      PersistentMemberID persistentId = persistentIds.get(member);
      if (persistentId != null) {
        // Fix for bug 42142 - In order for recovery to work,
        // we must either
        // 1) persistent the region operation successfully on the peer
        // 2) record that the peer is offline
        // or
        // 3) fail the operation

        // if we have started to shutdown, we don't want to mark the peer
        // as offline, or we will think we have newer data when in fact we don't
        getRegion().getCancelCriterion().checkCancelInProgress(null);

        // Otherwise, mark the peer as offline, because it didn't complete
        // the operation.
        getRegion().getPersistenceAdvisor().markMemberOffline(member, persistentId);
      }
    }
  }

  protected boolean shouldAck() {
    return getRegion().scope.isAck();
  }

  protected DistributedRegion getRegion() {
    return (DistributedRegion) event.getRegion();
  }

  protected EntryEventImpl getEvent() {
    return (EntryEventImpl) event;
  }

  protected Set<InternalDistributedMember> getRecipients() {
    CacheDistributionAdvisor advisor = getRegion().getCacheDistributionAdvisor();
    originalRecipients = advisor.adviseCacheOp();
    return originalRecipients;
  }

  protected FilterRoutingInfo getRecipientFilterRouting(
      Set<InternalDistributedMember> cacheOpRecipients) {
    LocalRegion region = getRegion();
    if (!region.isUsedForPartitionedRegionBucket()) {
      return null;
    }
    CacheDistributionAdvisor advisor;
    advisor = region.getPartitionedRegion().getCacheDistributionAdvisor();
    return advisor.adviseFilterRouting(event, cacheOpRecipients);
  }

  /**
   * @param frInfo the filter routing computed for distribution to peers
   * @return the filter routing computed for distribution to clients of this process
   */
  protected FilterInfo getLocalFilterRouting(FilterRoutingInfo frInfo) {
    FilterProfile fp = getRegion().getFilterProfile();
    if (fp == null) {
      return null;
    }
    FilterRoutingInfo fri = fp.getFilterRoutingInfoPart2(frInfo, event);
    if (fri == null) {
      return null;
    }
    return fri.getLocalFilterInfo();
  }

  protected abstract CacheOperationMessage createMessage();

  protected void initMessage(CacheOperationMessage msg, DirectReplyProcessor p) {
    msg.regionPath = getRegion().getFullPath();
    msg.processorId = p == null ? 0 : p.getProcessorId();
    msg.processor = p;
    if (event.getOperation().isEntry()) {
      EntryEventImpl entryEvent = getEvent();
      msg.callbackArg = entryEvent.getRawCallbackArgument();
      msg.possibleDuplicate = entryEvent.isPossibleDuplicate();

      VersionTag tag = entryEvent.getVersionTag();
      msg.setInhibitNotificationsBit(entryEvent.inhibitAllNotifications());
      if (tag != null && tag.hasValidVersion()) {
        msg.setVersionTag(tag);
      }

    } else {
      msg.callbackArg = ((RegionEventImpl) event).getRawCallbackArgument();
    }
    msg.op = event.getOperation();
    msg.owner = this;
    msg.regionAllowsConflation = getRegion().getEnableAsyncConflation();

  }

  @Override
  public String toString() {
    String cname = getClass().getName().substring(getClass().getPackage().getName().length() + 1);
    return cname + "(" + event + ")";
  }

  /**
   * Add an internal callback which is run before the CacheOperationMessage is distributed with
   * dm.putOutgoing.
   */
  public static void setBeforePutOutgoing(Runnable beforePutOutgoing) {
    internalBeforePutOutgoing = beforePutOutgoing;
  }

  public abstract static class CacheOperationMessage extends SerialDistributionMessage
      implements MessageWithReply, DirectReplyMessage, OldValueImporter,
      CacheOperationMessageMarker {

    protected static final short POSSIBLE_DUPLICATE_MASK = POS_DUP;
    protected static final short OLD_VALUE_MASK = DistributionMessage.UNRESERVED_FLAGS_START;
    protected static final short DIRECT_ACK_MASK = (OLD_VALUE_MASK << 1);
    protected static final short FILTER_INFO_MASK = (DIRECT_ACK_MASK << 1);
    protected static final short CALLBACK_ARG_MASK = (FILTER_INFO_MASK << 1);
    protected static final short DELTA_MASK = (CALLBACK_ARG_MASK << 1);
    protected static final short NEEDS_ROUTING_MASK = (DELTA_MASK << 1);
    protected static final short VERSION_TAG_MASK = (NEEDS_ROUTING_MASK << 1);
    protected static final short PERSISTENT_TAG_MASK = (VERSION_TAG_MASK << 1);
    protected static final short UNRESERVED_FLAGS_START = (PERSISTENT_TAG_MASK << 1);

    private static final int INHIBIT_NOTIFICATIONS_MASK = 0x400;

    public boolean needsRouting;

    protected String regionPath;

    protected int processorId;

    public DirectReplyProcessor processor;

    protected Object callbackArg;

    protected Operation op;

    public transient boolean directAck = false;

    protected transient DistributedCacheOperation owner;

    protected transient boolean appliedOperation = false;

    protected transient boolean closed = false;

    protected boolean hasOldValue;
    protected boolean oldValueIsSerialized;
    protected Object oldValue;

    protected boolean hasDelta = false;

    protected FilterRoutingInfo filterRouting;

    protected transient boolean sendDelta = true;

    /** concurrency versioning tag */
    protected VersionTag versionTag;

    protected transient short flags;

    protected boolean inhibitAllNotifications;

    public Operation getOperation() {
      return op;
    }

    /** sets the concurrency versioning tag for this message */
    public void setVersionTag(VersionTag tag) {
      versionTag = tag;
    }

    public VersionTag getVersionTag() {
      return versionTag;
    }

    @Override
    public boolean isSevereAlertCompatible() {
      // allow forced-disconnect processing for all cache op messages
      return true;
    }

    @Override
    public DirectReplyProcessor getDirectReplyProcessor() {
      return processor;
    }

    @Override
    public void registerProcessor() {
      if (processor != null) {
        processorId = processor.register();
      }
      directAck = false;
    }

    public void setFilterInfo(FilterRoutingInfo fInfo) {
      filterRouting = fInfo;
    }

    public void setInhibitNotificationsBit(boolean inhibit) {
      inhibitAllNotifications = inhibit;
    }

    public String getRegionPath() {
      return regionPath;
    }

    /**
     * process a reply
     *
     * @return true if the reply-processor should continue to process this response
     */
    boolean processReply(ReplyMessage reply, CacheOperationReplyProcessor processor) {
      // notification that a reply has been received. Most messages
      // don't do anything special here
      return true;
    }

    /**
     * Add the cache event's old value to this message. We must propagate the old value when the
     * receiver is doing GII and has listeners (CQs) that require the old value.
     *
     * @since GemFire 5.5
     * @param event the entry event that contains the old value
     */
    public void appendOldValueToMessage(EntryEventImpl event) {
      @Unretained
      Object val = event.getRawOldValue();
      if (val == null || val == Token.NOT_AVAILABLE || val == Token.REMOVED_PHASE1
          || val == Token.REMOVED_PHASE2 || val == Token.DESTROYED || val == Token.TOMBSTONE) {
        return;
      }
      event.exportOldValue(this);
    }

    /**
     * Insert this message's oldValue into the given event. This fixes bug 38382 by propagating old
     * values with Entry level CacheOperationMessages during initial image transfer
     *
     * @since GemFire 5.5
     */
    public void setOldValueInEvent(EntryEventImpl event) {
      CqService cqService = event.getRegion().getCache().getCqService();
      if (cqService.isRunning()/* || event.getOperation().guaranteesOldValue() */) {
        event.setOldValueForQueryProcessing();
        if (!event.hasOldValue() && hasOldValue) {
          if (oldValueIsSerialized) {
            event.setSerializedOldValue((byte[]) oldValue);
          } else {
            event.setOldValue(oldValue);
          }
        }
      }
    }

    /**
     * Sets a flag in the message indicating that this message contains delta bytes.
     *
     * @since GemFire 6.1
     */
    protected void setHasDelta(boolean flag) {
      hasDelta = flag;
    }

    protected boolean hasDelta() {
      return hasDelta;
    }

    public FilterRoutingInfo getFilterInfo() {
      return filterRouting;
    }

    /**
     * @since GemFire 4.2.3
     */
    protected transient boolean regionAllowsConflation;

    public boolean possibleDuplicate;

    @Override
    public int getProcessorId() {
      return processorId;
    }

    @Override
    public boolean containsRegionContentChange() {
      return true;
    }

    protected LocalRegion getLocalRegionForProcessing(ClusterDistributionManager dm) {
      Assert.assertTrue(regionPath != null, "regionPath was null");
      InternalCache gfc = dm.getExistingCache();
      return (LocalRegion) gfc.getRegionByPathForProcessing(regionPath);
    }

    @Override
    protected void process(final ClusterDistributionManager dm) {
      Throwable thr = null;
      boolean sendReply = true;

      if (versionTag != null) {
        versionTag.replaceNullIDs(getSender());
      }

      EntryLogger.setSource(getSender(), "p2p");
      final InitializationLevel oldLevel =
          LocalRegion.setThreadInitLevelRequirement(BEFORE_INITIAL_IMAGE);
      try {
        if (dm.getDMType() == ClusterDistributionManager.ADMIN_ONLY_DM_TYPE) {
          // this was probably a multicast message
          return;
        }

        final LocalRegion lclRgn = getLocalRegionForProcessing(dm);
        sendReply = false;
        basicProcess(dm, lclRgn);
      } catch (CancelException ignore) {
        closed = true;
        if (logger.isDebugEnabled()) {
          logger.debug("{} Cancelled: nothing to do", this);
        }
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        thr = t;
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
        if (sendReply) {
          ReplyException rex = null;
          if (thr != null) {
            rex = new ReplyException(thr);
          }
          sendReply(getSender(), processorId, rex, getReplySender(dm));
        } else if (thr != null) {
          if (logger.isDebugEnabled()) {
            logger.debug("In {}. process, got exception (NO_ACK)", getClass().getName(), thr);
          }
        }
        EntryLogger.clearSource();
      }
    }

    /** Return true if a reply should be sent */
    protected void basicProcess(ClusterDistributionManager dm, LocalRegion lclRgn) {
      Throwable thr = null;
      boolean sendReply = true;

      if (logger.isTraceEnabled()) {
        logger.trace("DistributedCacheOperation.basicProcess: {}", this);
      }
      try {
        if (lclRgn == null) {
          closed = true;
          if (logger.isDebugEnabled()) {
            logger.debug("{} region not found, nothing to do", this);
          }
          return;
        }
        // Could this cause a deadlock, because this can block a P2P reader
        // thread which might be needed to read the create region reply??
        lclRgn.waitOnInitialization();
        // In some subclasses, lclRgn may be destroyed, so be careful not to
        // allow a RegionDestroyedException to be thrown on lclRgn access
        if (lclRgn.scope.isLocal()) {
          if (logger.isDebugEnabled()) {
            logger.debug("{} local scope region, nothing to do", this);
          }
          return;
        }
        DistributedRegion rgn = (DistributedRegion) lclRgn;

        // check to see if the region is in recovery mode, in which case
        // we only operate if this is a DestroyRegion operation
        if (rgn.getImageState().getInRecovery()) {
          return;
        }

        @Released
        InternalCacheEvent event = createEvent(rgn);
        try {
          boolean isEntry = event.getOperation().isEntry();

          if (isEntry && possibleDuplicate) {
            ((EntryEventImpl) event).setPossibleDuplicate(true);
            // If the state of the initial image yet to be received is unknown,
            // we must not apply the event. It may already be reflected in the
            // initial image state and, in fact, have been modified by subsequent
            // events. This code path could be modified to pass the event to
            // listeners and bridges, but it should not apply the change to the
            // region
            if (!rgn.isEventTrackerInitialized()
                && (rgn.getDataPolicy().withReplication() || rgn.getDataPolicy().withPreloaded())) {
              if (logger.isTraceEnabled()) {
                logger.trace(LogMarker.DM_BRIDGE_SERVER_VERBOSE,
                    "Ignoring possible duplicate event");
              }
              return;
            }
          }

          sendReply = operateOnRegion(event, dm);
        } finally {
          if (event instanceof EntryEventImpl) {
            ((Releasable) event).release();
          }
        }
      } catch (RegionDestroyedException ignore) {
        closed = true;
        if (logger.isDebugEnabled()) {
          logger.debug("{} Region destroyed: nothing to do", this);
        }
      } catch (CancelException ignore) {
        closed = true;
        if (logger.isDebugEnabled()) {
          logger.debug("{} Cancelled: nothing to do", this);
        }
      } catch (DiskAccessException e) {
        closed = true;
        if (!lclRgn.isDestroyed()) {
          logger.error("Got disk access exception, expected region to be destroyed", e);
        }
      } catch (EntryNotFoundException ignore) {
        appliedOperation = true;
        if (logger.isDebugEnabled()) {
          logger.debug("{} Entry not found, nothing to do", this);
        }
      } catch (InvalidDeltaException ide) {
        ReplyException re = new ReplyException(ide);
        sendReply = false;
        sendReply(getSender(), processorId, re, getReplySender(dm));
        lclRgn.getCachePerfStats().incDeltaFullValuesRequested();
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        thr = t;
      } finally {
        checkVersionIsRecorded(versionTag, lclRgn);
        if (sendReply) {
          ReplyException rex = null;
          if (thr != null) {
            rex = new ReplyException(thr);
          }
          sendReply(getSender(), processorId, rex, getReplySender(dm));
        } else if (thr != null) {
          logger.error(String.format("Exception occurred while processing %s",
              this),
              thr);
        }
      } // finally
    }

    public void sendReply(InternalDistributedMember recipient, int pId, ReplyException rex,
        ReplySender dm) {
      // Don't respond to distributed-no-ack message.
      if (pId != 0 || (!(dm instanceof DistributionManager)) || directAck) {
        ReplyMessage.send(recipient, pId, rex, dm, !appliedOperation, closed, false,
            isInternal());
      }
    }

    /**
     * Ensure that a version tag has been recorded in the region's version vector. This makes note
     * that the event has been received and processed but probably didn't affect the cache's state
     * or it would have been recorded earlier.
     *
     * @param tag the version information
     * @param r the affected region
     */
    public void checkVersionIsRecorded(VersionTag tag, LocalRegion r) {
      if (tag != null && !tag.isRecorded()) { // oops - someone forgot to record the event
        if (r != null) {
          RegionVersionVector v = r.getVersionVector();
          if (v != null) {
            VersionSource mbr = tag.getMemberID();
            if (mbr == null) {
              mbr = getSender();
            }
            if (logger.isTraceEnabled()) {
              logger.trace(
                  "recording version tag in RVV in basicProcess since it wasn't done earlier");
            }
            v.recordVersion(mbr, tag);
          }
        }
      }
    }

    /**
     * When an event is discarded because of an attempt to overwrite a more recent change we still
     * need to deliver that event to clients. Clients can then perform their own concurrency checks
     * on the event.
     */
    protected void dispatchElidedEvent(LocalRegion rgn, EntryEventImpl ev) {
      if (logger.isDebugEnabled()) {
        logger.debug("dispatching elided event: {}", ev);
      }
      ev.isConcurrencyConflict(true);
      rgn.generateLocalFilterRouting(ev);
      rgn.notifyBridgeClients(ev);
    }

    protected abstract InternalCacheEvent createEvent(DistributedRegion rgn)
        throws EntryNotFoundException;

    protected abstract boolean operateOnRegion(CacheEvent<?, ?> event,
        ClusterDistributionManager dm)
        throws EntryNotFoundException;

    @Override
    public String toString() {
      StringBuilder buff = new StringBuilder();
      buff.append(getShortClassName());
      buff.append("(region path='"); // make sure this is the first one
      buff.append(regionPath);
      buff.append("'");
      appendFields(buff);
      buff.append(")");
      return buff.toString();
    }

    // override and extend in subclasses
    protected void appendFields(StringBuilder buff) {
      buff.append("; sender=");
      buff.append(getSender());
      buff.append("; callbackArg=");
      buff.append(callbackArg);
      buff.append("; processorId=");
      buff.append(processorId);
      buff.append("; op=");
      buff.append(op);
      buff.append("; applied=");
      buff.append(appliedOperation);
      buff.append("; directAck=");
      buff.append(directAck);
      buff.append("; posdup=");
      buff.append(possibleDuplicate);
      buff.append("; hasDelta=");
      buff.append(hasDelta);
      buff.append("; hasOldValue=");
      buff.append(hasOldValue);
      if (versionTag != null) {
        buff.append("; version=");
        buff.append(versionTag);
      }
      if (filterRouting != null) {
        buff.append(" ");
        buff.append(filterRouting);
      }
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      short bits = in.readShort();
      short extBits = in.readShort();
      flags = bits;
      setFlags(bits, in);
      regionPath = DataSerializer.readString(in);
      op = Operation.fromOrdinal(in.readByte());
      // TODO dirack There's really no reason to send this flag across the wire
      // anymore
      directAck = (bits & DIRECT_ACK_MASK) != 0;
      possibleDuplicate = (bits & POSSIBLE_DUPLICATE_MASK) != 0;
      if ((bits & CALLBACK_ARG_MASK) != 0) {
        callbackArg = DataSerializer.readObject(in);
      }
      hasDelta = (bits & DELTA_MASK) != 0;
      hasOldValue = (bits & OLD_VALUE_MASK) != 0;
      if (hasOldValue) {
        byte b = in.readByte();
        if (b == 0) {
          oldValueIsSerialized = false;
        } else if (b == 1) {
          oldValueIsSerialized = true;
        } else {
          throw new IllegalStateException("expected 0 or 1");
        }
        oldValue = DataSerializer.readByteArray(in);
      }
      boolean hasFilterInfo = (bits & FILTER_INFO_MASK) != 0;
      needsRouting = (bits & NEEDS_ROUTING_MASK) != 0;
      if (hasFilterInfo) {
        filterRouting = new FilterRoutingInfo();
        InternalDataSerializer.invokeFromData(filterRouting, in);
      }
      if ((bits & VERSION_TAG_MASK) != 0) {
        boolean persistentTag = (bits & PERSISTENT_TAG_MASK) != 0;
        versionTag = VersionTag.create(persistentTag, in);
      }
      if ((extBits & INHIBIT_NOTIFICATIONS_MASK) != 0) {
        inhibitAllNotifications = true;
      }
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      short bits = 0;
      short extendedBits = 0;
      bits = computeCompressedShort(bits);
      extendedBits = computeCompressedExtBits(extendedBits);
      out.writeShort(bits);
      out.writeShort(extendedBits);
      if (processorId > 0) {
        out.writeInt(processorId);
      }

      DataSerializer.writeString(regionPath, out);
      out.writeByte(op.ordinal);
      if (callbackArg != null) {
        DataSerializer.writeObject(callbackArg, out);
      }
      if (hasOldValue) {
        out.writeByte(oldValueIsSerialized ? 1 : 0);
        // the receiving side expects that the old value will have been serialized
        // as a byte array
        final byte policy = valueIsToDeserializationPolicy(oldValueIsSerialized);
        final Object vObj;
        final byte[] vBytes;
        if (!oldValueIsSerialized && oldValue instanceof byte[]) {
          vObj = null;
          vBytes = (byte[]) oldValue;
        } else {
          vObj = oldValue;
          vBytes = null;
        }
        writeValue(policy, vObj, vBytes, out);
      }
      if (filterRouting != null) {
        InternalDataSerializer.invokeToData(filterRouting, out);
      }
      if (versionTag != null) {
        InternalDataSerializer.invokeToData(versionTag, out);
      }
    }

    protected short computeCompressedShort(short bits) {
      if (hasOldValue) {
        bits |= OLD_VALUE_MASK;
      }
      if (directAck) {
        bits |= DIRECT_ACK_MASK;
      }
      if (possibleDuplicate) {
        bits |= POSSIBLE_DUPLICATE_MASK;
      }
      if (processorId != 0) {
        bits |= HAS_PROCESSOR_ID;
      }
      if (callbackArg != null) {
        bits |= CALLBACK_ARG_MASK;
      }
      if (hasDelta) {
        bits |= DELTA_MASK;
      }
      if (filterRouting != null) {
        bits |= FILTER_INFO_MASK;
      }
      if (needsRouting) {
        bits |= NEEDS_ROUTING_MASK;
      }
      if (versionTag != null) {
        bits |= VERSION_TAG_MASK;
      }
      if (versionTag instanceof DiskVersionTag) {
        bits |= PERSISTENT_TAG_MASK;
      }
      if (inhibitAllNotifications) {
        bits |= INHIBIT_NOTIFICATIONS_MASK;
      }
      return bits;
    }

    protected short computeCompressedExtBits(short bits) {
      if (inhibitAllNotifications) {
        bits |= INHIBIT_NOTIFICATIONS_MASK;
      }
      return bits;
    }

    protected void setFlags(short bits, DataInput in) throws IOException, ClassNotFoundException {
      if ((bits & HAS_PROCESSOR_ID) != 0) {
        processorId = in.readInt();
        ReplyProcessor21.setMessageRPId(processorId);
      }
    }

    @Override
    public boolean supportsDirectAck() {
      return directAck;
    }

    public void setSendDelta(boolean sendDelta) {
      this.sendDelta = sendDelta;
    }

    @Override
    public boolean prefersOldSerialized() {
      return true;
    }

    @Override
    public boolean isUnretainedOldReferenceOk() {
      return true;
    }

    @Override
    public boolean isCachedDeserializableValueOk() {
      return false;
    }

    @Override
    public void importOldObject(Object ov, boolean isSerialized) {
      oldValueIsSerialized = isSerialized;
      oldValue = ov;
      hasOldValue = true;
    }

    @Override
    public void importOldBytes(byte[] ov, boolean isSerialized) {
      oldValueIsSerialized = isSerialized;
      oldValue = ov;
      hasOldValue = true;
    }

    protected boolean notifiesSerialGatewaySender(ClusterDistributionManager dm) {
      final InitializationLevel oldLevel = LocalRegion.setThreadInitLevelRequirement(ANY_INIT);
      try {
        LocalRegion lr = getLocalRegionForProcessing(dm);
        if (lr == null) {
          return false;
        }
        return lr.notifiesSerialGatewaySender();
      } catch (RuntimeException ignore) {
        return false;
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }
    }
  }

  /** Custom subclass that keeps all ReplyExceptions */
  private static class ReliableCacheReplyProcessor extends CacheOperationReplyProcessor {

    private final Set<InternalDistributedMember> failedMembers;

    public ReliableCacheReplyProcessor(InternalDistributedSystem system,
        Collection<InternalDistributedMember> initMembers,
        Set<InternalDistributedMember> departedMembers) {
      super(system, initMembers);
      failedMembers = departedMembers;
    }

    @Override
    protected synchronized void processException(DistributionMessage dmsg, ReplyException ex) {
      Throwable cause = ex.getCause();
      // only interested in CacheClosedException and RegionDestroyedException
      failedMembers.add(dmsg.getSender());
      if (!(cause instanceof CancelException) && !(cause instanceof RegionDestroyedException)) {
        // allow superclass to handle all other exceptions
        super.processException(dmsg, ex);
      }
    }

    @Override
    protected void process(DistributionMessage dmsg, boolean warn) {
      if (dmsg instanceof ReplyMessage && ((ReplyMessage) dmsg).getIgnored()) {
        if (logger.isDebugEnabled()) {
          logger.debug("{} replied with ignored true", dmsg.getSender());
        }
        failedMembers.add(dmsg.getSender());
      }
      super.process(dmsg, warn);
    }
  }

  static class CacheOperationReplyProcessor extends DirectReplyProcessor {
    public CacheOperationMessage msg;

    public CopyOnWriteHashSet<InternalDistributedMember> closedMembers = new CopyOnWriteHashSet<>();

    public CacheOperationReplyProcessor(InternalDistributedSystem system,
        Collection<InternalDistributedMember> initMembers) {
      super(system, initMembers);
    }

    @Override
    protected void process(final DistributionMessage dmsg, boolean warn) {
      if (dmsg instanceof ReplyMessage) {
        ReplyMessage replyMessage = (ReplyMessage) dmsg;
        if (msg != null) {
          boolean discard = !msg.processReply(replyMessage, this);
          if (discard) {
            return;
          }
        }
        if (replyMessage.getClosed()) {
          closedMembers.add(replyMessage.getSender());
        }
      }

      super.process(dmsg, warn);
    }

  }
}
