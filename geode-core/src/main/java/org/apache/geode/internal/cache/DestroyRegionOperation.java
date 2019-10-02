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

import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.BEFORE_INITIAL_IMAGE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.LocalRegion.InitializationLevel;
import org.apache.geode.internal.cache.partitioned.PRLocallyDestroyedException;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

public class DestroyRegionOperation extends DistributedCacheOperation {

  private static final Logger logger = LogService.getLogger();

  @Override
  public boolean supportsDirectAck() {
    // Part of fix for bug 34450
    return false; // Changed to prevent problems with executing
                  // basicDestroyRegion in the waiting pool.
  }

  @Override
  protected boolean supportsAdjunctMessaging() {
    return false;
  }

  @Override
  public boolean canBeSentDuringShutdown() {
    return true;
  }

  private final boolean notifyOfRegionDeparture;

  private static final ThreadLocal regionDepartureNotificationDisabled = new ThreadLocal();

  /**
   * This was added to fix bug 41111
   */
  public static boolean isRegionDepartureNotificationOk() {
    return regionDepartureNotificationDisabled.get() != Boolean.TRUE;
  }

  /**
   * Creates new instance of DestroyRegionOperation
   *
   * @param notifyOfRegionDeparture was added to fix bug 41111. If false then don't deliver
   *        afterRemoteRegionDeparture events.
   */
  public DestroyRegionOperation(RegionEventImpl event, boolean notifyOfRegionDeparture) {
    super(event);
    this.notifyOfRegionDeparture = notifyOfRegionDeparture;
  }

  @Override
  protected Set getRecipients() {
    CacheDistributionAdvisor advisor = getRegion().getCacheDistributionAdvisor();
    return advisor.adviseDestroyRegion();
  }

  @Override
  protected boolean shouldAck() {
    return true;
  }

  @Override
  protected CacheOperationMessage createMessage() {
    DestroyRegionMessage mssg;
    if (this.event instanceof ClientRegionEventImpl) {
      mssg = new DestroyRegionWithContextMessage();
      ((DestroyRegionWithContextMessage) mssg).context =
          ((ClientRegionEventImpl) this.event).getContext();
    } else {
      mssg = new DestroyRegionMessage();
    }

    mssg.notifyOfRegionDeparture = this.notifyOfRegionDeparture;
    DistributedRegion rgn = getRegion();
    mssg.serialNum = rgn.getSerialNumber();
    Assert.assertTrue(mssg.serialNum != DistributionAdvisor.ILLEGAL_SERIAL);

    mssg.subregionSerialNumbers = rgn.getDestroyedSubregionSerialNumbers();

    RegionEventImpl rei = (RegionEventImpl) this.event;
    mssg.eventID = rei.getEventId();
    return mssg;
  }

  public static class DestroyRegionMessage extends CacheOperationMessage {
    protected EventID eventID;

    /** serial number of the region to be destroyed */
    protected int serialNum;

    /** map of subregion full paths to serial numbers */
    protected HashMap subregionSerialNumbers;

    protected boolean notifyOfRegionDeparture;

    /**
     * true if need to automatically recreate region, and mark destruction as a reinitialization
     */
    protected transient LocalRegion lockRoot = null; // used for early destroy

    @Override
    protected InternalCacheEvent createEvent(DistributedRegion rgn) throws EntryNotFoundException {
      RegionEventImpl event = createRegionEvent(rgn);
      if (this.filterRouting != null) {
        event.setLocalFilterInfo(
            this.filterRouting.getFilterInfo((InternalDistributedMember) rgn.getMyId()));
      }
      event.setEventID(this.eventID);
      return event;
    }

    protected RegionEventImpl createRegionEvent(DistributedRegion rgn) {
      return new RegionEventImpl(rgn, getOperation(), this.callbackArg, true /* originRemote */,
          getSender());
    }

    private Runnable destroyOp(final ClusterDistributionManager dm, final LocalRegion lclRgn,
        final boolean sendReply) {
      return new Runnable() {
        @Override
        public void run() {
          final InitializationLevel oldLevel =
              LocalRegion.setThreadInitLevelRequirement(BEFORE_INITIAL_IMAGE);

          Throwable thr = null;
          try {
            if (lclRgn == null) {
              // following block is specific to buckets...
              // need to wait for queued bucket profiles to be processed
              // or this destroy may do nothing and result in a stale profile
              boolean waitForBucketInitializationToComplete = true;
              CacheDistributionAdvisee advisee = null;
              try {
                advisee = PartitionedRegionHelper.getProxyBucketRegion(dm.getCache(), regionPath,
                    waitForBucketInitializationToComplete);
              } catch (PRLocallyDestroyedException ignore) {
                // region not found - it's been destroyed
              } catch (RegionDestroyedException ignore) {
                // ditto
              } catch (PartitionedRegionException e) {
                if (!e.getMessage().contains("destroyed")) {
                  throw e;
                }
                // region failed registration & is unusable
              }

              if (advisee != null) {
                boolean isDestroy = op.isRegionDestroy() && !op.isClose();
                advisee.getDistributionAdvisor().removeIdWithSerial(getSender(), serialNum,
                    isDestroy);
              } else if (logger.isDebugEnabled()) {
                logger.debug("{} region not found, nothing to do", this);
              }
              return;
            } // lclRegion == null

            // refetch to use special destroy region logic
            final LocalRegion lr = getRegionFromPath(dm, lclRgn.getFullPath());
            if (lr == null) {
              if (logger.isDebugEnabled())
                logger.debug("{} region not found, nothing to do", this);
              return;
            }
            // In some subclasses, lclRgn may be destroyed, so be careful not
            // to
            // allow a RegionDestroyedException to be thrown on lclRgn access
            if (!(lr instanceof DistributedRegion)) {
              if (logger.isDebugEnabled())
                logger.debug("{} local scope region, nothing to do", this);
              return;
            }
            DistributedRegion rgn = (DistributedRegion) lr;

            InternalCacheEvent event = createEvent(rgn);
            if (DestroyRegionMessage.this.needsRouting
                && lclRgn.cache.getCacheServers().size() > 0) {
              lclRgn.generateLocalFilterRouting(event);
            }

            doRegionDestroy(event);
          } catch (RegionDestroyedException ignore) {
            logger.debug("{} Region destroyed: nothing to do", this);
          } catch (CancelException ignore) {
            logger.debug("{} Cancelled: nothing to do", this);
          } catch (EntryNotFoundException ignore) {
            logger.debug("{} Entry not found, nothing to do", this);
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

            if (DestroyRegionMessage.this.lockRoot != null) {
              DestroyRegionMessage.this.lockRoot.releaseDestroyLock();
            }

            if (sendReply) {
              if (DestroyRegionMessage.this.processorId != 0) {
                ReplyException rex = null;
                if (thr != null) {
                  rex = new ReplyException(thr);
                }
                sendReply(getSender(), DestroyRegionMessage.this.processorId, rex,
                    getReplySender(dm));
              }
            } else if (thr != null) {
              logger.error(String.format("Exception while processing [ %s ]", this),
                  thr);
            }
          }
        } // run
      };
    }

    /** Return true if a reply should be sent */
    @Override
    protected void basicProcess(final ClusterDistributionManager dm, final LocalRegion lclRgn) {
      Assert.assertTrue(this.serialNum != DistributionAdvisor.ILLEGAL_SERIAL);
      try {
        this.lockRoot = null;
        // may set lockRoot to the root region where destroyLock is acquired

        final boolean sendReply = true;

        // Part of fix for bug 34450 which was caused by a PR destroy region op
        // dead-locked with
        // a PR create region op. The create region op required an entry update
        // to release a
        // DLock needed by the PR destroy.. by moving the destroy to the waiting
        // pool, the entry
        // update is allowed to complete.
        dm.getExecutors().getWaitingThreadPool().execute(destroyOp(dm, lclRgn, sendReply));
      } catch (RejectedExecutionException ignore) {
        // rejected while trying to execute destroy thread
        // must be shutting down, just quit
      }
    }

    protected LocalRegion getRegionFromPath(ClusterDistributionManager dm, String path) {
      // allow a destroyed region to be returned if we're dealing with a
      // shared region, since another cache may
      // have already destroyed it in shared memory, in which our listeners
      // still need to be called and java region object cleaned up.
      InternalCache cache = dm.getExistingCache();

      // only get the region while holding the appropriate destroy lock.
      // this prevents us from getting a "stale" region
      if (getOperation().isDistributed()) {
        String rootName = GemFireCacheImpl.parsePath(path)[0];
        this.lockRoot = (LocalRegion) cache.getRegion(rootName);
        if (this.lockRoot == null)
          return null;
        this.lockRoot.acquireDestroyLock();
      }

      return (LocalRegion) cache.getRegion(path);
    }

    private void disableRegionDepartureNotification() {
      if (!this.notifyOfRegionDeparture) {
        regionDepartureNotificationDisabled.set(Boolean.TRUE);
      }
    }

    private void enableRegionDepartureNotification() {
      if (!this.notifyOfRegionDeparture) {
        regionDepartureNotificationDisabled.remove();
      }
    }

    protected boolean doRegionDestroy(CacheEvent event) throws EntryNotFoundException {
      this.appliedOperation = true;
      RegionEventImpl ev = (RegionEventImpl) event;
      final DistributedRegion rgn = (DistributedRegion) ev.region;

      if (getOperation().isLocal()) {
        Assert.assertTrue(serialNum != DistributionAdvisor.ILLEGAL_SERIAL);
        disableRegionDepartureNotification();
        try {
          rgn.handleRemoteLocalRegionDestroyOrClose(getSender(), serialNum, subregionSerialNumbers,
              !getOperation().isClose());
        } finally {
          enableRegionDepartureNotification();
        }
        return true;
      }

      try {
        String fullPath = null;
        if (logger.isDebugEnabled()) {
          fullPath = rgn.getFullPath();
          StringBuffer subregionNames = new StringBuffer();
          for (Iterator itr = rgn.debugGetSubregionNames().iterator(); itr.hasNext();) {
            subregionNames.append(itr.next());
            subregionNames.append(", ");
          }
          logger.debug(
              "Processing DestroyRegionOperation, about to destroy {}, has immediate subregions: {}",
              fullPath, subregionNames);
        }
        if (getOperation() == Operation.REGION_LOAD_SNAPSHOT) {
          if (logger.isDebugEnabled()) {
            logger.debug("Processing DestroyRegionOperation, calling reinitialize_destroy: {}",
                fullPath);
          }

          // do just the destroy here, then spawn a thread to do the
          // the re-create. This allows the ack to be send for the
          // destroy message before we re-create the region.
          // Don't release the destroy lock until after we re-create
          rgn.reinitialize_destroy(ev);

          final LocalRegion loc_lockRoot = this.lockRoot;
          this.lockRoot = null; // spawned thread will release lock, not
                                // basicProcess

          rgn.getDistributionManager().getExecutors().getWaitingThreadPool()
              .execute(new Runnable() {
                @Override
                public void run() {
                  try {
                    rgn.reinitializeFromImageTarget(getSender());
                  } catch (TimeoutException e) {
                    // dlock timed out, log message
                    logger.warn(String.format(
                        "Got timeout when trying to recreate region during re-initialization: %s",
                        rgn.getFullPath()),
                        e);
                  } catch (IOException e) {
                    // only if loading snapshot, not here
                    InternalGemFireError assErr = new InternalGemFireError(
                        "unexpected exception");
                    assErr.initCause(e);
                    throw assErr;
                  } catch (ClassNotFoundException e) {
                    // only if loading snapshot, not here
                    InternalGemFireError assErr = new InternalGemFireError(
                        "unexpected exception");
                    assErr.initCause(e);
                    throw assErr;
                  } finally {
                    if (loc_lockRoot != null)
                      loc_lockRoot.releaseDestroyLock();
                  }
                }
              });
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("Processing DestroyRegionOperation, calling basicDestroyRegion: {}",
                fullPath);
          }
          rgn.basicDestroyRegion(ev, false /* cacheWrite */, false /* lock */,
              true/* cacheCallbacks */);
        }
      } catch (CacheWriterException ignore) {
        throw new Error(
            "CacheWriter should not have been called");
      } catch (TimeoutException ignore) {
        throw new Error(
            "DistributedLock should not have been acquired");
      } catch (RejectedExecutionException ignore) {
        // rejected while trying to execute recreate thread
        // must be shutting down, so what we were trying to do must not be
        // important anymore, so just quit
      }
      return true;
    }

    @Override
    protected boolean operateOnRegion(CacheEvent event, ClusterDistributionManager dm)
        throws EntryNotFoundException {
      Assert.assertTrue(false,
          "Region Destruction message implementation is in basicProcess, not this method");
      return false;
    }

    @Override
    protected void appendFields(StringBuilder buff) {
      super.appendFields(buff);
      buff.append("; eventID=").append(this.eventID).append("; serialNum=").append(this.serialNum)
          .append("; subregionSerialNumbers=").append(this.subregionSerialNumbers)
          .append("; notifyOfRegionDeparture=").append(this.notifyOfRegionDeparture);
    }

    @Override
    public int getDSFID() {
      return DESTROY_REGION_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.eventID = (EventID) DataSerializer.readObject(in);
      this.serialNum = DataSerializer.readPrimitiveInt(in);
      this.notifyOfRegionDeparture = DataSerializer.readPrimitiveBoolean(in);
      this.subregionSerialNumbers = DataSerializer.readHashMap(in);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeObject(this.eventID, out);
      DataSerializer.writePrimitiveInt(this.serialNum, out);
      DataSerializer.writePrimitiveBoolean(this.notifyOfRegionDeparture, out);
      DataSerializer.writeHashMap(this.subregionSerialNumbers, out);
    }
  }

  public static class DestroyRegionWithContextMessage extends DestroyRegionMessage {

    protected transient Object context;

    @Override
    public RegionEventImpl createRegionEvent(DistributedRegion rgn) {
      return new ClientRegionEventImpl(rgn, getOperation(), this.callbackArg,
          true /* originRemote */, getSender(), (ClientProxyMembershipID) this.context);
    }

    @Override
    protected void appendFields(StringBuilder buff) {
      super.appendFields(buff);
      buff.append("; context=").append(this.context);
    }

    @Override
    public int getDSFID() {
      return DESTROY_REGION_WITH_CONTEXT_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.context = DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeObject(this.context, out);
    }
  }
}
