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

package org.apache.geode.distributed.internal.locks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.distributed.LockServiceDestroyedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ProcessorKeeper21;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Provides handling of remote and local lock requests. <br>
 * A lock client sends a <code>DLockRequestMessage</code> to the lock grantor and then blocks,
 * waiting for the reply. <br>
 * When the lock grantor grants or times out the request, a <code>DLockResponseMessage</code> is
 * finally sent back to the waiting client.
 *
 */
public class DLockRequestProcessor extends ReplyProcessor21 {
  private static final Logger logger = LogService.getLogger();

  protected final DLockRequestMessage request;

  private final DistributionManager dm;

  protected final DLockService svc;

  private final InternalDistributedMember grantor;

  private volatile boolean gotLock = false;

  private DLockResponseMessage response;

  private boolean disableAlerts;

  @Override
  protected boolean processTimeout() {
    // if this returns false then no need to log warning/severe msg as Dlock request will have
    // timeout
    return !disableAlerts;
  }

  // private volatile boolean doneProcessing = false;

  // private final long grantorVersion;

  protected static ProcessorKeeper21 getKeeper() {
    return ReplyProcessor21.keeper;
  }

  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------

  protected DLockRequestProcessor(LockGrantorId lockGrantorId, DLockService svc, Object objectName,
      int threadId, long startTime, long leaseMillis, long waitMillis, boolean reentrant,
      boolean tryLock, DistributionManager dm) {
    this(lockGrantorId, svc, objectName, threadId, startTime, leaseMillis, waitMillis, reentrant,
        tryLock, false, dm, false);
  }

  protected DLockRequestProcessor(LockGrantorId lockGrantorId, DLockService svc, Object objectName,
      int threadId, long startTime, long leaseMillis, long waitMillis, boolean reentrant,
      boolean tryLock, boolean disableAlerts, DistributionManager dm) {
    this(lockGrantorId, svc, objectName, threadId, startTime, leaseMillis, waitMillis, reentrant,
        tryLock, disableAlerts, dm, false);
  }

  protected DLockRequestProcessor(LockGrantorId lockGrantorId, DLockService svc, Object objectName,
      int threadId, long startTime, long leaseMillis, long waitMillis, boolean reentrant,
      boolean tryLock, DistributionManager dm, boolean async) {
    this(lockGrantorId, svc, objectName, threadId, startTime, leaseMillis, waitMillis, reentrant,
        tryLock, false, dm, false);
  }

  protected DLockRequestProcessor(LockGrantorId lockGrantorId, DLockService svc, Object objectName,
      int threadId, long startTime, long leaseMillis, long waitMillis, boolean reentrant,
      boolean tryLock, boolean disableAlerts, DistributionManager dm, boolean async) {
    super(dm, lockGrantorId.getLockGrantorMember());

    this.svc = svc;
    this.dm = dm;
    this.grantor = lockGrantorId.getLockGrantorMember();
    // this.grantorVersion = grantorVersion;

    this.request = createRequest();
    Assert.assertTrue(getProcessorId() > 0);

    this.request.processorId = getProcessorId();
    this.request.serviceName = svc.getName();
    this.request.objectName = objectName;
    this.request.threadId = threadId;
    this.request.startTime = startTime;
    this.request.leaseMillis = leaseMillis;
    this.request.waitMillis = waitMillis;
    this.request.reentrant = reentrant;
    this.request.tryLock = tryLock;
    this.request.grantorVersion = lockGrantorId.getLockGrantorVersion();
    this.request.grantorSerialNumber = lockGrantorId.getLockGrantorSerialNumber();
    this.request.dlsSerialNumber = svc.getSerialNumber();

    this.request.setRecipient(grantor);
    this.disableAlerts = disableAlerts;
  }

  protected DLockRequestMessage createRequest() {
    return new DLockRequestMessage();
  }

  protected CancelCriterion getCancelCriterion(DistributionManager ignoreDM) {
    return this.svc.getCancelCriterion();
  }

  boolean repliedDestroyed() {
    if (this.response == null) {
      return false;
    }
    boolean result = this.response.responseCode == DLockResponseMessage.DESTROYED;
    return result;
  }

  boolean repliedNotHolder() {
    if (this.response == null) {
      return false;
    }
    boolean result = this.response.responseCode == DLockResponseMessage.NOT_HOLDER;
    return result;
  }

  boolean repliedNotGrantor() {
    if (this.response == null) {
      return false;
    }
    boolean result = this.response.responseCode == DLockResponseMessage.NOT_GRANTOR;
    return result;
  }

  boolean hadNoResponse() {
    return this.response == null;
  }

  boolean tryLockFailed() {
    if (this.response == null) {
      return false;
    }
    boolean result = this.response.responseCode == DLockResponseMessage.TRY_LOCK_FAILED;
    return result;
  }

  String getResponseCodeString() {
    if (this.response == null)
      return null;
    return DLockResponseMessage.responseCodeToString(this.response.responseCode);
  }

  public DLockResponseMessage getResponse() {
    return this.response;
  }

  long getLeaseExpireTime() {
    return this.response.leaseExpireTime;
  }

  protected boolean requestLock(boolean interruptible, int lockId) throws InterruptedException {
    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS_VERBOSE);

    Assert.assertTrue(lockId > -1, "lockId is < 0: " + this);
    this.request.lockId = lockId;

    // local grantor... don't use messaging... fake it
    if (isLockGrantor()) {
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS_VERBOSE,
            "DLockRequestProcessor processing lock request directly");
      }
      this.request.setSender(this.dm.getDistributionManagerId());

      // calls processor (this) process...
      this.request.processLocally(this.dm);
    }

    // remote grantor... use messaging
    else {
      // send the message...
      this.dm.putOutgoing(this.request);
    }

    if (interruptible) {
      try {
        waitForReplies();
      } catch (ReplyException ex) {
        if (ex.getCause() instanceof InterruptedException) {
          throw (InterruptedException) ex.getCause();
        }
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS_VERBOSE, "DLockRequestProcessor caught ReplyException", ex);
        }
        return false;
      }
    } else { // not interruptible
      try {
        waitForRepliesUninterruptibly();
      } catch (ReplyException ex) {
        if (ex.getCause() instanceof InterruptedException) {
          throw (InterruptedException) ex.getCause();
        }
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS_VERBOSE, "DLockRequestProcessor caught ReplyException", ex);
        }
        return false;
      }
    }

    if (isDebugEnabled_DLS) {
      logger.trace(LogMarker.DLS_VERBOSE, "DLockRequestProcessor {} for {}",
          (this.gotLock ? "got lock" : "failed to get lock"), this.request);
    }
    return this.gotLock;
  }

  @Override
  protected boolean allowReplyFromSender() {
    return true;
  }

  private boolean isLockGrantor() {
    return this.dm.getDistributionManagerId().equals(this.grantor);
  }

  Object getKeyIfFailed() {
    if (this.gotLock || this.response == null)
      return null;
    return this.response.keyIfFailed;
  }

  protected boolean gotLock() {
    return this.gotLock;
  }

  @Override
  public void process(DistributionMessage msg) {
    final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS_VERBOSE);
    try {
      Assert.assertTrue(msg instanceof DLockResponseMessage,
          "DLockRequestProcessor is unable to process message of type " + msg.getClass());

      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS_VERBOSE, "Processing DLockResponseMessage: '{}'", msg);
      }
      final DLockResponseMessage reply = (DLockResponseMessage) msg;
      this.response = reply;

      if (this.response.getLockId() != this.request.getLockId()) {
        // Ignore this response since it was sent for a lockId that
        // must have timed out.
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS_VERBOSE,
              "Failed to find processor for lockId {} processor ids must have wrapped.",
              this.response.getLockId());
        }
        Assert.assertTrue(this.response.getLockId() == this.request.getLockId());
      }

      switch (reply.responseCode) {
        case DLockResponseMessage.GRANT:
          // grantor has granted the lock request...
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE, "{} has granted lock for {} in {}",
                reply.getSender(), reply.objectName, reply.serviceName);
          }
          this.gotLock = true;
          break;
        case DLockResponseMessage.NOT_GRANTOR:
          // target was not the grantor! who is the grantor?!
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE,
                "{} has responded DLockResponseMessage.NOT_GRANTOR for {}", reply.getSender(),
                reply.serviceName);
          }
          break;
        case DLockResponseMessage.DESTROYED:
          // grantor claims we sent it a NonGrantorDestroyedMessage
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE,
                "{} has responded DLockResponseMessage.DESTROYED for {}", reply.getSender(),
                reply.serviceName);
          }
          break;
        case DLockResponseMessage.TIMEOUT:
          // grantor told us the lock request has timed out...
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE,
                "{} has responded DLockResponseMessage.TIMEOUT for {} in {}", reply.getSender(),
                reply.objectName, reply.serviceName);
          }
          break;
        case DLockResponseMessage.SUSPENDED:
          // grantor told us that locking has been suspended for the service...
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE,
                "{} has responded DLockResponseMessage.SUSPENDED for {} in {}", reply.getSender(),
                reply.objectName, reply.serviceName);
          }
          break;
        case DLockResponseMessage.NOT_HOLDER:
          // tried to reenter lock but grantor says we're not the lock holder...
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE,
                "{} has responded DLockResponseMessage.NOT_HOLDER for {} in {}", reply.getSender(),
                reply.objectName, reply.serviceName);
          }
          break;
        case DLockResponseMessage.TRY_LOCK_FAILED:
          // tried to acquire try-lock but grantor says it's held and we failed...
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE,
                "{} has responded DLockResponseMessage.TRY_LOCK_FAILED for {} in {}",
                reply.getSender(), reply.objectName, reply.serviceName);
          }
          break;
        default:
          throw new InternalGemFireError(
              String.format("Unknown response code %s",
                  Integer.valueOf(reply.responseCode)));
      } // switch

    } finally {
      super.process(msg);
      if (isDebugEnabled_DLS) {
        logger.trace(LogMarker.DLS_VERBOSE, "Finished processing DLockResponseMessage: '{}'", msg);
      }
      ((DLockResponseMessage) msg).processed = true;
    }
  }

  /**
   * LockGrantorDestroyedException or LockServiceDestroyedException is an anticipated reply
   * exception. Receiving multiple replies with this exception is normal.
   */
  @Override
  protected boolean logMultipleExceptions() {
    return false;
  }

  // -------------------------------------------------------------------------
  // DLockRequestMessage
  // -------------------------------------------------------------------------
  public static class DLockRequestMessage extends HighPriorityDistributionMessage
      implements MessageWithReply {
    /**
     * The id of the DLockRequestProcessor on the initiator node. This will be communicated back in
     * the response to enable collation of the results.
     */
    protected int processorId;

    /** The name of the DistributedLockService */
    protected String serviceName;

    /** The object name */
    protected Object objectName;

    /** Time when lock request was initiated */
    protected long startTime;

    protected long leaseMillis;

    protected long waitMillis;

    /** True if re-entering held lock */
    protected boolean reentrant;

    /** True if try lock and respond immediately without scheduling */
    protected boolean tryLock;

    /** Uniquely identifies this request for later releasing or re-entry */
    protected int lockId;

    /** Uniquely identifies the thread making this request */
    protected int threadId;

    /** version of the grantor that this request is targeted toward */
    protected long grantorVersion;

    /** serial number of grantor's DLS that this request is going to */
    protected int grantorSerialNumber;

    /** serial number of the DLockService that originated this request */
    protected int dlsSerialNumber;

    protected transient DLockService svc;
    protected transient DLockGrantor grantor;
    private transient long statStart = -1;
    private transient volatile DistributionManager receivingDM;
    private transient DLockResponseMessage response;
    private transient RemoteThread rThread;

    /** True if we've responded to this request */
    private boolean responded = false;

    public DLockRequestMessage() {}

    public boolean isLocal() {
      Assert.assertTrue(this.receivingDM != null);
      return getSender().equals(this.receivingDM.getId());
    }

    public boolean isTryLock() {
      return this.tryLock;
    }

    @Override
    public int getProcessorId() {
      return this.processorId;
    }

    public Object getObjectName() {
      return this.objectName;
    }

    public long getStartTime() {
      return this.startTime;
    }

    public long getLeaseTime() {
      return this.leaseMillis;
    }

    public long getWaitTime() {
      return this.waitMillis;
    }

    public int getLockId() {
      return this.lockId;
    }

    public int getThreadId() {
      return this.threadId;
    }

    public long getGrantorVersion() {
      return this.grantorVersion;
    }

    public int getGrantorSerialNumber() {
      return this.grantorSerialNumber;
    }

    private final transient Object rThreadLock = new Object();

    public RemoteThread getRemoteThread() {
      synchronized (this.rThreadLock) {
        if (this.rThread == null) {
          // grantor will need RemoteThread to process this request...
          this.rThread = new RemoteThread(getSender(), getThreadId());
        }
        return this.rThread;
      }
    }

    boolean isSuspendLockingRequest() {
      return getObjectName().equals(DLockService.SUSPEND_LOCKING_TOKEN);
    }

    // void setReceivingDM(DM dm) {
    // this.receivingDM = dm;
    //
    // this.response = createResponse();
    // this.response.setProcessorId(getProcessorId());
    // this.response.setRecipient(getSender());
    // this.response.serviceName = this.serviceName;
    // this.response.objectName = this.objectName;
    // }

    private long startGrantWait() {
      return DLockService.getDistributedLockStats().startGrantWait();
    }

    protected DLockResponseMessage createResponse() {
      return new DLockResponseMessage();
    }

    /**
     * Processes this message - invoked on the node that is the lock grantor.
     */
    @Override
    protected void process(final ClusterDistributionManager dm) {
      boolean failed = false;
      Throwable replyException = null;
      try {
        this.statStart = startGrantWait();
        this.svc = DLockService.getInternalServiceNamed(this.serviceName);
        if (this.svc == null) {
          failed = false; // basicProcess has it's own finally-block w reply
          basicProcess(dm, false); // don't have a grantor anymore
        } else {
          executeBasicProcess(dm); // use executor
        }
        failed = false; // nothing above threw anything
      } catch (RuntimeException e) {
        replyException = e;
        throw e;
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        replyException = e;
        throw e;
      } catch (Error e) {
        SystemFailure.checkFailure();
        replyException = e;
        throw e;
      } finally {
        if (failed) {
          // above code failed so now ensure reply is sent
          if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
            logger.trace(LogMarker.DLS_VERBOSE, "DLockRequestMessage.process failed for <{}>",
                this);
          }
          this.response = createResponse();
          this.response.setProcessorId(getProcessorId());
          this.response.setRecipient(getSender());
          this.response.serviceName = this.serviceName;
          this.response.objectName = this.objectName;
          this.response.lockId = this.lockId;
          respondWithException(replyException);
        }
      }
    }

    /** Process locally without using messaging or executor */
    protected void processLocally(final DistributionManager dm) {
      this.statStart = startGrantWait();
      this.svc = DLockService.getInternalServiceNamed(this.serviceName);
      basicProcess(dm, true); // don't use executor
    }

    /**
     * Execute basicProcess inside Pooled Executor because grantor may not be initializing which
     * will require us to wait.
     * <p>
     * this.svc and this.grantor must be set before calling this method.
     */
    private void executeBasicProcess(final DistributionManager dm) {
      final DLockRequestMessage msg = this;
      dm.getExecutors().getWaitingThreadPool().execute(new Runnable() {
        @Override
        public void run() {
          if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
            logger.trace(LogMarker.DLS_VERBOSE, "calling waitForGrantor {}", msg);
          }
          basicProcess(dm, true);
        }
      });
    }

    protected void basicProcess(final DistributionManager dm, final boolean waitForGrantor) {
      final boolean isDebugEnabled_DLS = logger.isTraceEnabled(LogMarker.DLS_VERBOSE);
      try {
        this.receivingDM = dm;
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS_VERBOSE, "DLockRequestMessage.basicProcess processing <{}>",
              this);
        }
        this.response = createResponse();
        this.response.setProcessorId(getProcessorId());
        this.response.setRecipient(getSender());
        this.response.serviceName = this.serviceName;
        this.response.objectName = this.objectName;
        this.response.lockId = this.lockId;

        if (waitForGrantor && this.svc != null) {
          try {
            this.grantor = DLockGrantor.waitForGrantor(this.svc);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            this.grantor = null; // fail it
          }
        }

        if (this.svc == null || this.grantor == null) {
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE, "respondWithNotGrantor this.svc={} this.grantor={}",
                this.svc, this.grantor);
          }
          respondWithNotGrantor();
        }

        else if (this.grantor.isDestroyed()) {
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE, "respondWithNotGrantor grantor was destroyed {}",
                this.grantor);
          }
          respondWithNotGrantor();
        } else if (this.grantor.getVersionId() != this.grantorVersion) {
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE,
                "respondWithNotGrantor current version is {}; request was for {}",
                this.grantor.getVersionId(), this.grantorVersion);
          }
          respondWithNotGrantor();
        } else if (this.svc.getSerialNumber() != this.grantorSerialNumber) {
          if (isDebugEnabled_DLS) {
            logger.trace(LogMarker.DLS_VERBOSE,
                "respondWithNotGrantor current serial number is {}; request was for {}",
                this.svc.getSerialNumber(), this.grantorSerialNumber);
          }
          respondWithNotGrantor();
        }

        // this is the grantor, so the request will be processed...
        else {
          this.svc.checkDestroyed();
          if (!this.svc.isLockGrantor()) {
            if (isDebugEnabled_DLS) {
              logger.trace(LogMarker.DLS_VERBOSE,
                  "respondWithNotGrantor service !isLockGrantor svc={}", this.svc);
            }
            respondWithNotGrantor();
          }
          this.grantor.checkDestroyed();

          // handle lock re-entry...
          if (this.reentrant) {
            long leaseExpireTime;
            try {
              leaseExpireTime = this.grantor.reenterLock(this);
            } catch (InterruptedException e) {
              leaseExpireTime = 0; // just fail it
            }
            if (leaseExpireTime == 0) {
              respondWithNotHolder();
            } else {
              respondWithGrant(leaseExpireTime);
            }
          }

          // queue up this request to be granted...
          else {
            if (isDebugEnabled_DLS) {
              logger.trace(LogMarker.DLS_VERBOSE, "Handling lock request: <{}>", this);
            }
            if (this.grantor.isDestroyed()) {
              if (isDebugEnabled_DLS) {
                logger.trace(LogMarker.DLS_VERBOSE,
                    "respondWithNotGrantor grantor was destroyed grantor={}", this.grantor);
              }
              respondWithNotGrantor();
            } else {
              try {
                this.grantor.handleLockRequest(this);
              } catch (InterruptedException | LockGrantorDestroyedException e) {
                // just fail it
                respondWithNotGrantor();
              }
            }
          }
        }
      } catch (LockGrantorDestroyedException e) {
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS_VERBOSE,
              "LockGrantorDestroyedException respondWithNotGrantor svc={}", this.svc);
        }
        respondWithNotGrantor();
      } catch (LockServiceDestroyedException e) {
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS_VERBOSE,
              "LockServiceDestroyedException respondWithNotGrantor svc={}", this.svc);
        }
        respondWithNotGrantor();
      } catch (CancelException e) {
        if (isDebugEnabled_DLS) {
          logger.trace(LogMarker.DLS_VERBOSE,
              "CacheClosedException respondWithNotGrantor svc={} exception = {}", this.svc, e);
        }
        if (isLocal()) {
          throw e;
        } else {
          respondWithNotGrantor();
        }
      } catch (RuntimeException e) {
        logger.warn(LogMarker.DLS_MARKER,
            "[DLockRequestMessage.process] Caught throwable:",
            e);
        respondWithException(e);
      }
    }

    synchronized void respondWithNotGrantor() {
      this.response.responseCode = DLockResponseMessage.NOT_GRANTOR;
      sendResponse();
    }

    synchronized void respondWithDestroyed() {
      this.response.responseCode = DLockResponseMessage.DESTROYED;
      sendResponse();
    }

    private synchronized void respondWithNotHolder() {
      this.response.responseCode = DLockResponseMessage.NOT_HOLDER;
      sendResponse();
    }

    /** Callers must be synchronized on this */
    private void respondWithTimeout() {
      if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
        logger.trace(LogMarker.DLS_VERBOSE, "Request {} timed out; grantor status = {}", this,
            this.grantor.displayStatus(rThread, objectName));
      }
      this.response.responseCode = DLockResponseMessage.TIMEOUT;
      sendResponse();
    }

    synchronized void respondWithTryLockFailed(Object keyIfFailed) {
      this.response.responseCode = DLockResponseMessage.TRY_LOCK_FAILED;
      this.response.keyIfFailed = keyIfFailed;
      sendResponse();
    }

    synchronized void respondWithGrant(long leaseExpireTime) {
      this.response.responseCode = DLockResponseMessage.GRANT;
      this.response.leaseExpireTime = leaseExpireTime;
      this.response.dlsSerialNumber = this.dlsSerialNumber;
      sendResponse();
    }

    synchronized void respondWithException(Throwable t) {
      try {
        if (this.response.getException() == null) {
          this.response.setException(new ReplyException(t));
          if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
            logger.trace(LogMarker.DLS_VERBOSE,
                "While processing <{}>, got exception, returning to sender", this,
                this.response.getException());
          }
        } else {
          logger.warn(LogMarker.DLS_VERBOSE,
              String.format("More than one exception thrown in %s",
                  this),
              t);
        }
      } finally {
        sendResponse();
      }
    }

    /**
     * Return the timestamp at which this request should timeout. If it should never timeout then
     * returns Long.MAX_VALUE.
     */
    long getTimeoutTS() {
      if (this.waitMillis == -1 || this.waitMillis == Long.MAX_VALUE) {
        return Long.MAX_VALUE;
      } else {
        long result = this.startTime + this.waitMillis;
        if (result < this.startTime) {
          result = Long.MAX_VALUE;
        }
        return result;
      }
    }

    synchronized boolean checkForTimeout() {
      if (this.waitMillis == -1 || this.waitMillis == Long.MAX_VALUE)
        return false;
      if (this.tryLock)
        return false;
      long now = DLockService.getLockTimeStamp(this.receivingDM);
      if (now < this.startTime)
        now = this.startTime;
      if (this.waitMillis + this.startTime - now <= 0) {
        if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
          logger.trace(LogMarker.DLS_VERBOSE,
              "DLockRequestProcessor request timed out: waitMillis={} now={} startTime={}",
              this.waitMillis, now, this.startTime);
        }
        respondWithTimeout();
        return true;
      }
      return false;
    }

    private void endGrantWaitStatistic() {
      if (this.statStart == -1)
        return; // failed to start the stat
      DistributedLockStats stats = DLockService.getDistributedLockStats();
      switch (this.response.responseCode) {
        case DLockResponseMessage.GRANT:
          stats.endGrantWait(this.statStart);
          break;
        case DLockResponseMessage.NOT_GRANTOR:
          stats.endGrantWaitNotGrantor(this.statStart);
          break;
        case DLockResponseMessage.TIMEOUT:
          stats.endGrantWaitTimeout(this.statStart);
          break;
        case DLockResponseMessage.SUSPENDED:
          stats.endGrantWaitSuspended(this.statStart);
          break;
        case DLockResponseMessage.NOT_HOLDER:
          stats.endGrantWaitNotHolder(this.statStart);
          break;
        case DLockResponseMessage.TRY_LOCK_FAILED:
          stats.endGrantWaitFailed(this.statStart);
          break;
        case DLockResponseMessage.DESTROYED:
          stats.endGrantWaitDestroyed(this.statStart);
          break;
        default:
          Assert.assertTrue(false, "Unknown responseCode: " + this.response.responseCode);
          break;
      }
    }

    /** Callers must be synchronized on this */
    private void sendResponse() {
      try {
        if (this.responded)
          return;
        InternalDistributedMember myId = this.receivingDM.getDistributionManagerId();

        // local... don't actually use messaging
        if (getSender().equals(myId)) {
          if (debugReleaseOrphanedGrant()) {
            waitToProcessDLockResponse(this.receivingDM);
          }
          ReplyProcessor21 processor = ReplyProcessor21.getProcessor(processorId);
          if (processor == null) {
            // lock request was probably interrupted so we need to release it...
            logger.warn(LogMarker.DLS_MARKER,
                "Failed to find processor for {}",
                this.response);
            if (this.response.responseCode == DLockResponseMessage.GRANT) {
              logger.info(LogMarker.DLS_MARKER,
                  "Releasing local orphaned grant for {}.",
                  this);
              try {
                this.grantor.releaseIfLocked(this.objectName, getSender(), this.lockId);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while releasing grant.", e);
              }
              logger.info("Handled local orphaned grant.");
            }
            endGrantWaitStatistic();
            return;
          }
          this.response.setSender(getSender());
          endGrantWaitStatistic();
          processor.process(this.response);
        }

        // remote... use messaging
        else {
          this.receivingDM.putOutgoing(this.response);
          endGrantWaitStatistic();
        }
      } finally {
        this.responded = true;
      }
    }

    synchronized void handleDepartureOfSender() {
      try {
        if (this.receivingDM.getDistributionManagerIds().contains(this.sender)) {
          // sender must have sent us a NonGrantorDestroyedMessage
          // still need to send a reply to make the thread stop waiting
          respondWithDestroyed();
        }
      } finally {
        if (!this.responded) {
          endGrantWaitStatistic();
          this.responded = true;
        }
      }
    }

    synchronized boolean responded() {
      return this.responded;
    }

    /** Callers must be synchronized on this */
    boolean respondedNoSync() {
      return this.responded;
    }

    @Override
    public int getDSFID() {
      return DLOCK_REQUEST_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeUTF(this.serviceName);
      DataSerializer.writeObject(this.objectName, out);
      out.writeLong(this.startTime);
      out.writeLong(this.leaseMillis);
      out.writeLong(this.waitMillis);
      out.writeBoolean(this.reentrant);
      out.writeBoolean(this.tryLock);
      out.writeInt(this.processorId);
      out.writeInt(this.lockId);
      out.writeInt(this.threadId);
      out.writeLong(this.grantorVersion);
      out.writeInt(this.grantorSerialNumber);
      out.writeInt(this.dlsSerialNumber);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.serviceName = in.readUTF();
      this.objectName = DataSerializer.readObject(in);
      this.startTime = in.readLong();
      this.leaseMillis = in.readLong();
      this.waitMillis = in.readLong();
      this.reentrant = in.readBoolean();
      this.tryLock = in.readBoolean();
      this.processorId = in.readInt();
      this.lockId = in.readInt();
      this.threadId = in.readInt();
      this.grantorVersion = in.readLong();
      this.grantorSerialNumber = in.readInt();
      this.dlsSerialNumber = in.readInt();
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();

      sb.append("{DLockRequestMessage id=" + this.processorId);
      sb.append(" for " + this.serviceName + ":" + this.dlsSerialNumber);
      sb.append(" name=" + this.objectName);
      sb.append(" start=" + this.startTime);
      sb.append(" sender=" + getSender());
      sb.append(" threadId=" + this.threadId);
      sb.append(" leaseMillis=" + this.leaseMillis);
      sb.append(" waitMillis=" + this.waitMillis);
      sb.append(" reentrant=" + this.reentrant);
      sb.append(" tryLock=" + this.tryLock);
      sb.append(" lockId=" + this.lockId);
      sb.append(" grantorVersion=" + this.grantorVersion);
      sb.append(" grantorSerialNumber=" + this.grantorSerialNumber);
      sb.append(" dlsSerialNumber=" + this.dlsSerialNumber);
      sb.append("}");
      return sb.toString();
    }

  }

  // -------------------------------------------------------------------------
  // DLockResponseMessage
  // -------------------------------------------------------------------------
  /**
   * This is a response to an DLockRequestMessage. A response communicates one of two things: -
   * GRANT - you can have the lock - NOT_GRANTOR - I am not the lock grantor for this service -
   * TIMEOUT - the lock request has timed out
   */
  public static class DLockResponseMessage extends ReplyMessage {

    public static final int GRANT = 0;
    public static final int NOT_GRANTOR = 1;
    public static final int TIMEOUT = 2;
    public static final int NOT_HOLDER = 3; // reentrant locking attempted
    public static final int TRY_LOCK_FAILED = 4; // try lock failed
    public static final int SUSPENDED = 5; // dlock has suspended locking
    public static final int DESTROYED = 6; // requestor sent NonGrantorDestroyedMessage

    /** The name of the DistributedLockService */
    protected String serviceName;

    /** The object name */
    protected Object objectName;

    /** Specifies the results of this response */
    protected int responseCode = NOT_GRANTOR; // default

    /** Absolute cache time millis when the lease expires */
    protected long leaseExpireTime;

    /** Starts out null and then set to key that conflicted for failure */
    protected Object keyIfFailed;

    /** Used to match this release up with its original request */
    protected int lockId;

    /** The serial number of the dlock service that requested this lock */
    protected int dlsSerialNumber;

    // set during processing of this message...
    /** True if the receiving node has processed this reply */
    protected boolean processed;

    public DLockResponseMessage() {}

    /**
     * Need to handle race condition in which this side times out waiting for the lock before
     * receiving a GRANT response which may already be in transit to this node.
     */
    @Override
    public void process(final DistributionManager dm, final ReplyProcessor21 processor) {
      if (processor == null) {
        // The processor was probably cleaned up because of memberDeparted and we need to abandon
        return;
      }
      if (debugReleaseOrphanedGrant()) {
        waitToProcessDLockResponse(dm);
      }


      // TODO - This a partial fix for bug 37158. It doesn't completely
      // eliminate the race condition when interrupting a lock request, but
      // it does make DistributedLockServiceTest continue to pass.
      if (keeper.retrieve(processor.getProcessorId()) != null) {
        super.process(dm, processor);
      }
      if (!this.processed) {
        if (this.responseCode == GRANT) {
          logger.warn("No processor found for DLockResponseMessage: {}",
              this);
          // got a problem... response prolly came in after client side timed out
          releaseOrphanedGrant(dm);
        } else {
          logger.info("No processor found for DLockResponseMessage: {}",
              this);
        }
      }
    }

    protected boolean callReleaseProcessor(DistributionManager dm,
        InternalDistributedMember grantor) {
      return DLockService.callReleaseProcessor(dm, this.serviceName, grantor, this.objectName,
          false, this.lockId);
    }

    /**
     * Releases a granted lock that was orphaned by interruption of the lock request. This also
     * releases any lock grant for which we cannot find an active reply processor.
     */
    public void releaseOrphanedGrant(DistributionManager dm) {
      InternalDistributedMember grantor = getSender();
      // method is rewritten to fix bug 35252
      boolean released = false;
      logger.info("Releasing orphaned grant for  {}", this);
      try {
        while (!released) {
          dm.getCancelCriterion().checkCancelInProgress(null);
          try {
            if (grantor == null) { // use grantor arg on first iteration
              GrantorInfo gi = DLockService.checkLockGrantorInfo(this.serviceName, dm.getSystem());
              grantor = gi.getId();
            }
            if (grantor == null) { // still null if elder says no one is grantor
              // nobody knows about our zombie lock so exit
              released = true;
            } else {
              released = callReleaseProcessor(dm, grantor);
            }
          } catch (LockGrantorDestroyedException e) {
            // loop back around to get next lock grantor
          } catch (IllegalStateException e) {
            if (dm.getId().equals(grantor)) {
              // DLockToken probably threw IllegalStateException because destroyed
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "[releaseOrphanedGrant] Local grantor threw IllegalStateException handling {}",
                    this);
              }
            }
            try {
              Thread.sleep(200);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
            }
          } finally {
            grantor = null;
          }
        }
      } finally {
        if (released) {
          logger.info("Handled orphaned grant with release.");
        } else {
          logger.info("Handled orphaned grant without release.");
        }
      }
    }

    public int getLockId() {
      return this.lockId;
    }

    public int getResponseCode() {
      return this.responseCode;
    }

    public void setResponseCode(int code) {
      this.responseCode = code;
    }

    public static String responseCodeToString(int responseCode) {
      String response = null;
      switch (responseCode) {
        case GRANT:
          response = "GRANT";
          break;
        case NOT_GRANTOR:
          response = "NOT_GRANTOR";
          break;
        case TIMEOUT:
          response = "TIMEOUT";
          break;
        case SUSPENDED:
          response = "SUSPENDED";
          break;
        case NOT_HOLDER:
          response = "NOT_HOLDER";
          break;
        case TRY_LOCK_FAILED:
          response = "TRY_LOCK_FAILED";
          break;
        case DESTROYED:
          response = "DESTROYED";
          break;
        default:
          response = "UNKNOWN:" + String.valueOf(responseCode);
          break;
      }
      return response;
    }

    @Override
    public int getDSFID() {
      return DLOCK_RESPONSE_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeByte(this.responseCode);
      out.writeUTF(this.serviceName);
      DataSerializer.writeObject(this.objectName, out);
      out.writeLong(this.leaseExpireTime);
      DataSerializer.writeObject(this.keyIfFailed, out);
      out.writeInt(this.lockId);
      out.writeInt(this.dlsSerialNumber);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.responseCode = in.readByte();
      this.serviceName = in.readUTF();
      this.objectName = DataSerializer.readObject(in);
      this.leaseExpireTime = in.readLong();
      this.keyIfFailed = DataSerializer.readObject(in);
      this.lockId = in.readInt();
      this.dlsSerialNumber = in.readInt();
    }

    @Override
    public String toString() {
      String response = responseCodeToString(this.responseCode);
      return "DLockRequestProcessor.DLockResponseMessage " + "responding " + response
          + "; serviceName=" + serviceName + "(version " + dlsSerialNumber + ")" + "; objectName="
          + objectName + "; responseCode=" + responseCode + "; keyIfFailed=" + keyIfFailed
          + "; leaseExpireTime=" + leaseExpireTime + "; processorId=" + this.processorId
          + "; lockId=" + this.lockId;
    }
  }

  @MutableForTesting
  private static boolean debugReleaseOrphanedGrant = false;
  private static final Object waitToProcessDLockResponseLock = new Object();
  @MutableForTesting
  private static volatile boolean waitToProcessDLockResponse = false;

  public static boolean debugReleaseOrphanedGrant() {
    return debugReleaseOrphanedGrant;
  }

  public static void setDebugReleaseOrphanedGrant(boolean value) {
    debugReleaseOrphanedGrant = value;
  }

  public static void setWaitToProcessDLockResponse(boolean value) {
    synchronized (waitToProcessDLockResponseLock) {
      waitToProcessDLockResponse = value;
      waitToProcessDLockResponseLock.notifyAll();
    }
  }

  public static void waitToProcessDLockResponse(DistributionManager dm) {
    synchronized (waitToProcessDLockResponseLock) {
      while (waitToProcessDLockResponse) {
        dm.getCancelCriterion().checkCancelInProgress(null);
        boolean interrupted = Thread.interrupted();
        try {
          logger.info("Waiting to process DLockResponseMessage");
          waitToProcessDLockResponseLock.wait();
        } catch (InterruptedException e) {
          interrupted = true;
        } finally {
          if (interrupted)
            Thread.currentThread().interrupt();
        }
      }
    }
  }

}
