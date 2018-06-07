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
package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.tcp.ConnectionTable;

/**
 * An instruction to all members with cache that their PR should gracefully close and disconnect DS
 */
public class ShutdownAllRequest extends AdminRequest {

  private static final Logger logger = LogService.getLogger();

  private static final long SLEEP_TIME_BEFORE_DISCONNECT_DS =
      Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "sleep-before-disconnect-ds", 1000);

  public ShutdownAllRequest() {
    // do nothing
  }

  /**
   * Sends a shutdownAll request to all other members and performs local shutdownAll processing in
   * the waitingThreadPool.
   */
  public static Set send(final DistributionManager dm, long timeout) {
    boolean hadCache = hasCache(dm);
    ClusterDistributionManager dism =
        dm instanceof ClusterDistributionManager ? (ClusterDistributionManager) dm : null;
    InternalDistributedMember myId = dm.getDistributionManagerId();

    Set recipients = dm.getOtherNormalDistributionManagerIds();

    recipients.remove(myId);

    // now do shutdownall
    ShutdownAllRequest request = new ShutdownAllRequest();
    request.setRecipients(recipients);

    ShutDownAllReplyProcessor replyProcessor = new ShutDownAllReplyProcessor(dm, recipients);
    request.msgId = replyProcessor.getProcessorId();
    dm.putOutgoing(request);

    if (!InternalLocator.isDedicatedLocator()) {
      if (hadCache && dism != null) {
        AdminResponse response;
        try {
          request.setSender(myId);
          response = request.createResponse(dism);
        } catch (Exception ex) {
          if (logger.isDebugEnabled()) {
            logger.debug("caught exception while processing shutdownAll locally", ex);
          }
          response = AdminFailureResponse.create(myId, ex);
        }
        response.setSender(myId);
        replyProcessor.process(response);
      }
    }

    boolean interrupted = false;
    try {
      if (!replyProcessor.waitForReplies(timeout)) {
        return null;
      }
    } catch (ReplyException e) {
      if (!(e.getCause() instanceof CancelException)) {
        e.handleCause();
      }
    } catch (CancelException ignore) {
      // expected
    } catch (InterruptedException ignore) {
      interrupted = true;
    }

    // wait until all the recipients send response, shut down itself (if not a locator)
    if (hadCache) {
      // at this point,GemFireCacheImpl.getInstance() might return null,
      // because the cache is closed at GemFireCacheImpl.getInstance().shutDownAll()
      if (!InternalLocator.isDedicatedLocator()) {
        InternalDistributedSystem ids = dm.getSystem();
        if (ids.isConnected()) {
          ids.disconnect();
        }
      }
    }

    if (interrupted) {
      Thread.currentThread().interrupt();
    }

    try {
      Thread.sleep(3 * SLEEP_TIME_BEFORE_DISCONNECT_DS);
    } catch (InterruptedException ignore) {
    }
    return replyProcessor.getResults();
  }

  @Override
  public boolean sendViaUDP() {
    return true;
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    boolean isToShutdown = hasCache(dm);
    super.process(dm);

    if (isToShutdown) {
      // Do the disconnect in an async thread. The thread we are running
      // in is one in the dm threadPool so we do not want to call disconnect
      // from this thread because it prevents dm from cleaning up all its threads
      // and causes a 20 second delay.
      final InternalDistributedSystem ids = dm.getSystem();
      if (ids.isConnected()) {
        Thread t = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              Thread.sleep(SLEEP_TIME_BEFORE_DISCONNECT_DS);
            } catch (InterruptedException ignore) {
            }
            ConnectionTable.threadWantsSharedResources();
            if (ids.isConnected()) {
              ids.disconnect();
            }
          }
        });
        t.start();
      }
    }
  }

  private static boolean hasCache(DistributionManager manager) {
    InternalCache cache = manager.getCache();
    return cache != null && !cache.isClosed();
  }

  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    boolean isToShutdown = hasCache(dm);
    if (isToShutdown) {
      boolean isSuccess = false;
      try {
        dm.getCache().shutDownAll();
        isSuccess = true;
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

        if (t instanceof InternalGemFireError) {
          logger.fatal("DistributedSystem is closed due to InternalGemFireError", t);
        } else {
          logger.fatal("DistributedSystem is closed due to unexpected exception", t);
        }
      } finally {
        if (!isSuccess) {
          InternalDistributedMember me = dm.getDistributionManagerId();
          InternalDistributedSystem ids = dm.getSystem();
          if (!this.getSender().equals(me)) {
            if (ids.isConnected()) {
              logger.fatal("ShutdownAllRequest: disconnect distributed without response.");
              ids.disconnect();
            }
          }
        }
      }
    }

    return new ShutdownAllResponse(this.getSender(), isToShutdown);
  }

  @Override
  public int getDSFID() {
    return SHUTDOWN_ALL_REQUEST;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public String toString() {
    return "ShutdownAllRequest sent to " + Arrays.toString(this.getRecipients()) + " from "
        + this.getSender();
  }

  private static class ShutDownAllReplyProcessor extends AdminMultipleReplyProcessor {
    Set<DistributedMember> results = Collections.synchronizedSet(new TreeSet<>());

    ShutDownAllReplyProcessor(DistributionManager dm, Collection initMembers) {
      super(dm, initMembers);
    }

    @Override
    protected boolean stopBecauseOfExceptions() {
      return false;
    }

    /**
     * If response arrives, we will save into results and keep wait for member's departure. If the
     * member is departed before sent response, no wait for its response
     */
    @Override
    public void process(DistributionMessage msg) {
      if (logger.isDebugEnabled()) {
        logger.debug("shutdownAll reply processor is processing {}", msg);
      }
      if (msg instanceof ShutdownAllResponse) {
        if (((ShutdownAllResponse) msg).isToShutDown()) {
          if (logger.isDebugEnabled()) {
            synchronized (results) {
              logger.debug("{} adding {} to result set {}", this, msg.getSender(),
                  results);
            }
          }
          this.results.add(msg.getSender());
        } else {
          // for member without cache, we will not wait for its result
          // so no need to wait its DS to close either
          removeMember(msg.getSender(), false);
        }

        if (msg.getSender().equals(this.dmgr.getDistributionManagerId())) {
          // mark myself as done since my response has been sent and my DS
          // will be closed later anyway
          removeMember(msg.getSender(), false);
        }
      }

      if (msg instanceof ReplyMessage) {
        ReplyException ex = ((ReplyMessage) msg).getException();
        if (ex != null) {
          processException(msg, ex);
        }
      }

      checkIfDone();
    }

    public Set getResults() {
      logger.debug("{} shutdownAll returning {}", this,
          results/* , new Exception("stack trace") */);
      return results;
    }
  }
}
