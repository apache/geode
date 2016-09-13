/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.tcp.ConnectionTable;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * An instruction to all members with cache that their PR should gracefully
 * close and disconnect DS
 *
 */
public class ShutdownAllRequest extends AdminRequest {
  
  private static final Logger logger = LogService.getLogger();

  static final long SLEEP_TIME_BEFORE_DISCONNECT_DS = Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "sleep-before-disconnect-ds", 1000).longValue();

  public ShutdownAllRequest() {
  }

  /**
   * Sends a shutdownAll request to all other members and performs local
   * shutdownAll processing in the waitingThreadPool.
   */
  public static Set send(final DM dm, long timeout) {
    
    boolean hadCache = hasCache();
    boolean interrupted = false;
    DistributionManager dism = (dm instanceof DistributionManager) ? (DistributionManager)dm : null;
    InternalDistributedMember myId = dm.getDistributionManagerId();
    
    Set recipients = dm.getOtherNormalDistributionManagerIds();
    
    recipients.remove(myId);
    
    // now do shutdownall
    // recipients = dm.getOtherNormalDistributionManagerIds();
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
          response = AdminFailureResponse.create(dism, myId, ex); 
        }
        response.setSender(myId);
        replyProcessor.process(response);
      }
    }
    
    try {
      if(!replyProcessor.waitForReplies(timeout)) {
        return null;
      }
    } catch (ReplyException e) {
      if(!(e.getCause() instanceof CancelException)) {
        e.handleAsUnexpected();
      }
    } catch (CancelException e) {
      // expected
    } catch (InterruptedException e) {
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
      Thread.sleep(3*SLEEP_TIME_BEFORE_DISCONNECT_DS);
    } catch (InterruptedException e) {
    }
    return replyProcessor.getResults();
  }
  
  @Override
  public boolean sendViaUDP() {
    return true;
  }
  
  @Override
  protected void process(DistributionManager dm) {
    boolean isToShutdown = hasCache();
    super.process(dm);
    
    if (isToShutdown) {
      // Do the disconnect in an async thread. The thread we are running
      // in is one in the dm threadPool so we do not want to call disconnect
      // from this thread because it prevents dm from cleaning up all its threads
      // and causes a 20 second delay.
      final InternalDistributedSystem ids = dm.getSystem();
      if (ids.isConnected()) {
        Thread t = new Thread(new Runnable() {
          public void run() {
            try {
              Thread.sleep(SLEEP_TIME_BEFORE_DISCONNECT_DS);
            } catch (InterruptedException e) {
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

  private static boolean hasCache() {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if(cache != null && !cache.isClosed()) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    boolean isToShutdown = hasCache();
    boolean isSuccess = false;
    if (isToShutdown) {
      try {
        GemFireCacheImpl.getInstance().shutDownAll();
        isSuccess = true;
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error.  We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above).  However, there is
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

  public int getDSFID() {
    return SHUTDOWN_ALL_REQUEST;
  }
  
  @Override
  public void fromData(DataInput in) throws IOException,ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override  
  public String toString() {
    return "ShutdownAllRequest sent to " + Arrays.toString(this.getRecipients()) +
      " from " + this.getSender();
  }

  private static class ShutDownAllReplyProcessor extends AdminMultipleReplyProcessor {
    Set results = Collections.synchronizedSet(new TreeSet());

    public ShutDownAllReplyProcessor(DM dm, Collection initMembers) {
      super(dm, initMembers);
    }

    @Override
    protected boolean stopBecauseOfExceptions() {
      return false;
    }

    /* 
     * If response arrives, we will save into results and keep wait for member's 
     * departure. If the member is departed before sent response, no wait
     * for its response
     * @see com.gemstone.gemfire.distributed.internal.ReplyProcessor21#process(com.gemstone.gemfire.distributed.internal.DistributionMessage)
     */
    @Override
    public void process(DistributionMessage msg) {
      if (logger.isDebugEnabled()) {
        logger.debug("shutdownAll reply processor is processing {}", msg);
      }
      if(msg instanceof ShutdownAllResponse) {
        if (((ShutdownAllResponse)msg).isToShutDown()) {
          logger.debug("{} adding {} to result set {}", this, msg.getSender(), results);
          results.add(msg.getSender());
        }
        else {
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
        ReplyException ex = ((ReplyMessage)msg).getException();
        if (ex != null) {
          processException(msg, ex);
        }
      }

      checkIfDone();
    }

    public Set getResults() {
      logger.debug("{} shutdownAll returning {}", this, results, new Exception("stack trace"));
      return results;
    }
  }
}
