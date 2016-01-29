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
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.GemFireRethrowable;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.streaming.StreamingOperation;
import com.gemstone.gemfire.internal.CopyOnWriteHashSet;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PrimaryBucketException;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.util.BlobHelper;


/**
* StreamingPartitionOperation is an abstraction for sending messages to multiple (or single)
 * recipients requesting a potentially large amount of data from a Partitioned Region datastore
 * and receiving the reply with data chunked into several messages.
 *
 * @author Eric Zoerner
 */

public abstract class StreamingPartitionOperation extends StreamingOperation {
  private static final Logger logger = LogService.getLogger();
  
  protected final int regionId;
  
  /** Creates a new instance of StreamingPartitionOperation */
  public StreamingPartitionOperation(InternalDistributedSystem sys, int regionId) {
    super(sys);
    this.regionId = regionId;
  }
  
  @Override
  public void getDataFromAll(Set recipients) {
    throw new UnsupportedOperationException(LocalizedStrings.StreamingPartitionOperation_CALL_GETPARTITIONEDDATAFROM_INSTEAD.toLocalizedString());
  }
  
  /**
  * Returns normally if succeeded to get data, otherwise throws an exception
   */
  public Set<InternalDistributedMember> getPartitionedDataFrom(Set recipients)
  throws com.gemstone.gemfire.cache.TimeoutException, InterruptedException, QueryException, ForceReattemptException {
    if (Thread.interrupted()) throw new InterruptedException();
    if (recipients.isEmpty())
      return Collections.emptySet();
    
    StreamingPartitionResponse processor = new StreamingPartitionResponse(this.sys, recipients);
    DistributionMessage m = createRequestMessage(recipients, processor);
    this.sys.getDistributionManager().putOutgoing(m);
    // should we allow this to timeout?
    Set<InternalDistributedMember> failedMembers = processor.waitForCacheOrQueryException();
    return failedMembers;
  }
  
  /** Override in subclass to instantiate request message */
  @Override  
  protected abstract DistributionMessage createRequestMessage(Set recipients, ReplyProcessor21 processor);

  protected class StreamingPartitionResponse extends ReplyProcessor21 {
    protected volatile boolean abort = false;
    protected final Map statusMap = new HashMap();
    
    protected final AtomicInteger msgsBeingProcessed = new AtomicInteger();
    private volatile String memberDepartedMessage = null;
    private final Set<InternalDistributedMember> failedMembers = new CopyOnWriteHashSet<InternalDistributedMember>();
    
    class Status {
      int msgsProcessed = 0;
      int numMsgs = 0;
      
      /** Return true if this is the very last reply msg to process for this member */
      protected synchronized boolean trackMessage(StreamingReplyMessage m) {
        this.msgsProcessed++;
        
        if (m.isLastMessage()) {
          this.numMsgs = m.getMessageNumber() + 1;
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Streaming Message Tracking Status: Processor id: {}; Sender: {}; Messages Processed: {}; NumMsgs: {}",
              getProcessorId(), m.getSender(), this.msgsProcessed, this.numMsgs);
        }
        
        // this.numMsgs starts out as zero and gets initialized
        // only when we get a lastMsg true.
        // Since we increment msgsProcessed, the following condition
        // cannot be true until sometime after we've received the
        // lastMsg, and signals that all messages have been processed
        return this.msgsProcessed == this.numMsgs;
      }
  
    }
    
    public StreamingPartitionResponse(InternalDistributedSystem system,
                                      Set members) {
      super(system, members);
    }
    
    protected boolean stopBecauseOfExceptions() {
      return false;
    }

    @Override
    public void process(DistributionMessage msg) {
      // ignore messages from members not in the wait list
      if (!waitingOnMember(msg.getSender())) {
        return;
      }
      
      this.msgsBeingProcessed.incrementAndGet();
      try {
        StreamingReplyMessage m = (StreamingReplyMessage)msg;
        boolean isLast = true; // is last message for this member?
        List objects = m.getObjects();
        if (objects != null) {  // CONSTRAINT: objects should only be null if there's no data at all
          // Bug 37461: don't allow abort flag to be cleared
          boolean isAborted = this.abort; // volatile fetch
          if (!isAborted) {
            isAborted = !processChunk(objects, m.getSender(),
                m.getMessageNumber(), m.isLastMessage());
            if (isAborted) {
              this.abort = true; // volatile store
            }
          }
          isLast = isAborted || trackMessage(m); // interpret msgNum
          // @todo ezoerner send an abort message to data provider if
          // !doContinue (region was destroyed or cache closed);
          // also provide ability to explicitly cancel
        }
        else {
          // if a null chunk was received (no data), then
          // we're done with that member
          isLast = true;
        }
       if (isLast) { //commented by Suranjan watch this out
          super.process(msg, false); // removes from members and cause us to
                                     // ignore future messages received from that member
        }
      }
      finally {
        this.msgsBeingProcessed.decrementAndGet();
        checkIfDone(); // check to see if decrementing msgsBeingProcessed requires signalling to proceed
      }          
    }  

    protected synchronized void processException(DistributionMessage msg,
        ReplyException ex) {
      Throwable t = ex.getCause();
      if (t instanceof ForceReattemptException ||
          t instanceof CacheClosedException) {
        if (logger.isDebugEnabled()) {
          logger.debug("StreamingPartitionResponse received exception {} for member {} query retry required.", t, msg.getSender());
        }
        this.failedMembers.add(msg.getSender());
      } else {
        super.processException(msg, ex);
      }
    }

    @Override
    public void memberDeparted(InternalDistributedMember id, boolean crashed) {
      if (id!=null && waitingOnMember(id)) {
        this.failedMembers.add(id);
        this.memberDepartedMessage = LocalizedStrings.StreamingPartitionOperation_GOT_MEMBERDEPARTED_EVENT_FOR_0_CRASHED_1.toLocalizedString(new Object[] {id, Boolean.valueOf(crashed)});
      }
      super.memberDeparted(id, crashed);
    }

    /**
     * Waits for the response from the {@link PartitionMessage}'s recipient
     * @throws CacheException  if the recipient threw a cache exception during message processing
     * @throws QueryException if the recipient threw a query exception
     * @throws RegionDestroyedException if the peer has closed its copy of the region
     * @return The set of members that failed.
     */
    public Set<InternalDistributedMember> waitForCacheOrQueryException()
        throws CacheException, QueryException {
      try {
        waitForRepliesUninterruptibly();
        return this.failedMembers;
      }
      catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof CacheException) {
          throw (CacheException)t;
        } else if(t instanceof RegionDestroyedException) {
          throw (RegionDestroyedException)t;
        } else if (t instanceof QueryException) {
          throw (QueryException)t;
        } else if (t instanceof PrimaryBucketException) {
          throw new PrimaryBucketException("Peer failed primary test", t);
        }
        e.handleAsUnexpected();
        //This won't be reached, because of the above,
        //but it makes the compiler happy.
        throw e;
      }
    }

    
    /**
     * Contract of {@link ReplyProcessor21#stillWaiting()} is that it never
     * returns true after returning false.
     */
    private volatile boolean finishedWaiting = false;
    
    /** Overridden to wait for messages being currently processed:
      *  This situation can come about if a member departs while we
      *  are still processing data from that member
      */
    @Override  
    protected boolean stillWaiting() {
      if (finishedWaiting) { // volatile fetch
        return false;
      }
      if (this.msgsBeingProcessed.get() > 0 && this.numMembers() > 0) {
        // to fix bug 37391 always wait for msgsBeingProcessod to go to 0;
        // even if abort is true
        return true;
      }
      // volatile fetches and volatile store:
      finishedWaiting =  finishedWaiting ||
          this.abort || !super.stillWaiting();
      return !finishedWaiting;
    }
    
    @Override  
    public String toString() {
      // bug 37213: make sure toString is bullet-proof from escaped constructor
      StringBuffer sb = new StringBuffer();
      sb.append("<");
      sb.append(this.getClass().getName());
      sb.append(" ");
      sb.append(Integer.toString(this.getProcessorId()));
      if (this.members == null) {
        sb.append(" (null memebrs)");
      }
      else {
        sb.append(" waiting for ");
        sb.append(Integer.toString(numMembers()));
        sb.append(" replies"); 
        sb.append((exception == null ? "" : (" exception: " + exception)));
        sb.append(" from ");
        sb.append(membersToString());
      }
      sb.append("; waiting for ");
      sb.append(Integer.toString(this.msgsBeingProcessed.get()));
      sb.append(" messages in the process of being processed" + ">");
      return sb.toString();
    }    
    
    protected boolean trackMessage(StreamingReplyMessage m) {
      Status status;
      synchronized (this) {
        status = (Status)this.statusMap.get(m.getSender());
        if (status == null) {
          status = new Status();
          this.statusMap.put(m.getSender(), status);
        }
      }
      return status.trackMessage(m);
    }

    /**
     * @param notReceivedMembers
     */
    public void removeFailedSenders(Set notReceivedMembers) {
      for (Iterator i=notReceivedMembers.iterator(); i.hasNext(); ) {
        removeMember((InternalDistributedMember)i.next(), true);
      }
    }
  }
  
  public static abstract class StreamingPartitionMessage extends PartitionMessage {    
    // the following transient fields are used for passing extra data to the sendReply method
    transient HeapDataOutputStream outStream = null;
    transient int replyMsgNum = 0;
    transient boolean replyLastMsg = true;
    transient int numObjectsInChunk = 0;
    
    public StreamingPartitionMessage() {
      super();
    }
    
    public StreamingPartitionMessage(Set recipients, int regionId, ReplyProcessor21 processor) {
      super(recipients, regionId, processor);
    }

    public StreamingPartitionMessage(InternalDistributedMember recipient, int regionId,
        ReplyProcessor21 processor) {
      super(recipient, regionId, processor);
    }
    
    /** send a reply message.  This is in a method so that subclasses can override the reply message type
      *  @see PutMessage#sendReply
      */
    @Override  
    protected void sendReply(InternalDistributedMember member, int procId, DM dm, ReplyException ex, PartitionedRegion pr, long startTime) {
      // if there was an exception, then throw out any data
      if (ex != null) {
        this.outStream = null;
        this.replyMsgNum = 0;
        this.replyLastMsg = true;
      }
      if (this.replyLastMsg) {
        if (pr != null && startTime > 0) {
          pr.getPrStats().endPartitionMessagesProcessing(startTime);
        }
      }
      StreamingReplyMessage.send(member, procId, ex, dm, this.outStream,
                                 this.numObjectsInChunk, this.replyMsgNum,
                                 this.replyLastMsg);
    }
    
    /**
      * An operation upon the messages partitioned region
     * 
     * @param dm
     *          the manager that received the message
     * @param pr
     *          the partitioned region that should be modified
     * @return true if a reply message should be sent
     * @throws CacheException if an error is generated in the remote cache
     * @throws ForceReattemptException if the peer is no longer available
     */
    @Override  
    protected boolean operateOnPartitionedRegion(DistributionManager dm,
                                                 PartitionedRegion pr, long startTime)
    throws CacheException, QueryException, ForceReattemptException, InterruptedException {
      final boolean isTraceEnabled = logger.isTraceEnabled();
      
      if (Thread.interrupted()) throw new InterruptedException();
      Object nextObject;
      Object failedObject = null;
      int socketBufferSize = dm.getSystem().getConfig().getSocketBufferSize();
      int chunkSize = socketBufferSize - StreamingOperation.MSG_OVERHEAD;
      boolean sentFinalMessage = false;
      boolean receiverCacheClosed = false;
      
      this.outStream = new HeapDataOutputStream(chunkSize, getSender().getVersionObject());
      
      try {        
        do {
//          boolean firstObject = true;
          
          // always write at least one object, allowing expansion
          // if we have an object already that didn't get added, then use
          // that object instead of getting another one
          if (failedObject == null) {
            nextObject = getNextReplyObject(pr);
            this.replyLastMsg = nextObject == Token.END_OF_STREAM;
          } else {
            nextObject = failedObject;
            failedObject = null;
          }

          if (!this.replyLastMsg) {
            numObjectsInChunk = 1;
            if (isTraceEnabled) {
              logger.trace("Writing this object to StreamingPartitionMessage outStream: '{}'", nextObject);
            } 
            BlobHelper.serializeTo(nextObject, outStream);
            
            // for the next objects, disallow stream from allocating more storage
            do {
              outStream.disallowExpansion(CHUNK_FULL); // sets the mark where rollback occurs on CHUNK_FULL
              
              nextObject = getNextReplyObject(pr);
              this.replyLastMsg = nextObject == Token.END_OF_STREAM;
              
              if (!this.replyLastMsg) {
                try {
                  
                  if (isTraceEnabled) {
                    logger.trace("Writing this object to StreamingPartitionMessage outStream: '{}'", nextObject);
                  } 
                  BlobHelper.serializeTo(nextObject, outStream);
                  this.numObjectsInChunk++;
                } catch (GemFireRethrowable e) {
                  // can only be thrown when expansion is disallowed
                  // and buffer is automatically reset to point where it was disallowed
                  failedObject = nextObject;
                  break;
                } 
              }
            } while (nextObject != Token.END_OF_STREAM);
          }
          
          try {
            sendReply(getSender(), this.processorId, dm, null, pr, startTime);
            this.replyMsgNum++;
            if (this.replyLastMsg) {
              sentFinalMessage = true;
            }
          } 
          catch (CancelException e) {
            receiverCacheClosed = true;
            break;
          }
          this.outStream.reset(); // ready for reuse, assumes sendReply
                                   // does not queue the message but outStream has
                                   // already been used          
          this.numObjectsInChunk = 0;
        } while (!this.replyLastMsg);
      } catch (IOException ioe) {
        // not expected to ever happen
        throw new InternalGemFireException(ioe);
      }
      
      
      if (!sentFinalMessage && !receiverCacheClosed) {    
        throw new InternalGemFireError(LocalizedStrings.StreamingPartitionOperation_UNEXPECTED_CONDITION.toLocalizedString());
      }
      
      //otherwise, we're already done, so don't send another reply
      return false;      
    } 
    
    /** override in subclass to provide reply data.
      *  terminate by returning END_OF_STREAM token object
      */
    protected abstract Object getNextReplyObject(PartitionedRegion pr)
    throws CacheException, ForceReattemptException, InterruptedException;
    
    protected Object getNextReplyObject() {
      throw new UnsupportedOperationException(LocalizedStrings.StreamingPartitionOperation_USE_GETNEXTREPLYOBJECTPARTITIONEDREGION_INSTEAD.toLocalizedString());
    }
  }
}
