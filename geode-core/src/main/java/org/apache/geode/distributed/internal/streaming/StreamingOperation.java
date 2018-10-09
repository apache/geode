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

package org.apache.geode.distributed.internal.streaming;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.GemFireRethrowable;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.PRQueryTraceInfo;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegionQueryEvaluator;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.BlobHelper;

/**
 * StreamingOperation is an abstraction for sending messages to multiple (or single) recipient
 * requesting a potentially large amount of data and receiving the reply with data chunked into
 * several messages.
 */
public abstract class StreamingOperation {
  private static final Logger logger = LogService.getLogger();

  /**
   * This is the number of bytes that need to be allowed in addition to data chunk to prevent
   * overflowing the socket buffer size in one message. For now, this is just an estimate
   */
  public static final int MSG_OVERHEAD = 200; // seems to need to be greater than 100

  public final InternalDistributedSystem sys;

  /**
   * Creates a new instance of StreamingOperation
   */
  public StreamingOperation(InternalDistributedSystem sys) {
    this.sys = sys;
  }

  /**
   * Returns normally if succeeded to get data, otherwise throws an exception
   *
   * @throws InterruptedException TODO-javadocs
   */
  public void getDataFromAll(Set recipients)
      throws org.apache.geode.cache.TimeoutException, InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    if (recipients.isEmpty()) {
      return;
    }

    StreamingProcessor processor = new StreamingProcessor(this.sys, recipients);
    DistributionMessage m = createRequestMessage(recipients, processor);
    this.sys.getDistributionManager().putOutgoing(m);
    // while() loop removed for bug 36983 - you can't loop on waitForReplies()
    try {
      // should we allow this to timeout?
      processor.waitForRepliesUninterruptibly();
    } catch (InternalGemFireException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof org.apache.geode.cache.TimeoutException) {
        throw (org.apache.geode.cache.TimeoutException) cause;
      }
      throw ex;
    } catch (ReplyException e) {
      e.handleCause();
      // throws exception
    }
  }

  /**
   * Override in subclass to instantiate request message
   */
  protected abstract DistributionMessage createRequestMessage(Set recipients,
      ReplyProcessor21 processor);

  /**
   * Called from separate thread when reply is processed.
   *
   * @return false if should abort (region was destroyed or cache was closed)
   */
  public boolean processChunk(List objects, InternalDistributedMember sender, int sequenceNum,
      boolean lastInSequence) {
    return processData(objects, sender, sequenceNum, lastInSequence);
  }

  /**
   * Override in subclass to do something useful with the data.
   *
   * @param sequenceNum the sequence of this data (0-based), in case ordering matters
   * @param lastInSequence true if this is the last chunk in the sequence
   * @return false to abort
   */

  protected abstract boolean processData(List objects, InternalDistributedMember sender,
      int sequenceNum, boolean lastInSequence);

  public class StreamingProcessor extends ReplyProcessor21 {
    protected volatile boolean abort = false;
    private final Map statusMap = new HashMap();

    protected final AtomicInteger msgsBeingProcessed = new AtomicInteger();

    class Status {
      int msgsProcessed = 0;
      int numMsgs = 0;

      /** Return true if this is the very last reply msg to process for this member */
      protected synchronized boolean trackMessage(StreamingReplyMessage m) {
        this.msgsProcessed++;

        if (m.lastMsg) {
          this.numMsgs = m.msgNum + 1;
        }
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Streaming Message Tracking Status: Processor id: {}; Sender: {}; Messages Processed: {}; NumMsgs: {}",
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


    public StreamingProcessor(final InternalDistributedSystem system,
        InternalDistributedMember member) {
      super(system, member);
    }

    public StreamingProcessor(InternalDistributedSystem system, Set members) {
      super(system, members);
    }

    @Override
    public void process(DistributionMessage msg) {
      // ignore messages from members not in the wait list
      if (!waitingOnMember(msg.getSender())) {
        return;
      }
      this.msgsBeingProcessed.incrementAndGet();
      try {
        StreamingReplyMessage m = (StreamingReplyMessage) msg;
        boolean isLast = true; // is last message for this member?
        List objects = m.getObjects();
        if (objects != null) { // CONSTRAINT: objects should only be null if there's no data at all
          // Bug 37461: don't allow abort to be reset.
          boolean isAborted = this.abort; // volatile fetch
          if (!isAborted) {
            isAborted = !processChunk(objects, m.getSender(), m.msgNum, m.lastMsg);
            if (isAborted) {
              this.abort = true; // volatile store
            }
          }
          isLast = isAborted || trackMessage(m); // interpret msgNum
          // @todo ezoerner send an abort message to data provider if
          // !doContinue (region was destroyed or cache closed);
          // also provide ability to explicitly cancel
        } else {
          // if a null chunk was received (no data), then
          // we're done with that member
          isLast = true;
        }
        if (isLast) {
          super.process(msg, false); // removes from members and cause us to
                                     // ignore future messages received from that member
        }
      } finally {
        this.msgsBeingProcessed.decrementAndGet();
        checkIfDone(); // check to see if decrementing msgsBeingProcessed requires signalling to
                       // proceed
      }
    }

    /**
     * Contract of {@link ReplyProcessor21#stillWaiting()} is to never return true after returning
     * false.
     */
    private volatile boolean finishedWaiting = false;

    /**
     * Overridden to wait for messages being currently processed: This situation can come about if a
     * member departs while we are still processing data from that member
     */
    @Override
    protected boolean stillWaiting() {
      if (finishedWaiting) { // volatile fetch
        return false;
      }
      if (this.msgsBeingProcessed.get() > 0) {
        // to fix bug 37391 always wait for msgsBeingPRocessod to go to 0,
        // even if abort is true
        return true;
      }
      // volatile fetches
      finishedWaiting = finishedWaiting || this.abort || !super.stillWaiting();
      return !finishedWaiting;
    }


    @Override
    public String toString() {
      return "<" + this.getClass().getName() + " " + this.getProcessorId() + " waiting for "
          + numMembers() + " replies" + (exception == null ? "" : (" exception: " + exception))
          + " from " + membersToString() + "; waiting for " + this.msgsBeingProcessed.get()
          + " messages in the process of being processed" + ">";
    }

    protected boolean trackMessage(StreamingReplyMessage m) {
      Status status;
      synchronized (this) {
        status = (Status) this.statusMap.get(m.getSender());
        if (status == null) {
          status = new Status();
          this.statusMap.put(m.getSender(), status);
        }
      }
      return status.trackMessage(m);
    }

  }

  public abstract static class RequestStreamingMessage extends PooledDistributionMessage
      implements MessageWithReply {

    protected int processorId;

    @Override
    public int getProcessorId() {
      return this.processorId;
    }

    @Override
    protected void process(final ClusterDistributionManager dm) {
      Throwable thr = null;
      ReplyException rex = null;
      Object nextObject = null;
      Object failedObject = null;
      int socketBufferSize = dm.getSystem().getConfig().getSocketBufferSize();
      int chunkSize = socketBufferSize - MSG_OVERHEAD;
      HeapDataOutputStream outStream =
          new HeapDataOutputStream(chunkSize, getSender().getVersionObject());
      boolean sentFinalMessage = false;
      boolean receiverCacheClosed = false;
      int msgNum = 0;

      try {
        do {
          int numObjectsInChunk = 0;
          // boolean firstObject = true;

          // always write at least one object, allowing expansion
          // if we have an object already that didn't get added, then use
          // that object instead of getting another one
          if (failedObject == null) {
            nextObject = getNextReplyObject();
          } else {
            nextObject = failedObject;
            failedObject = null;
          }

          if (nextObject != Token.END_OF_STREAM) {
            numObjectsInChunk = 1;
            BlobHelper.serializeTo(nextObject, outStream);

            // for the next objects, disallow stream from allocating more storage
            do {
              outStream.disallowExpansion(CHUNK_FULL); // sets the mark where rollback occurs on
                                                       // CHUNK_FULL

              nextObject = getNextReplyObject();

              if (nextObject != Token.END_OF_STREAM) {
                try {
                  BlobHelper.serializeTo(nextObject, outStream);
                  numObjectsInChunk++;
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
            replyWithData(dm, outStream, numObjectsInChunk, msgNum++,
                nextObject == Token.END_OF_STREAM);
            if (nextObject == Token.END_OF_STREAM) {
              sentFinalMessage = true;
            }
          } catch (CancelException e) {
            receiverCacheClosed = true;
            break; // receiver no longer cares
          }
          outStream.reset(); // ready for reuse, assumes replyWithData
                             // does not queue the message but outStream has
                             // already been used
        } while (nextObject != Token.END_OF_STREAM);
        // } catch (CancelException e) {
        // // if cache is closed, we cannot send a reply (correct?)
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
      }

      if (thr != null) {
        rex = new ReplyException(thr);
        replyWithException(dm, rex);
      } else if (!sentFinalMessage && !receiverCacheClosed) {
        throw new InternalGemFireError(
            "this should not happen");
        // replyNoData(dm);
      }
    }

    /**
     * override in subclass to provide reply data. terminate by returning Token.END_OF_STREAM
     */
    protected abstract Object getNextReplyObject() throws InterruptedException;

    // private void replyNoData(DistributionManager dm) {
    // StreamingReplyMessage.send(getSender(), this.processorId, null, dm, null, 0, 0, true);
    // }

    protected void replyWithData(ClusterDistributionManager dm, HeapDataOutputStream outStream,
        int numObjects, int msgNum, boolean lastMsg) {
      StreamingReplyMessage.send(getSender(), this.processorId, null, dm, outStream, numObjects,
          msgNum, lastMsg);
    }

    protected void replyWithException(ClusterDistributionManager dm, ReplyException rex) {
      StreamingReplyMessage.send(getSender(), this.processorId, rex, dm, null, 0, 0, true);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append(getClass().getName());
      buff.append("'; sender=");
      buff.append(getSender());
      buff.append("; processorId=");
      buff.append(this.processorId);
      buff.append(")");
      return buff.toString();
    }
  }

  public static class StreamingReplyMessage extends ReplyMessage {

    /** the number of this message */
    protected int msgNum;

    /** whether this message is the last one in this series */
    protected boolean lastMsg;

    private transient HeapDataOutputStream chunkStream; // used only on sending side, null means
                                                        // abort
    private transient int numObjects; // used only on sending side
    private transient List objectList = null; // used only on receiving side

    private boolean pdxReadSerialized = false; // used to read PDX types in serialized form.
    private transient boolean isCanceled = false; // used only on receiving side and if
                                                  // messageProcessor is of type
                                                  // PartitionedRegionQueryEvaluator.StreamingQueryPartitionResponse


    /**
     * @param chunkStream the data to send back, if null then all the following parameters are
     *        ignored and any future replies from this member will be ignored, and the streaming of
     *        chunks is considered aborted by the receiver.
     *
     * @param msgNum message number in this series (0-based)
     * @param lastMsg if this is the last message in this series
     */
    public static void send(InternalDistributedMember recipient, int processorId,
        ReplyException exception, DistributionManager dm, HeapDataOutputStream chunkStream,
        int numObjects, int msgNum, boolean lastMsg) {
      send(recipient, processorId, exception, dm, chunkStream, numObjects, msgNum, lastMsg, false);
    }

    public static void send(InternalDistributedMember recipient, int processorId,
        ReplyException exception, DistributionManager dm, HeapDataOutputStream chunkStream,
        int numObjects, int msgNum, boolean lastMsg, boolean pdxReadSerialized) {
      StreamingReplyMessage replyMessage = new StreamingReplyMessage();
      replyMessage.processorId = processorId;

      if (exception != null) {
        replyMessage.setException(exception);
        logger.debug("Replying with exception: {}", replyMessage, exception);
      }

      replyMessage.chunkStream = chunkStream;
      replyMessage.numObjects = numObjects;
      replyMessage.setRecipient(recipient);
      replyMessage.msgNum = msgNum;
      replyMessage.lastMsg = lastMsg;
      replyMessage.pdxReadSerialized = pdxReadSerialized;
      dm.putOutgoing(replyMessage);
    }

    public int getMessageNumber() {
      return this.msgNum;
    }

    public boolean isLastMessage() {
      return this.lastMsg;
    }

    public boolean isCanceled() {
      return isCanceled;
    }

    /** Return the objects in this chunk as a List, used only on receiving side */
    public List getObjects() {
      return this.objectList;
    }

    @Override
    public int getDSFID() {
      return STREAMING_REPLY_MESSAGE;
    }


    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      int n;
      super.fromData(in);
      n = in.readInt();
      this.msgNum = in.readInt();
      this.lastMsg = in.readBoolean();
      this.pdxReadSerialized = in.readBoolean();
      Version senderVersion = InternalDataSerializer.getVersionForDataStream(in);
      boolean isSenderAbove_8_1 = senderVersion.compareTo(Version.GFE_81) > 0;
      InternalCache cache = null;
      Boolean initialPdxReadSerialized = false;
      try {
        cache =
            (InternalCache) GemFireCacheImpl.getForPdx("fromData invocation in StreamingOperation");
        initialPdxReadSerialized = cache.getPdxReadSerializedOverride();
      } catch (CacheClosedException e) {
        logger.debug("Cache is closed. PdxReadSerializedOverride set to false");
      }
      if (n == -1) {
        this.objectList = null;
      } else {
        this.numObjects = n; // for benefit of toString()
        this.objectList = new ArrayList(n);
        // Check if the PDX types needs to be kept in serialized form.
        // This will make readObject() to return PdxInstance form.
        if (this.pdxReadSerialized && cache != null) {
          cache.setPdxReadSerializedOverride(true);
        }
        try {
          ReplyProcessor21 messageProcessor = ReplyProcessor21.getProcessor(processorId);
          boolean isQueryMessageProcessor =
              messageProcessor instanceof PartitionedRegionQueryEvaluator.StreamingQueryPartitionResponse;
          ObjectType elementType = null;
          if (isQueryMessageProcessor) {
            elementType =
                ((PartitionedRegionQueryEvaluator.StreamingQueryPartitionResponse) messageProcessor)
                    .getResultType();
          }

          boolean lowMemoryDetected = false;
          for (int i = 0; i < n; i++) {
            // TestHook used in ResourceManagerWithQueryMonitorDUnitTest.
            // will simulate an critical memory event after a certain number of calls to
            // doTestHook(BEFORE_ADD_OR_UPDATE_MAPPING_OR_DESERIALIZING_NTH_STREAMINGOPERATION)
            if (DefaultQuery.testHook != null) {
              DefaultQuery.testHook.doTestHook(
                  DefaultQuery.TestHook.SPOTS.BEFORE_ADD_OR_UPDATE_MAPPING_OR_DESERIALIZING_NTH_STREAMINGOPERATION,
                  null);
            }
            if (isQueryMessageProcessor && QueryMonitor.isLowMemory()) {
              lowMemoryDetected = true;
              break;
            }
            Object o = DataSerializer.readObject(in);
            if (isQueryMessageProcessor && elementType != null && elementType.isStructType()) {
              boolean convertToStruct = isSenderAbove_8_1;
              if (convertToStruct && i == 0) {
                convertToStruct = !(o instanceof PRQueryTraceInfo);
              }
              if (convertToStruct) {
                o = new StructImpl((StructTypeImpl) elementType, (Object[]) o);
              }
            }
            this.objectList.add(o);
          }
          if (lowMemoryDetected) {
            isCanceled = true;
            // TestHook to help verify that objects have been rejected.
            if (DefaultQuery.testHook != null) {
              DefaultQuery.testHook.doTestHook(
                  DefaultQuery.TestHook.SPOTS.LOW_MEMORY_WHEN_DESERIALIZING_STREAMINGOPERATION,
                  null);
            }
          }
        } finally {
          if (this.pdxReadSerialized && cache != null) {
            cache.setPdxReadSerializedOverride(initialPdxReadSerialized);
          }
        }
      }
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      if (this.chunkStream == null) {
        out.writeInt(-1);
      } else {
        out.writeInt(this.numObjects);
      }
      out.writeInt(this.msgNum);
      out.writeBoolean(this.lastMsg);
      out.writeBoolean(this.pdxReadSerialized);
      if (this.chunkStream != null && this.numObjects > 0) {
        this.chunkStream.sendTo(out);
      }
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append(getClass().getName());
      buff.append("(processorId=");
      buff.append(this.processorId);
      buff.append(" from ");
      buff.append(this.getSender());
      ReplyException ex = this.getException();
      if (ex != null) {
        buff.append(" with exception ");
        buff.append(ex);
      }
      buff.append(";numObjects=");
      buff.append(this.numObjects);
      buff.append(";msgNum ");
      buff.append(this.msgNum);
      buff.append(";lastMsg=");
      buff.append(this.lastMsg);
      if (this.objectList != null) {
        buff.append(";objectList(size=");
        buff.append(this.objectList.size());
        buff.append(")");
      } else {
        buff.append(";chunkStream=");
        buff.append(this.chunkStream);
      }
      buff.append(")");
      return buff.toString();
    }

  }

  public static final GemFireRethrowable CHUNK_FULL = new GemFireRethrowable();
}
