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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.QueryExecutionLowMemoryException;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.IndexTrackingQueryObserver;
import com.gemstone.gemfire.cache.query.internal.PRQueryTraceInfo;
import com.gemstone.gemfire.cache.query.internal.QueryMonitor;
import com.gemstone.gemfire.cache.query.internal.QueryObserver;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.streaming.StreamingOperation.StreamingReplyMessage;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PRQueryProcessor;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.cache.query.Struct;

public final class QueryMessage extends StreamingPartitionOperation.StreamingPartitionMessage
  {
  private static final Logger logger = LogService.getLogger();
  
  private volatile String queryString;
  private volatile boolean cqQuery;
  private volatile Object[] parameters;
  private volatile List buckets;
  private volatile boolean isPdxSerialized;
  private volatile boolean traceOn;

//  private transient PRQueryResultCollector resultCollector = new PRQueryResultCollector();
  private transient List<Collection> resultCollector = new ArrayList<Collection>();
  private transient int tokenCount = 0; // counts how many end of stream tokens received
  private transient Iterator currentResultIterator;
  private transient Iterator<Collection> currentSelectResultIterator;
  private transient boolean isTraceInfoIteration = false;
  private transient boolean isStructType = false;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public QueryMessage() {}

  public QueryMessage(InternalDistributedMember recipient,  int regionId, ReplyProcessor21 processor,
      DefaultQuery query, Object[] parameters, final List buckets) {
    super(recipient, regionId, processor);
    this.queryString = query.getQueryString();
    this.buckets = buckets;
    this.parameters = parameters;
    this.cqQuery = query.isCqQuery();
    this.traceOn = query.isTraced() || DefaultQuery.QUERY_VERBOSE;
  }


  /**  Provide results to send back to requestor.
    *  terminate by returning END_OF_STREAM token object
    */
  @Override
  protected Object getNextReplyObject(PartitionedRegion pr)
  throws CacheException, ForceReattemptException, InterruptedException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    if (QueryMonitor.isLowMemory()) {
      String reason = LocalizedStrings.QueryMonitor_LOW_MEMORY_CANCELED_QUERY.toLocalizedString(QueryMonitor.getMemoryUsedDuringLowMemory());
      throw new QueryExecutionLowMemoryException(reason);
    }
    if (Thread.interrupted()) throw new InterruptedException();
    
    while ((this.currentResultIterator == null || !this.currentResultIterator.hasNext())) {
      if (this.currentSelectResultIterator.hasNext()) {
        if(this.isTraceInfoIteration && this.currentResultIterator != null) {
          this.isTraceInfoIteration = false;
        }
        Collection results = this.currentSelectResultIterator.next();
        if (isDebugEnabled) {
          logger.debug("Query result size: {}", results.size());
        }
        this.currentResultIterator = results.iterator();
      } else {
        //Assert.assertTrue(this.resultCollector.isEmpty());
        return Token.END_OF_STREAM;
      }
    }
    Object data = this.currentResultIterator.next();
    boolean isPostGFE_8_1 = this.getSender().getVersionObject().compareTo(Version.GFE_81) > 0 ;
    //Asif: There is a bug in older versions of GFE such that the query node expects the structs to have
    // type as ObjectTypes only & not specific types. So the new version needs to send the inaccurate 
    //struct type for backward compatibility.
    if(this.isStructType && !this.isTraceInfoIteration && isPostGFE_8_1) {
      return ((Struct)data).getFieldValues(); 
    }else if(this.isStructType && !this.isTraceInfoIteration) {
      Struct s = (Struct)data;
      ObjectType[] fieldTypes = s.getStructType().getFieldTypes();
      for(int i = 0; i < fieldTypes.length; ++i) {
        fieldTypes[i] = new ObjectTypeImpl(Object.class);
      }
      return data;
    }else {
      return data;
    }
  }


  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm, PartitionedRegion r, long startTime)
  throws CacheException, QueryException, ForceReattemptException, InterruptedException {
    //calculate trace start time if trace is on
    //this is because the start time is only set if enableClock stats is on
    //in this case we still want to see trace time even if clock is not enabled
    long traceStartTime = 0;
    if (this.traceOn) {
      traceStartTime = NanoTimer.getTime();
    }
    PRQueryTraceInfo queryTraceInfo = null; 
    List queryTraceList = null;
    if (Thread.interrupted()) throw new InterruptedException();
    if (logger.isTraceEnabled(LogMarker.DM)) {
      logger.trace(LogMarker.DM, "QueryMessage operateOnPartitionedRegion: {} buckets {}", r.getFullPath(), buckets);
    }

    r.waitOnInitialization();

    //PartitionedRegionDataStore ds = r.getDataStore();

    //if (ds != null) {
    if (QueryMonitor.isLowMemory()) {
      String reason = LocalizedStrings.QueryMonitor_LOW_MEMORY_CANCELED_QUERY.toLocalizedString(QueryMonitor.getMemoryUsedDuringLowMemory());
      //throw query exception to piggyback on existing error handling as qp.executeQuery also throws the same error for low memory
      throw new QueryExecutionLowMemoryException(reason);
    }
    
    DefaultQuery query = new DefaultQuery(this.queryString, r.getCache(), false);
    // Remote query, use the PDX types in serialized form.
    DefaultQuery.setPdxReadSerialized(r.getCache(), true);
    // In case of "select *" queries we can keep the results in serialized
    // form and send
    query.setRemoteQuery(true);
    QueryObserver indexObserver = query.startTrace();
    boolean isQueryTraced = false;
    try {
      query.setIsCqQuery(this.cqQuery);
      // ds.queryLocalNode(query, this.parameters, this.buckets,
      // this.resultCollector);
      PRQueryProcessor qp = new PRQueryProcessor(r, query, parameters, buckets);
      if (logger.isDebugEnabled()) {
        logger.debug("Started executing query from remote node: {}", query.getQueryString());
      }
      isQueryTraced = query.isTraced() && this.sender.getVersionObject().compareTo(Version.GFE_81) >= 0;
      // Adds a query trace info object to the results list for remote queries
      if (isQueryTraced) {
        this.isTraceInfoIteration = true;
        if (DefaultQuery.testHook != null) {
          DefaultQuery.testHook.doTestHook("Create PR Query Trace Info for Remote Query");
        }
        queryTraceInfo = new PRQueryTraceInfo();
        queryTraceList = Collections.singletonList(queryTraceInfo);
        
      }

      this.isStructType = qp.executeQuery(this.resultCollector);
      //Add the trace info list object after the NWayMergeResults is created so as to 
      //exclude it from the sorted collection of NWayMergeResults
      if(isQueryTraced) {
        this.resultCollector.add(0,queryTraceList);
      }
      this.currentSelectResultIterator = this.resultCollector.iterator();

      // If trace is enabled, we will generate a trace object to send back
      // The time info will be slightly different than the one logged on this
      // node
      // due to generating the trace object information here rather than the
      // finally
      // block.
      if (isQueryTraced) {
        if (DefaultQuery.testHook != null) {
          DefaultQuery.testHook.doTestHook("Populating Trace Info for Remote Query");
        }
        // calculate the number of rows being sent
        int traceSize = 0;
        traceSize = queryTraceInfo.calculateNumberOfResults(resultCollector);
        traceSize -= 1; // subtract the query trace info object
        queryTraceInfo.setTimeInMillis((NanoTimer.getTime() - traceStartTime) / 1.0e6f);
        queryTraceInfo.setNumResults(traceSize);
        // created the indexes used string
        if (indexObserver instanceof IndexTrackingQueryObserver) {
          Map indexesUsed = ((IndexTrackingQueryObserver) indexObserver).getUsedIndexes();
          StringBuffer buf = new StringBuffer();
          buf.append(" indexesUsed(").append(indexesUsed.size()).append(")");
          if (indexesUsed.size() > 0) {
            buf.append(":");
            for (Iterator itr = indexesUsed.entrySet().iterator(); itr.hasNext();) {
              Map.Entry entry = (Map.Entry) itr.next();
              buf.append(entry.getKey().toString() + entry.getValue());
              if (itr.hasNext()) {
                buf.append(",");
              }
            }
          }
          queryTraceInfo.setIndexesUsed(buf.toString());
        }
      }

      // resultSize = this.resultCollector.size() - this.buckets.size(); //Minus
      // END_OF_BUCKET elements.
      if (QueryMonitor.isLowMemory()) {
        String reason = LocalizedStrings.QueryMonitor_LOW_MEMORY_CANCELED_QUERY
            .toLocalizedString(QueryMonitor.getMemoryUsedDuringLowMemory());
        throw new QueryExecutionLowMemoryException(reason);
      }
      super.operateOnPartitionedRegion(dm, r, startTime);
    } finally {
      // remove trace info so that it is not included in the num results when
      // logged
      if (isQueryTraced) {
        resultCollector.remove(queryTraceList);
      }
      DefaultQuery.setPdxReadSerialized(r.getCache(), false);
      query.setRemoteQuery(false);
      query.endTrace(indexObserver, traceStartTime, this.resultCollector);
    }
    //}
    //else {
    //  l.warning(LocalizedStrings.QueryMessage_QUERYMESSAGE_DATA_STORE_NOT_CONFIGURED_FOR_THIS_MEMBER);
    //}

    // Unless there was an exception thrown, this message handles sending the response
    return false;
  }

  @Override
  protected void appendFields(StringBuffer buff)
  {
    super.appendFields(buff);
    buff.append("; query=").append(this.queryString)
    .append("; bucketids=").append(this.buckets);
  }

  public int getDSFID() {
    return PR_QUERY_MESSAGE;
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
        this.replyLastMsg, this.isPdxSerialized);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.queryString = DataSerializer.readString(in);
    this.buckets = DataSerializer.readArrayList(in);
    this.parameters = DataSerializer.readObjectArray(in);
    this.cqQuery = DataSerializer.readBoolean(in);
    this.isPdxSerialized = DataSerializer.readBoolean(in);
    this.traceOn = DataSerializer.readBoolean(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    DataSerializer.writeString(this.queryString, out);
    DataSerializer.writeArrayList((ArrayList)this.buckets, out);
    DataSerializer.writeObjectArray(this.parameters, out);
    DataSerializer.writeBoolean(this.cqQuery, out);
    DataSerializer.writeBoolean(true, out);
    DataSerializer.writeBoolean(this.traceOn, out);
  }
  
}
