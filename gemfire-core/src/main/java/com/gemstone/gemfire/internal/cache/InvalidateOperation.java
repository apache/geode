/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.distributed.internal.ConflationKey;
import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.versions.ConcurrentCacheModificationException;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Handles distribution messaging for invalidating an entry in a region.
 * 
 * @author Eric Zoerner
 *  
 */
public class InvalidateOperation extends DistributedCacheOperation
{
  private static final Logger logger = LogService.getLogger();

  /** Creates a new instance of InvalidateOperation */
  public InvalidateOperation(EntryEventImpl event) {
    super(event);
  }

  @Override
  protected CacheOperationMessage createMessage()
  {
    if (this.event.hasClientOrigin()) {
      InvalidateWithContextMessage msgwithContxt = new InvalidateWithContextMessage();
      msgwithContxt.context = ((EntryEventImpl)this.event).getContext();
      return msgwithContxt;
    }
    else {
      return new InvalidateMessage();
    }
  }

  @Override
  protected void initMessage(CacheOperationMessage msg,
      DirectReplyProcessor processor)
  {
    super.initMessage(msg, processor);
    InvalidateMessage imsg = (InvalidateMessage)msg;
    EntryEventImpl eei = (EntryEventImpl)this.event;
    imsg.key = eei.getKey();
    imsg.eventId = eei.getEventId();
  }

  public static class InvalidateMessage extends CacheOperationMessage
  {
    protected Object key;
    protected EventID eventId = null;

    @Override
    protected boolean operateOnRegion(CacheEvent event, DistributionManager dm)
        throws EntryNotFoundException
    {
      EntryEventImpl ev = (EntryEventImpl)event;
      DistributedRegion rgn = (DistributedRegion)ev.region;

      try {
        if (!rgn.isCacheContentProxy()) {
          if (logger.isTraceEnabled()) {
            logger.trace("InvalidateMessage.operationOnRegion; key={}", ev.getKey());
          }

          // if this is a mirrored region and we're still initializing, or
          // concurrency conflict detection is enabled (requiring version #
          // retention) then force new entry creation
          boolean forceNewEntry = rgn.dataPolicy.withReplication()
          && (!rgn.isInitialized() || rgn.getConcurrencyChecksEnabled());
          boolean invokeCallbacks = rgn.isInitialized();
          rgn.basicInvalidate(ev, invokeCallbacks, forceNewEntry);
        }
        this.appliedOperation = true;
        return true;
      } catch (ConcurrentCacheModificationException e) {
        dispatchElidedEvent(rgn, ev);
        return true; // concurrent modification problems are not reported to senders
      }
    }

    @Override
    protected InternalCacheEvent createEvent(DistributedRegion rgn)
        throws EntryNotFoundException {
      if (rgn.keyRequiresRegionContext()) {
        ((KeyWithRegionContext)this.key).setRegionContext(rgn);
      }
      EntryEventImpl ev = new EntryEventImpl(
         rgn, getOperation(), this.key,
         null, this.callbackArg, true, getSender());
      ev.setEventId(this.eventId);
      setOldValueInEvent(ev);
      ev.setVersionTag(this.versionTag);
      if (this.filterRouting != null) {
        ev.setLocalFilterInfo(this.filterRouting
            .getFilterInfo(rgn.getMyId()));
      }
      ev.setInhibitAllNotifications(this.inhibitAllNotifications);
      return ev;
    }

    @Override
    protected void appendFields(StringBuilder buff) {
      super.appendFields(buff);
      buff.append("; key=");
      buff.append(this.key);
    }

    public int getDSFID() {
      return INVALIDATE_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      this.eventId = (EventID)DataSerializer.readObject(in);
      this.key = DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      DataSerializer.writeObject(this.eventId, out);
      DataSerializer.writeObject(this.key, out);
    }

    @Override
    public EventID getEventID() {
      return this.eventId;
    }

    @Override
    public List getOperations()
    {
      byte deserializationPolicy = DistributedCacheOperation.DESERIALIZATION_POLICY_NONE;
      QueuedOperation qOp = new QueuedOperation(getOperation(),
                                                this.key,
                                                null,
                                                null,
                                                deserializationPolicy,
                                                this.callbackArg);
      return Collections.singletonList(qOp);
    }

    @Override
    public ConflationKey getConflationKey()
    {
      if (!super.regionAllowsConflation || getProcessorId() != 0) {
        // if the publisher's region attributes do not support conflation
        // or if it is an ack region
        // then don't even bother with a conflation key
        return null;
      }
      else {
        // don't conflate invalidates
        return new ConflationKey(this.key, super.regionPath, false);
      }
    }
  }
  
  public static final class InvalidateWithContextMessage extends InvalidateMessage
  {
    transient ClientProxyMembershipID context;

    @Override
    protected InternalCacheEvent createEvent(DistributedRegion rgn)
      throws EntryNotFoundException  {
      EntryEventImpl event = (EntryEventImpl)super.createEvent(rgn);
      event.setContext(this.context);
      return event;
    }

    @Override
    protected void appendFields(StringBuilder buff)
    {
      super.appendFields(buff);
      buff.append("; membershipID=");
      buff.append(this.context == null ? "" : this.context.toString());
    }

    @Override
    public int getDSFID() {
      return INVALIDATE_WITH_CONTEXT_MESSAGE;
    }
    
    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      this.context = ClientProxyMembershipID.readCanonicalized(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      DataSerializer.writeObject(this.context, out);
    }

  }
}


