/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.internal.admin.remote;

// import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*; // import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.cache.*;

import java.io.*; // import java.net.*;
// import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * A message that is sent in response to a {@link DurableClientInfoRequest}.
 * 
 * @since 5.6
 */
public class DurableClientInfoResponse extends AdminResponse
{
  // instance variables
  /**
   * The result for the query made the request (hasDurableClient or
   * isPrimaryForDurableClient).
   */
  private boolean returnVal = false;

  /**
   * Returns a <code>DurableClientInfoResponse</code> that will be returned to
   * the specified recipient.
   */
  public static DurableClientInfoResponse create(DistributionManager dm,
      InternalDistributedMember recipient, DurableClientInfoRequest request)
  {
    DurableClientInfoResponse m = new DurableClientInfoResponse();
    m.setRecipient(recipient);
    try {
      GemFireCacheImpl c = (GemFireCacheImpl)CacheFactory.getInstanceCloseOk(dm
          .getSystem());
      if (c.getCacheServers().size() > 0) {

        CacheServerImpl server = (CacheServerImpl)c.getCacheServers()
            .iterator().next();
        switch (request.action) {
        case DurableClientInfoRequest.HAS_DURABLE_CLIENT_REQUEST: {
          m.returnVal = server.getAcceptor().getCacheClientNotifier()
              .hasDurableClient(request.durableId);
          break;
        }
        case DurableClientInfoRequest.IS_PRIMARY_FOR_DURABLE_CLIENT_REQUEST: {
          m.returnVal = server.getAcceptor().getCacheClientNotifier()
              .hasPrimaryForDurableClient(request.durableId);
          break;
        }
        }
      }
    }
    catch (CacheClosedException ex) {
      // do nothing
    }
    return m;
  }

  public boolean getResultBoolean()
  {
    return this.returnVal;
  }

  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    out.writeBoolean(this.returnVal);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    this.returnVal = in.readBoolean();
  }

  public String toString()
  {
    return "DurableClientInfoResponse from " + this.getSender();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.DataSerializableFixedID#getDSFID()
   */
  public int getDSFID() {
    return DURABLE_CLIENT_INFO_RESPONSE;
  }
}
