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
 * @since GemFire 5.6
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
