/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

//import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.cache.*;
import java.io.*;
//import java.net.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * A message that is sent in response to a {@link CacheInfoRequest}.
 * @since 3.5
 */
public final class CacheInfoResponse extends AdminResponse {
  // instance variables
  private RemoteCacheInfo info;
  
  
  /**
   * Returns a <code>CacheInfoResponse</code> that will be returned to the
   * specified recipient.
   */
  public static CacheInfoResponse create(DistributionManager dm, InternalDistributedMember recipient) {
    CacheInfoResponse m = new CacheInfoResponse();
    m.setRecipient(recipient);
    try {
      GemFireCacheImpl c = (GemFireCacheImpl)CacheFactory.getInstanceCloseOk(dm.getSystem());
      m.info = new RemoteCacheInfo(c);
    } 
    catch (CancelException ex) {
      m.info = null;
    }
    return m;
  }

  public RemoteCacheInfo getCacheInfo() {
    return this.info;
  }
  
  public int getDSFID() {
    return CACHE_INFO_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.info, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.info = (RemoteCacheInfo)DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "CacheInfoResponse from " + this.getSender() + " info=" + this.info;
  }
}
