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
 * @since GemFire 3.5
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
