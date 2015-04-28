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
import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.cache.*;
import java.io.*;
//import java.net.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * A message that is sent in response to a {@link CacheConfigRequest}.
 * @since 3.5
 */
public final class CacheConfigResponse extends AdminResponse {
  // instance variables
  private RemoteCacheInfo info;
  
  /** An exception thrown while configuring the cache
   *
   * @since 4.0 */
  private Exception exception;
  
  /**
   * Returns a <code>CacheConfigResponse</code> that will be returned to the
   * specified recipient.
   */
  public static CacheConfigResponse create(DistributionManager dm, InternalDistributedMember recipient, int cacheId, byte attributeCode, int newValue) {
    CacheConfigResponse m = new CacheConfigResponse();
    m.setRecipient(recipient);
    try {
      GemFireCacheImpl c = (GemFireCacheImpl)CacheFactory.getInstanceCloseOk(dm.getSystem());
      if (cacheId != System.identityHashCode(c)) {
        m.info = null;
      } else {
        switch (attributeCode) {
        case RemoteGemFireVM.LOCK_TIMEOUT_CODE:
          c.setLockTimeout(newValue);
          break;
        case RemoteGemFireVM.LOCK_LEASE_CODE:
          c.setLockLease(newValue);
          break;
        case RemoteGemFireVM.SEARCH_TIMEOUT_CODE:
          c.setSearchTimeout(newValue);
          break;
        default:
          Assert.assertTrue(false, "Unknown config code: " +
                            attributeCode);
        }
      }
      m.info = new RemoteCacheInfo(c);
    } 
    catch (CancelException ex) {
      m.info = null;

    } catch (Exception ex) {
      m.exception = ex;
      m.info = null;
    }
    return m;
  }

  public RemoteCacheInfo getCacheInfo() {
    return this.info;
  }
  
  /**
   * Returns the exception that was thrown while changing the cache
   * configuration.
   */
  public Exception getException() {
    return this.exception;
  }

  public int getDSFID() {
    return CACHE_CONFIG_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.info, out);
    DataSerializer.writeObject(this.exception, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.info = (RemoteCacheInfo)DataSerializer.readObject(in);
    this.exception = (Exception) DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "CacheConfigResponse from " + this.getSender() + " info=" + this.info;
  }
}
