/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
//import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.*;
//import java.util.*;

/**
 * A message that is sent to a particular distribution manager to
 * get information on its current cache.
 * @since 3.5
 */
public final class CacheConfigRequest extends AdminRequest {
  private byte attributeCode;
  private int newValue;
  private int cacheId;

  /**
   * Returns a <code>CacheConfigRequest</code>.
   */
  public static CacheConfigRequest create(CacheInfo c, int attCode, int v) {
    CacheConfigRequest m = new CacheConfigRequest();
    m.attributeCode = (byte)attCode;
    m.newValue = v;
    m.cacheId = c.getId();
    return m;
  }

  public CacheConfigRequest() {
    friendlyName = LocalizedStrings.CacheConfigRequest_SET_A_SINGLE_CACHE_CONFIGURATION_ATTRIBUTE.toLocalizedString();
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return CacheConfigResponse.create(dm, this.getSender(), this.cacheId, this.attributeCode, this.newValue); 
  }

  public int getDSFID() {
    return CACHE_CONFIG_REQUEST;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeByte(this.attributeCode);
    out.writeInt(this.newValue);
    out.writeInt(this.cacheId);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.attributeCode = in.readByte();
    this.newValue = in.readInt();
    this.cacheId = in.readInt();
  }

  @Override
  public String toString() {
    return "CacheConfigRequest from " + this.getSender();
  }
}
