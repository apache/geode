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


package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.admin.CacheInfo;

/**
 * A message that is sent to a particular distribution manager to get information on its current
 * cache.
 *
 * @since GemFire 3.5
 */
public class CacheConfigRequest extends AdminRequest {
  private byte attributeCode;
  private int newValue;
  private int cacheId;

  /**
   * Returns a <code>CacheConfigRequest</code>.
   */
  public static CacheConfigRequest create(CacheInfo c, int attCode, int v) {
    CacheConfigRequest m = new CacheConfigRequest();
    m.attributeCode = (byte) attCode;
    m.newValue = v;
    m.cacheId = c.getId();
    return m;
  }

  public CacheConfigRequest() {
    friendlyName = "Set a single cache configuration attribute";
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return CacheConfigResponse.create(dm, this.getSender(), this.cacheId, this.attributeCode,
        this.newValue);
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
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
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
