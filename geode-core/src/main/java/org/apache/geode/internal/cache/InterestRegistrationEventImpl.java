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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.ClientSession;
import org.apache.geode.cache.InterestRegistrationEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;

public class InterestRegistrationEventImpl implements InterestRegistrationEvent, DataSerializable {

  private static final long serialVersionUID = -5791294858933070049L;

  /**
   * The name of the region to which this interest event belongs
   */
  private String regionName;

  /**
   * The Set of keys being registered or unregistered
   */
  private Set<?> keysOfInterest;

  /**
   * The type of interest
   */
  private int interestType;

  /**
   * Whether this interest event represents an interest registration
   */
  private boolean isRegister;

  /**
   * The <code>ClientSession</code> initiating this interest event
   */
  private transient ClientSession clientSession;

  /**
   * The GemFire <code>Cache</code>
   */
  private transient Cache cache;

  /**
   * Constructor. No-arg constructor for data serialization.
   */
  public InterestRegistrationEventImpl() {}

  /**
   * Constructor.
   *
   * @param regionName The name of the region to which this interest event belongs
   */
  public InterestRegistrationEventImpl(final @NotNull CacheClientProxy clientSession,
      final @NotNull String regionName, final @NotNull Set<?> keysOfInterest,
      final @NotNull InterestType interestType, final boolean isRegister) {
    cache = clientSession.getCache();
    this.clientSession = clientSession;
    this.regionName = regionName;
    this.keysOfInterest = keysOfInterest;
    this.interestType = interestType.ordinal();
    this.isRegister = isRegister;
  }

  @Override
  public ClientSession getClientSession() {
    return clientSession;
  }

  @Override
  public String getRegionName() {
    return regionName;
  }

  @Override
  public Region<?, ?> getRegion() {
    return cache.getRegion(regionName);
  }

  @Override
  public Set<?> getKeysOfInterest() {
    return keysOfInterest;
  }

  @Override
  public int getInterestType() {
    return interestType;
  }

  @Override
  public boolean isRegister() {
    return isRegister;
  }

  @Override
  public boolean isKey() {
    return interestType == InterestType.KEY.ordinal();
  }

  @Override
  public boolean isRegularExpression() {
    return interestType == InterestType.REGULAR_EXPRESSION.ordinal();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    // The proxy isn't being serialized right now, but if it needs to be
    // then the proxyId would probably be the best way to do it.
    DataSerializer.writeString(regionName, out);
    DataSerializer.writeHashSet((HashSet<?>) keysOfInterest, out);
    DataSerializer.writePrimitiveInt(interestType, out);
    DataSerializer.writePrimitiveBoolean(isRegister, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    regionName = DataSerializer.readString(in);
    keysOfInterest = DataSerializer.readHashSet(in);
    interestType = DataSerializer.readPrimitiveInt(in);
    isRegister = DataSerializer.readPrimitiveBoolean(in);
  }

  @Override
  public String toString() {
    return "InterestRegistrationEvent [" + "isRegister=" + isRegister
        + "clientSession=" + clientSession + "; isRegister="
        + "; regionName=" + regionName + "; keysOfInterest="
        + keysOfInterest + "; interestType="
        + InterestType.getString(InterestType.valueOf(interestType)) + "]";
  }
}
