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
package com.gemstone.gemfire.pdx.internal;

import java.util.Map;

import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.cache.CacheConfig;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

/**
 * A type registration that is used for loners. In the 
 * loner case, we'll try to be helpful and not decide
 * what type registration to give the user until they actually 
 * use it.
 *
 */
public class LonerTypeRegistration implements TypeRegistration {
  
  private volatile TypeRegistration delegate = null;
  
  private final GemFireCacheImpl cache;
  
  
  public LonerTypeRegistration(GemFireCacheImpl cache) {
    this.cache = cache;
  }

  public int defineType(PdxType newType) {
    initializeRegistry();
    return delegate.defineType(newType);
  }

  public PdxType getType(int typeId) {
    initializeRegistry();
    return delegate.getType(typeId);
  }

  public void addRemoteType(int typeId, PdxType type) {
    initializeRegistry();
    delegate.addRemoteType(typeId, type);
  }

  public int getLastAllocatedTypeId() {
    initializeRegistry();
    return delegate.getLastAllocatedTypeId();
  }

  public void initialize() {
    //do nothing. This type registry is initialized lazily.
  }

  public void gatewaySenderStarted(GatewaySender gatewaySender) {
    initializeRegistry(false);
    delegate.gatewaySenderStarted(gatewaySender);
  }
  
  public void creatingPersistentRegion() {
    if(delegate != null) {
      delegate.creatingPersistentRegion();
    }

  }

  public void creatingPool() {
    initializeRegistry(true);
    delegate.creatingPool();
  }
  
  /**
   * Actually initialize the delegate. This is method
   * is called when the type registry is used. At that time,
   * it creates the registry.
   */
  private synchronized void initializeRegistry() {
    initializeRegistry(cache.hasPool());
  }
  
  private synchronized void initializeRegistry(boolean client) {
    if(delegate != null) {
      return;
    }
    TypeRegistration delegateTmp;
    
    if (client) {
      delegateTmp = new ClientTypeRegistration(cache);
    } else {
      delegateTmp = new PeerTypeRegistration(cache);
    }
    delegateTmp.initialize();
    delegate = delegateTmp;
  }

  /**
   * Check to see if the current member is a loner and we can't tell
   * if the user wants a peer or a client type registry.
   * @param cache
   * @return true if this member is a loner and we can't determine what
   * type of registry they want.
   */
  public static boolean isIndeterminateLoner(GemFireCacheImpl cache) {
    boolean isLoner = cache.getDistributedSystem().isLoner();
    boolean pdxConfigured = cache.getPdxPersistent();
    return isLoner && !pdxConfigured/* && !hasGateways*/;
  }

  public int getEnumId(Enum<?> v) {
    initializeRegistry();
    return this.delegate.getEnumId(v);
  }

  public void addRemoteEnum(int enumId, EnumInfo newInfo) {
    initializeRegistry();
    this.delegate.addRemoteEnum(enumId, newInfo);
  }

  public int defineEnum(EnumInfo newInfo) {
    initializeRegistry();
    return delegate.defineEnum(newInfo);
  }

  public EnumInfo getEnumById(int enumId) {
    initializeRegistry();
    return delegate.getEnumById(enumId);
  }

  @Override
  public Map<Integer, PdxType> types() {
    initializeRegistry();
    return delegate.types();
  }

  @Override
  public Map<Integer, EnumInfo> enums() {
    initializeRegistry();
    return delegate.enums();
  }

  @Override
  public PdxType getPdxTypeForField(String fieldName, String className) {
    return delegate.getPdxTypeForField(fieldName, className);
  }

  @Override
  public void testClearRegistry() {
  }
  
  @Override
  public boolean isClient() {
    return delegate.isClient();
  }

  @Override
  public void addImportedType(int typeId, PdxType importedType) {
    initializeRegistry();
    this.delegate.addImportedType(typeId, importedType);
  }

  @Override
  public void addImportedEnum(int enumId, EnumInfo importedInfo) {
    initializeRegistry();
    this.delegate.addImportedEnum(enumId, importedInfo);
  }
  
  @Override
  public int getLocalSize() {
    return delegate.getLocalSize();
  }
}
