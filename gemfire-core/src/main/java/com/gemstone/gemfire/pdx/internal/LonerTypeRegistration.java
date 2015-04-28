/*=========================================================================
 * Copyright (c) 2011-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author dsmith
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
    // TODO Auto-generated method stub
    
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
