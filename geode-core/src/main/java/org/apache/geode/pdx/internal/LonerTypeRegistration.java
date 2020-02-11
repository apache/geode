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
package org.apache.geode.pdx.internal;

import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.InternalCache;

/**
 * A type registration that is used for loners. In the loner case, we'll try to be helpful and not
 * decide what type registration to give the user until they actually use it.
 */
public class LonerTypeRegistration implements TypeRegistration {

  private volatile TypeRegistration delegate = null;

  private final InternalCache cache;

  public LonerTypeRegistration(InternalCache cache) {
    this.cache = cache;
  }

  @Override
  public int defineType(PdxType newType) {
    initializeRegistry(null);
    return delegate.defineType(newType);
  }

  @Override
  public PdxType getType(int typeId) {
    initializeRegistry(null);
    return delegate.getType(typeId);
  }

  @Override
  public void addRemoteType(int typeId, PdxType type) {
    initializeRegistry(null);
    delegate.addRemoteType(typeId, type);
  }

  @Override
  public void initialize() {
    // do nothing. This type registry is initialized lazily.
  }

  @Override
  public void gatewaySenderStarted(GatewaySender gatewaySender) {
    initializeRegistry(false);
    delegate.gatewaySenderStarted(gatewaySender);
  }

  @Override
  public void creatingPersistentRegion() {
    if (delegate != null) {
      delegate.creatingPersistentRegion();
    }
  }

  @Override
  public void creatingPool() {
    initializeRegistry(true);
    delegate.creatingPool();
  }

  /**
   * Actually initialize the delegate. This is method is called when the type registry is used. At
   * that time, it creates the registry.
   *
   * @param isClient Whether the registration is known to belong to a Client (true) or a Peer
   *        (false). If null, the presence of a pool in the cache will be used to determine the type
   *        of TypeRegistration that should be used for the delegate.
   */
  synchronized void initializeRegistry(Boolean isClient) {
    if (delegate != null) {
      return;
    }
    if (isClient == null) {
      isClient = cache.hasPool();
    }
    final TypeRegistration delegateTmp = createTypeRegistration(isClient);
    delegateTmp.initialize();
    delegate = delegateTmp;
  }

  protected TypeRegistration createTypeRegistration(boolean client) {
    TypeRegistration delegateTmp;
    if (client) {
      delegateTmp = new ClientTypeRegistration(cache);
    } else {
      delegateTmp = new PeerTypeRegistration(cache);
    }
    return delegateTmp;
  }

  /**
   * Check to see if the current member is a loner and we can't tell if the user wants a peer or a
   * client type registry.
   *
   * @return true if this member is a loner and we can't determine what type of registry they want.
   */
  static boolean isIndeterminateLoner(InternalCache cache) {
    boolean isLoner = cache.getInternalDistributedSystem().isLoner();
    boolean pdxConfigured = cache.getPdxPersistent();
    return isLoner && !pdxConfigured/* && !hasGateways */;
  }

  @Override
  public int getEnumId(Enum<?> v) {
    initializeRegistry(null);
    return this.delegate.getEnumId(v);
  }

  @Override
  public void addRemoteEnum(int enumId, EnumInfo newInfo) {
    initializeRegistry(null);
    this.delegate.addRemoteEnum(enumId, newInfo);
  }

  @Override
  public int defineEnum(EnumInfo newInfo) {
    initializeRegistry(null);
    return delegate.defineEnum(newInfo);
  }

  @Override
  public EnumInfo getEnumById(int enumId) {
    initializeRegistry(null);
    return delegate.getEnumById(enumId);
  }

  @Override
  public Map<Integer, PdxType> types() {
    initializeRegistry(null);
    return delegate.types();
  }

  @Override
  public Map<Integer, EnumInfo> enums() {
    initializeRegistry(null);
    return delegate.enums();
  }

  @Override
  public PdxType getPdxTypeForField(String fieldName, String className) {
    initializeRegistry(null);
    return delegate.getPdxTypeForField(fieldName, className);
  }

  @Override
  public Set<PdxType> getPdxTypesForClassName(String className) {
    initializeRegistry(null);
    return delegate.getPdxTypesForClassName(className);
  }

  @Override
  public boolean isClient() {
    initializeRegistry(null);
    return delegate.isClient();
  }

  @Override
  public void addImportedType(int typeId, PdxType importedType) {
    initializeRegistry(null);
    this.delegate.addImportedType(typeId, importedType);
  }

  @Override
  public void addImportedEnum(int enumId, EnumInfo importedInfo) {
    initializeRegistry(null);
    this.delegate.addImportedEnum(enumId, importedInfo);
  }

  @Override
  public int getLocalSize() {
    initializeRegistry(null);
    return delegate.getLocalSize();
  }

  @Override
  public Map<PdxType, Integer> getTypeToIdMap() {
    initializeRegistry(null);
    return delegate.getTypeToIdMap();
  }

  @Override
  public Map<EnumInfo, EnumId> getEnumToIdMap() {
    initializeRegistry(null);
    return delegate.getEnumToIdMap();
  }

  @Override
  public void clearLocalMaps() {
    initializeRegistry(null);
    delegate.clearLocalMaps();
  }

  @Override
  public void flushCache() {
    initializeRegistry(null);
    delegate.flushCache();
  }

  // For testing
  Class<? extends TypeRegistration> getDelegateClass() {
    if (delegate != null) {
      return delegate.getClass();
    } else {
      return null;
    }
  }
}
