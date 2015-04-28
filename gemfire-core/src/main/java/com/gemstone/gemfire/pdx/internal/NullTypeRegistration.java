/*=========================================================================
 * Copyright (c) 2011-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx.internal;

import java.util.Collections;
import java.util.Map;

import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.pdx.PdxInitializationException;

/**
 * A type registration that does nothing. Used if the user
 * explicity disables the type registry
 * @author dsmith
 *
 */
public class NullTypeRegistration implements TypeRegistration {

  public int defineType(PdxType newType) {
    throw new PdxInitializationException("Trying to use PDX type, but type registry is disabled");
  }

  public PdxType getType(int typeId) {
    throw new PdxInitializationException("Trying to use PDX type, but type registry is disabled");
  }

  public void addRemoteType(int typeId, PdxType type) {
    throw new PdxInitializationException("Trying to use PDX type, but type registry is disabled");
  }

  public int getLastAllocatedTypeId() {
    throw new PdxInitializationException("Trying to use PDX type, but type registry is disabled");
  }

  public void initialize() {
    //do nothing
  }

  public void gatewaySenderStarted(GatewaySender gatewaySender) {
    //do nothing
  }
  
  public void creatingPersistentRegion() {
    //do nothing
  }

  public void creatingPool() {
    //do nothing
  }

  public int getEnumId(Enum<?> v) {
    throw new PdxInitializationException("Trying to use PDX type, but type registry is disabled");
  }

  public void addRemoteEnum(int enumId, EnumInfo newInfo) {
    throw new PdxInitializationException("Trying to use PDX type, but type registry is disabled");
  }

  public int defineEnum(EnumInfo newInfo) {
    throw new PdxInitializationException("Trying to use PDX type, but type registry is disabled");
  }

  public EnumInfo getEnumById(int enumId) {
    throw new PdxInitializationException("Trying to use PDX type, but type registry is disabled");
  }

  @Override
  public Map<Integer, PdxType> types() {
    return Collections.emptyMap();
  }

  @Override
  public Map<Integer, EnumInfo> enums() {
    return Collections.emptyMap();
  }

  @Override
  public PdxType getPdxTypeForField(String fieldName, String className) {
   return null;
  }

  @Override
  public void testClearRegistry() {
    
  }
  
  @Override
  public boolean isClient() {
    return false;
  }

  @Override
  public void addImportedType(int typeId, PdxType importedType) {
    throw new PdxInitializationException("Trying to use PDX type, but type registry is disabled");
  }

  @Override
  public void addImportedEnum(int enumId, EnumInfo importedInfo) {
    throw new PdxInitializationException("Trying to use PDX type, but type registry is disabled");
  }

  @Override
  public int getLocalSize() {
    return 0;
  }
}
