/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx.internal;

import java.util.Map;

import com.gemstone.gemfire.cache.wan.GatewaySender;

/**
 * Interface for the part of the type registry
 * that interacts with remote members
 * @author dsmith
 *
 */
public interface TypeRegistration {
  
  /**
   * Define the type in the distributed system
   */
  public int defineType(PdxType newType);

  /**
   * Get the type id from the distributed system
   */
  public PdxType getType(int typeId);

  /**
   * Add a type id that has come from a remote member.
   */
  public void addRemoteType(int typeId, PdxType type);
  
  public void addImportedType(int typeId, PdxType importedType);
  
  /**
   * Test hook to get the last allocated type id
   */
  public int getLastAllocatedTypeId();

  public void initialize();
  
  public void gatewaySenderStarted(GatewaySender gatewaySender);

  public void creatingPersistentRegion();

  public void creatingPool();

  public int getEnumId(Enum<?> v);

  public void addRemoteEnum(int enumId, EnumInfo newInfo);

  public void addImportedEnum(int enumId, EnumInfo importedInfo);
  
  public int defineEnum(EnumInfo newInfo);

  public EnumInfo getEnumById(int enumId);

  /**
   * Returns the currently defined types.
   * @return the types
   */
  Map<Integer, PdxType> types();

  /**
   * Returns the currently defined enums.
   * @return the enums
   */
  Map<Integer, EnumInfo> enums();

  /**
   * Returns PdxType having the field
   * @param fieldName
   * @param className
   * @return PdxType or null if field not present
   */
  public PdxType getPdxTypeForField(String fieldName, String className);

 /*
   * test hook
   */
  public void testClearRegistry();

  public boolean isClient();
  
  /**
   * Return the size of the type registry in this member.
   */
  public int getLocalSize();
}
