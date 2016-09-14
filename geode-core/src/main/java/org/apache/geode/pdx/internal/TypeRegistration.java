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
package org.apache.geode.pdx.internal;

import java.util.Map;

import org.apache.geode.cache.wan.GatewaySender;

/**
 * Interface for the part of the type registry
 * that interacts with remote members
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
