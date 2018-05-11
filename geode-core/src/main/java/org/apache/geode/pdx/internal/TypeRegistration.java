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

/**
 * Interface for the part of the type registry that interacts with remote members
 *
 */
public interface TypeRegistration {

  /**
   * Define the type in the distributed system
   */
  int defineType(PdxType newType);

  /**
   * Get the type id from the distributed system
   */
  PdxType getType(int typeId);

  /**
   * Add a type id that has come from a remote member.
   */
  void addRemoteType(int typeId, PdxType type);

  void addImportedType(int typeId, PdxType importedType);

  void initialize();

  void gatewaySenderStarted(GatewaySender gatewaySender);

  void creatingPersistentRegion();

  void creatingPool();

  int getEnumId(Enum<?> v);

  void addRemoteEnum(int enumId, EnumInfo newInfo);

  void addImportedEnum(int enumId, EnumInfo importedInfo);

  int defineEnum(EnumInfo newInfo);

  EnumInfo getEnumById(int enumId);

  /**
   * Returns the currently defined types.
   *
   * @return the types
   */
  Map<Integer, PdxType> types();

  /**
   * Returns the currently defined enums.
   *
   * @return the enums
   */
  Map<Integer, EnumInfo> enums();

  /**
   * Returns PdxType having the field
   *
   * @return PdxType or null if field not present
   */
  PdxType getPdxTypeForField(String fieldName, String className);

  /**
   * Returns all the PdxTypes for the given class name.
   * An empty set will be returned if no types exist.
   */
  Set<PdxType> getPdxTypesForClassName(String className);

  boolean isClient();

  /**
   * Return the size of the type registry in this member.
   */
  int getLocalSize();
}
