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
package com.gemstone.gemfire.cache;

import javax.print.attribute.EnumSyntax;
/**
 * The action that an {@link com.gemstone.gemfire.cache.EvictionAlgorithm} takes.
 * @since GemFire 5.0
 * @see com.gemstone.gemfire.cache.EvictionAlgorithm
 * @see com.gemstone.gemfire.internal.cache.EvictionAttributesImpl
 */
public final class EvictionAction extends EnumSyntax
{
  private static final long serialVersionUID = -98840597493242980L;
  /** Canonical EvictionAction that represents no eviction 
   */
  public static final EvictionAction NONE = new EvictionAction(0);
  
  /** Perform a {@link com.gemstone.gemfire.cache.Region#localDestroy(Object)
   * localDestory} on the least recently used region entry. */
  public static final EvictionAction LOCAL_DESTROY = new EvictionAction(1);

  /** Write the value of the least recently used region entry to disk
   * and <code>null</code>-out its value in the VM to free up heap
   * space.  Note that this action is only available when the region
   * has been configured to access data on disk. */
  public static final EvictionAction OVERFLOW_TO_DISK = new EvictionAction(2);

  /** The default eviction action is to {@linkplain #LOCAL_DESTROY
   * locally destroy} an Entry. */
  public static final EvictionAction DEFAULT_EVICTION_ACTION = LOCAL_DESTROY;
  
  private EvictionAction(int val) { super(val); }
  
  private static final String[] stringTable = {
    "none",
    "local-destroy",
    "overflow-to-disk",
  };
  
  @Override
  final protected String[] getStringTable() {
    return stringTable;
  }
    
  //TODO post Java 1.8.0u45 uncomment final flag, see JDK-8076152
  private static /*final*/ EvictionAction[] enumValueTable = {
    NONE,
    LOCAL_DESTROY,
    OVERFLOW_TO_DISK
  };
    
  @Override
  final protected EnumSyntax[] getEnumValueTable() {
    return enumValueTable;
  }
  
  public final boolean isLocalDestroy() {
    return this == LOCAL_DESTROY;
  }
  
  public final boolean isOverflowToDisk() {
    return this == OVERFLOW_TO_DISK;
  }
  
  public final boolean isNone() {
    return this == NONE;
  }

  /**
   * Returns the eviction action the corresponds to the given parameter.
   * Returns <code>null</code> if no action corresponds.
   * @since GemFire 6.5
   */
  public static EvictionAction parseValue(int v) {
    if (v < 0 || v >= enumValueTable.length) {
      return null;
    } else {
      return enumValueTable[v];
    }
  }
  /** 
   * 
   * @param s
   * @return the action parsed from the provided string.  If there are problems with parsing
   * NONE is returned.
   */
  public static EvictionAction parseAction(String s) {
    if (s == null)
      return NONE;
    if (s.length() < 1) 
      return NONE;
    for (int i = 0; i < stringTable.length; ++i) {
      if (s.equals(stringTable[i])) {
        return enumValueTable[i]; 
      }
    }
    return NONE;
  }
}
