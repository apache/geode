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

/** The algorithm used to determine when to perform an {@link com.gemstone.gemfire.cache.EvictionAction}
 * 
 * @since GemFire 5.0
 * @see com.gemstone.gemfire.cache.EvictionAction
 * @see com.gemstone.gemfire.internal.cache.EvictionAttributesImpl
 */
public final class EvictionAlgorithm extends EnumSyntax
{
  private static final long serialVersionUID = 5778669432033106789L;
  /** 
   * The canonical EvictionAction that represents no eviction action  
   */
  public static final EvictionAlgorithm NONE = new EvictionAlgorithm(0);
  
  /**
   * An algorithm that considers the number of Entries in the Region before
   * invoking its {@link EvictionAction} 
   */
  public static final EvictionAlgorithm LRU_ENTRY = new EvictionAlgorithm(1);
  
  /** 
   * An algorithm that considers the JVM heap size before invoking its {@link EvictionAction}
   */
  public static final EvictionAlgorithm LRU_HEAP = new EvictionAlgorithm(2);
  
  /** 
   * An algorithm that considers the amount of bytes consumed by the Region before invoking its {@link EvictionAction} 
   */
  public static final EvictionAlgorithm LRU_MEMORY = new EvictionAlgorithm(3);
  
  /**
   * An algorithm that considers the number of Entries in the Region before
   * invoking its {@link EvictionAction}
   * 
   * @deprecated
   */
  public static final EvictionAlgorithm LIFO_ENTRY = new EvictionAlgorithm(4);

  /**
   * An algorithm that considers the amount of bytes consumed by the Region
   * before invoking its {@link EvictionAction}
   * 
   * @deprecated
   */
  public static final EvictionAlgorithm LIFO_MEMORY = new EvictionAlgorithm(5);
  
  private EvictionAlgorithm(int val) { super(val); }
  
  private static final String[] stringTable = {
    "none",
    "lru-entry-count",
    "lru-heap-percentage",
    "lru-memory-size",
    "lifo-entry-count",
    "lifo-memory-size"
  };
  
  @Override
  final protected String[] getStringTable() {
    return stringTable;
  }
    
  //TODO post Java 1.8.0u45 uncomment final flag, see JDK-8076152
  private static /*final*/ EvictionAlgorithm[] enumValueTable = {
    NONE,
    LRU_ENTRY,
    LRU_HEAP,
    LRU_MEMORY,
    LIFO_ENTRY,
    LIFO_MEMORY,
  };
    
  @Override
  final protected EnumSyntax[] getEnumValueTable() {
    return enumValueTable;
  }

  /**
   * Returns the eviction action the corresponds to the given parameter.
   * Returns <code>null</code> if no action corresponds.
   * @since GemFire 6.5
   */
  public static EvictionAlgorithm parseValue(int v) {
    if (v < 0 || v >= enumValueTable.length) {
      return null;
    } else {
      return enumValueTable[v];
    }
  }
  public static EvictionAlgorithm parseAction(String s) {
    if (s == null)
      return null;
    if (s.length() < 1) 
      return null;
    for (int i = 0; i < stringTable.length; ++i) {
      if (s.equals(stringTable[i])) {
        return enumValueTable[i]; 
      }
    }
    return null;
  }

    public final boolean isLRUEntry() {
	return this == LRU_ENTRY;
    }

    public final boolean isLRUMemory() {
	return this == LRU_MEMORY;
    }
    
    public final boolean isLRUHeap() {
	return this == LRU_HEAP;
    }

    /** returns true if this object uses a least-recently-used algorithm */
    public boolean isLRU() {
      return this.isLRUEntry() || this.isLRUMemory() || this.isLRUHeap();
    }

    public final boolean isNone() {
	return this == NONE;
    }

    /**
     * @deprecated
     */
    public boolean isLIFO() {
      return this == LIFO_ENTRY || this == LIFO_MEMORY;
    }
}
