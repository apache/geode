package com.gemstone.gemfire.cache;
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

import javax.print.attribute.EnumSyntax;

/** The algorithm used to determine when to perform an {@link com.gemstone.gemfire.cache.EvictionAction}
 * 
 * @author Mitch Thomas
 * @since 5.0
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
   * @since 6.5
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
