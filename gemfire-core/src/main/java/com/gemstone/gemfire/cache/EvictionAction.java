package com.gemstone.gemfire.cache;
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

import javax.print.attribute.EnumSyntax;
/**
 * The action that an {@link com.gemstone.gemfire.cache.EvictionAlgorithm} takes.
 * @author Mitch Thomas
 * @since 5.0
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
   * @since 6.5
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
