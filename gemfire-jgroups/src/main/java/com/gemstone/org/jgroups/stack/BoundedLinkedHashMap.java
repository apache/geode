/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.org.jgroups.stack;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class <code>BoundedLinkedHashMap</code> is a bounded
 * <code>LinkedHashMap</code>. The bound is the maximum
 * number of entries the <code>BoundedLinkedHashMap</code>
 * can contain.
 *
 * @author Barry Oglesby
 *
 * @since 4.2
 * @deprecated as of 5.7 create your own class that extends {@link LinkedHashMap}
 * and implement {@link LinkedHashMap#removeEldestEntry}
 * to enforce a maximum number of entries.
 */
@Deprecated
public class BoundedLinkedHashMap extends LinkedHashMap
{
  private static final long serialVersionUID = -3419897166186852692L;

  /**
   * The maximum number of entries allowed in this
   * <code>BoundedLinkedHashMap</code>
   */
  protected int _maximumNumberOfEntries;

  /**
   * Constructor.
   *
   * @param initialCapacity The initial capacity.
   * @param loadFactor The load factor
   * @param maximumNumberOfEntries The maximum number of allowed entries
   */
  public BoundedLinkedHashMap(int initialCapacity, float loadFactor, int maximumNumberOfEntries) {
    super(initialCapacity, loadFactor);
    this._maximumNumberOfEntries = maximumNumberOfEntries;
  }

  /**
   * Constructor.
   *
   * @param initialCapacity The initial capacity.
   * @param maximumNumberOfEntries The maximum number of allowed entries
   */
  public BoundedLinkedHashMap(int initialCapacity, int maximumNumberOfEntries) {
    super(initialCapacity);
    this._maximumNumberOfEntries = maximumNumberOfEntries;
  }

  /**
   * Constructor.
   *
   * @param maximumNumberOfEntries The maximum number of allowed entries
   */
  public BoundedLinkedHashMap(int maximumNumberOfEntries) {
    super();
    this._maximumNumberOfEntries = maximumNumberOfEntries;
  }

  /**
   * Returns the maximum number of entries.
   * @return the maximum number of entries
   */
  public int getMaximumNumberOfEntries(){
    return this._maximumNumberOfEntries;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry entry) {
    return size() > this._maximumNumberOfEntries;
  }
}
