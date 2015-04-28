/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.internal.size.ReflectionObjectSizer;
import com.gemstone.gemfire.internal.size.SizeClassOnceObjectSizer;

/**
 * The sizer interface defines a method that when called returns the size of the
 * object passed in. Implementations may return hardcoded values for object size
 * if the implementation knows the object size for all objects that are likely
 * to be cached.
 * 
 * You should use a sizer with a {@link EvictionAttributes#createLRUHeapAttributes(ObjectSizer)}
 * or {@link EvictionAttributes#createLRUMemoryAttributes(ObjectSizer)} if you want
 * to use a faster or more accurate method of sizing than provided by the default
 * object sizer, which is {#link {@link #SIZE_CLASS_ONCE}
 * 
 * @author Sudhir Menon
 * @author Dan Smith
 * 
 * @since 3.0
 */
public interface ObjectSizer {

  /**
   * An implementation of {@link ObjectSizer} that calculates an accurate size
   * of the first instance of each class that is put in the cache. After the
   * first instance, it will return the same size for every instance of that
   * class.
   * 
   * This sizer is a compromise between generating accurate sizes for every
   * object and performance. It should work well if the keys and values in the
   * region don't vary greatly in size. For accurate sizing of every instance use
   * {@link #REFLECTION_SIZE} instead.
   * 
   * This sizer does generate an accurate size for strings and byte arrays every
   * time, because there is very little performance impact from sizing these
   * objects.
   * 
   * @since 6.5
   */
  public static final ObjectSizer SIZE_CLASS_ONCE = SizeClassOnceObjectSizer.getInstance();

  /**
   * An implementation of {@link ObjectSizer} that calculates an accurate size
   * for each object that it sizes.
   * 
   * This sizer will add up the sizes of all objects that are reachable from the
   * keys and values in your region by non-static fields. 
   * 
   * For objects that are all approximately the same size, consider using
   * {@link #SIZE_CLASS_ONCE}. It will have much better performance.
   * 
   * @since 6.5
   */
  public static final ObjectSizer REFLECTION_SIZE = ReflectionObjectSizer.getInstance();
  
  
  /**
   * The default object sizer, currently {@link #SIZE_CLASS_ONCE}
   * @since 6.5
   */
  public static final ObjectSizer DEFAULT = SIZE_CLASS_ONCE;

  public int sizeof( Object o );

}
