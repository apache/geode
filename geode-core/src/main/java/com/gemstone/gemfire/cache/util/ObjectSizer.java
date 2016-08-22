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
 * 
 * @since GemFire 3.0
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
   * @since GemFire 6.5
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
   * @since GemFire 6.5
   */
  public static final ObjectSizer REFLECTION_SIZE = ReflectionObjectSizer.getInstance();
  
  
  /**
   * The default object sizer, currently {@link #SIZE_CLASS_ONCE}
   * @since GemFire 6.5
   */
  public static final ObjectSizer DEFAULT = SIZE_CLASS_ONCE;

  public int sizeof( Object o );

}
