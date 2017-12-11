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
package org.apache.geode.internal.cache.eviction;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.ObjectSizer;

abstract class SizeLRUController extends AbstractEvictionController {

  private ObjectSizer sizer;

  SizeLRUController(EvictionAction evictionAction, Region region, ObjectSizer sizer) {
    super(evictionAction, region);
    this.sizer = sizer;
  }

  /**
   * Return the size of an object as stored in GemFire. Typically this is the serialized size in
   * bytes. This implementation is slow. Need to add Sizer interface and call it for customer
   * objects.
   */
  int sizeof(Object object) throws IllegalArgumentException {
    return MemoryLRUController.basicSizeof(object, this.sizer);
  }

  /**
   * Sets the {@link ObjectSizer} used to calculate the size of objects placed in the cache.
   *
   * @param sizer The name of the sizer class
   */
  void setSizer(ObjectSizer sizer) {
    this.sizer = sizer;
  }

}
