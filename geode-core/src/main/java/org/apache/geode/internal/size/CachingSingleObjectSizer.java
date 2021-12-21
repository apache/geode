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
package org.apache.geode.internal.size;

import java.util.Map;

import org.apache.geode.internal.util.concurrent.CopyOnWriteWeakHashMap;

public class CachingSingleObjectSizer implements SingleObjectSizer {
  private final Map<Class, Long> sizeCache = new CopyOnWriteWeakHashMap<>();
  private final SingleObjectSizer wrappedSizer;

  public CachingSingleObjectSizer(SingleObjectSizer sizer) {
    wrappedSizer = sizer;
  }

  @Override
  public long sizeof(Object object) {
    Class clazz = object.getClass();
    if (clazz.isArray()) {
      return wrappedSizer.sizeof(object);
    } else {
      Long size = sizeCache.get(clazz);
      if (size != null) {
        return size.longValue();
      }
      size = Long.valueOf(wrappedSizer.sizeof(object));
      sizeCache.put(clazz, size);
      return size.longValue();
    }
  }

}
