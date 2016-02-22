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

package com.gemstone.gemfire.modules.session.internal.filter.util;

import com.gemstone.gemfire.modules.session.internal.common.CacheProperty;

import java.util.HashMap;

/**
 *
 */
public class TypeAwareMap<K extends CacheProperty, Object> extends HashMap {

  private Class<K> keyType;

  public TypeAwareMap(Class<K> keyType) {
    super();
    this.keyType = keyType;
  }

  public Object put(K key, Object value) {
    if (!key.getClazz().isAssignableFrom(value.getClass())) {
      if (key.getClazz() == Boolean.class) {
        return (Object) super.put(key, Boolean.valueOf((String) value));
      } else if (key.getClazz() == Integer.class) {
        return (Object) super.put(key, Integer.valueOf((String) value));
      } else {
        throw new IllegalArgumentException("Value is not of type " +
            key.getClazz().getName());
      }
    }

    return (Object) super.put(key, value);
  }
}
