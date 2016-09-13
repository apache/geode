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
package org.apache.geode.management.internal;

import java.lang.reflect.Type;

import javax.management.openmbean.OpenType;

/**
 * Converter for classes where the open data is identical to the original
 * data. This is true for any of the SimpleType types, and for an
 * any-dimension array of those
 * 
 *
 */
public final class IdentityConverter extends OpenTypeConverter {
  IdentityConverter(Type targetType, OpenType openType, Class openClass) {
    super(targetType, openType, openClass);
  }

  boolean isIdentity() {
    return true;
  }

  final Object toNonNullOpenValue(Object value) {
    return value;
  }

  public final Object fromNonNullOpenValue(Object value) {
    return value;
  }
}
