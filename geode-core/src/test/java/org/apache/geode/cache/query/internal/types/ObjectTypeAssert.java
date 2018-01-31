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
package org.apache.geode.cache.query.internal.types;

import org.assertj.core.api.AbstractAssert;

import org.apache.geode.cache.query.types.ObjectType;

/**
 * Custom Assertion to validate {@link ObjectType} instances.
 */
public class ObjectTypeAssert extends AbstractAssert<ObjectTypeAssert, ObjectType> {

  /**
   *
   * @param actual
   */
  public ObjectTypeAssert(ObjectType actual) {
    super(actual, ObjectTypeAssert.class);
  }

  /**
   *
   * @param actual
   * @return
   */
  public static ObjectTypeAssert assertThat(ObjectType actual) {
    return new ObjectTypeAssert(actual);
  }

  /**
   *
   * @return
   */
  public ObjectTypeAssert resolves(Class clazz) {
    isNotNull();
    org.assertj.core.api.Assertions.assertThat(actual.resolveClass()).isEqualTo(clazz);

    return this;
  }

  /**
   *
   * @return
   */
  public ObjectTypeAssert isObject() {
    isNotNull();
    isExactlyInstanceOf(ObjectTypeImpl.class);

    return this;
  }

  /**
   *
   * @param componentType
   * @return
   */
  public ObjectTypeAssert isCollectionOf(Class componentType) {
    isNotNull();
    isExactlyInstanceOf(CollectionTypeImpl.class);
    org.assertj.core.api.Assertions.assertThat(((CollectionTypeImpl) actual).getElementType())
        .isExactlyInstanceOf(componentType);

    return this;
  }

  /**
   *
   * @param keyType
   * @param valueType
   * @return
   */
  public ObjectTypeAssert isMapOf(Class keyType, Class valueType) {
    isNotNull();
    isExactlyInstanceOf(MapTypeImpl.class);
    org.assertj.core.api.Assertions.assertThat(((MapTypeImpl) actual).getKeyType())
        .isExactlyInstanceOf(keyType);
    org.assertj.core.api.Assertions.assertThat(((MapTypeImpl) actual).getElementType())
        .isExactlyInstanceOf(valueType);

    return this;
  }
}
