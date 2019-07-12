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

package org.apache.geode.management.api;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;

import javax.xml.bind.annotation.XmlTransient;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.runtime.RuntimeInfo;

/**
 * provides correlation to the additional object that would be returned with the configuration
 * to indicate runtime information.
 * If a cache element has no additional runtime info (yet), it should be declared as
 * implement CorrespondWith<RuntimeInfo>
 */
@Experimental
public interface CorrespondWith<R extends RuntimeInfo> {
  /**
   * for internal use only
   */
  @XmlTransient
  @JsonIgnore
  default Class<R> getRuntimeClass() {
    Type[] genericInterfaces = getClass().getGenericInterfaces();

    ParameterizedType type =
        Arrays.stream(genericInterfaces).filter(ParameterizedType.class::isInstance)
            .map(ParameterizedType.class::cast)
            .findFirst().orElse(null);

    if (type == null) {
      return null;
    }

    @SuppressWarnings("unchecked")
    final Class<R> actualTypeArgument = (Class<R>) type.getActualTypeArguments()[0];
    return actualTypeArgument;
  }

  /**
   * for internal use only
   */
  default boolean hasRuntimeInfo() {
    return !RuntimeInfo.class.equals(getRuntimeClass());
  }

  /**
   * this is to indicate when we need to go gather runtime information for this configuration,
   * should we go to all members in the group, or just any member in the group
   */
  @XmlTransient
  @JsonIgnore
  default boolean isGlobalRuntime() {
    return false;
  }

}
