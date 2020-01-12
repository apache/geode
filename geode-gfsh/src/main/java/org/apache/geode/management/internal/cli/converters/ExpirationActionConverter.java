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

package org.apache.geode.management.internal.cli.converters;

import java.util.Arrays;
import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.ExpirationAction;

public class ExpirationActionConverter implements Converter<ExpirationAction> {
  @Immutable
  private static final ExpirationAction[] actions = {ExpirationAction.INVALIDATE,
      ExpirationAction.LOCAL_INVALIDATE, ExpirationAction.DESTROY, ExpirationAction.LOCAL_DESTROY};

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return type.isAssignableFrom(ExpirationAction.class);
  }

  @Override
  public ExpirationAction convertFromText(String value, Class<?> targetType, String optionContext) {
    String enumValue = value.replace('-', '_');
    return Arrays.stream(actions).filter(x -> x.toString().equalsIgnoreCase(enumValue)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException(
            String.format("Expiration action %s is not valid.", value)));
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType,
      String existingData, String optionContext, MethodTarget target) {
    Arrays.stream(actions).forEach(x -> completions.add(new Completion(x.toString())));
    return true;
  }
}
