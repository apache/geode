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

import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

import org.apache.geode.management.cli.ConverterHint;

/***
 * Added converter to enable auto-completion for index-type
 *
 */
@SuppressWarnings("deprecation")
public class IndexTypeConverter implements Converter<org.apache.geode.cache.query.IndexType> {

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return org.apache.geode.cache.query.IndexType.class.isAssignableFrom(type)
        && optionContext.contains(ConverterHint.INDEX_TYPE);
  }

  @Override
  public org.apache.geode.cache.query.IndexType convertFromText(String value, Class<?> targetType,
      String optionContext) {
    switch (value.toLowerCase()) {
      case "range":
        return org.apache.geode.cache.query.IndexType.FUNCTIONAL;
      case "key":
        return org.apache.geode.cache.query.IndexType.PRIMARY_KEY;
      case "hash":
        return org.apache.geode.cache.query.IndexType.HASH;
    }

    throw new IllegalArgumentException("invalid index type: " + value);
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType,
      String existingData, String optionContext, MethodTarget target) {
    completions.add(new Completion("range"));
    completions.add(new Completion("key"));
    completions.add(new Completion("hash"));
    return true;
  }
}
