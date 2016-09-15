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
package org.apache.geode.management.internal.cli.converters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.MethodTarget;

import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.MultipleValueAdapter;

/**
 * 
 * 
 * @since GemFire 7.0
 */
public class StringListConverter extends MultipleValueAdapter<List<String>> {

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return List.class.isAssignableFrom(type) && ConverterHint.STRING_LIST.equals(optionContext);
  }

  @Override
  public List<String> convertFromText(String[] value, Class<?> targetType,
      String context) {
    List<String> list = null;

    if (List.class.isAssignableFrom(targetType) && ConverterHint.STRING_LIST.equals(context) && value != null && value.length > 0) {
      list = new ArrayList<String>(Arrays.asList(value));
    }
    return list;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions,
      Class<?> targetType, String[] existingData, String context,
      MethodTarget target) {
    return List.class.isAssignableFrom(targetType) && ConverterHint.STRING_LIST.equals(context);
  }

}
