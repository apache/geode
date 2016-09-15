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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.shell.Gfsh;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

/**
 *
 *
 * @since GemFire 7.0
 */
public class MemberIdNameConverter implements Converter<String> {
  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return String.class.equals(type) && ConverterHint.MEMBERIDNAME.equals(optionContext);
  }

  @Override
  public String convertFromText(String value, Class<?> targetType,
      String optionContext) {
    return value;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions,
      Class<?> targetType, String existingData, String optionContext,
      MethodTarget target) {
    if (String.class.equals(targetType) && ConverterHint.MEMBERIDNAME.equals(optionContext)) {
      Set<String> memberIdAndNames = getMemberIdAndNames();

      for (String string : memberIdAndNames) {
        completions.add(new Completion(string));
      }
    }

    return !completions.isEmpty();
  }

  private Set<String> getMemberIdAndNames() {
    final Set<String> nonLocatorMembers = new TreeSet<String>();

    final Gfsh gfsh = Gfsh.getCurrentInstance();

    if (gfsh != null && gfsh.isConnectedAndReady()) {
      nonLocatorMembers.addAll(Arrays.asList(gfsh.getOperationInvoker().getDistributedSystemMXBean().listMembers()));

      final String[] locatorMembers = gfsh.getOperationInvoker().getDistributedSystemMXBean().listLocatorMembers(true);

      if (locatorMembers != null && locatorMembers.length != 0) {
        nonLocatorMembers.removeAll(Arrays.asList(locatorMembers));
      }
    }

    return nonLocatorMembers;
  }

}
