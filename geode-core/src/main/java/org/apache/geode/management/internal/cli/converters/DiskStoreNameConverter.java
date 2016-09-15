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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
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
public class DiskStoreNameConverter implements Converter<String> {

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return String.class.equals(type) && ConverterHint.DISKSTORE_ALL.equals(optionContext);
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
    if (String.class.equals(targetType) && ConverterHint.DISKSTORE_ALL.equals(optionContext)) {
      Set<String> diskStoreNames = getDiskStoreNames();
      
      for (String diskStoreName : diskStoreNames) {
        if (existingData != null) {
          if (diskStoreName.startsWith(existingData)) {
            completions.add(new Completion(diskStoreName));
          }
        } else {
          completions.add(new Completion(diskStoreName));
        }
      }
    }
    
    return !completions.isEmpty();
  }

  private Set<String> getDiskStoreNames() {
    SortedSet<String> diskStoreNames = new TreeSet<String>();
    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (gfsh != null && gfsh.isConnectedAndReady()) { // gfsh exists & is not null
      Map<String, String[]> diskStoreInfo = gfsh.getOperationInvoker().getDistributedSystemMXBean().listMemberDiskstore();
      if (diskStoreInfo != null) {
        Set<Entry<String, String[]>> entries = diskStoreInfo.entrySet();
        for (Entry<String, String[]> entry : entries) {
          String[] value = entry.getValue();
          if (value != null) {
            diskStoreNames.addAll(Arrays.asList(value));
          }
        }
      }
    }

    return diskStoreNames;
  }

}
