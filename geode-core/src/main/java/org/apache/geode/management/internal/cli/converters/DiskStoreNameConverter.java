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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.shell.Gfsh;

/**
 *
 *
 * @since GemFire 7.0
 */
public class DiskStoreNameConverter extends BaseStringConverter {

  @Override
  public String getConverterHint() {
    return ConverterHint.DISKSTORE;
  }

  @Override
  public Set<String> getCompletionValues() {
    SortedSet<String> diskStoreNames = new TreeSet<>();
    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (gfsh != null && gfsh.isConnectedAndReady()) { // gfsh exists & is not null
      Map<String, String[]> diskStoreInfo =
          gfsh.getOperationInvoker().getDistributedSystemMXBean().listMemberDiskstore();
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
