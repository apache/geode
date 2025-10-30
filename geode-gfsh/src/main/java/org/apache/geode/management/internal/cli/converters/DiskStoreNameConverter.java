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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import org.apache.geode.management.internal.cli.shell.Gfsh;

/**
 * Spring Shell 3.x converter for disk store names.
 *
 * <p>
 * Converts a disk store name string to itself (passthrough conversion).
 * Used by commands that operate on disk stores (compact, describe, etc.).
 *
 * <p>
 * SPRING SHELL 3.x MIGRATION NOTE:
 * - Spring Shell 1.x: Extended BaseStringConverter with getCompletionValues()
 * - Spring Shell 3.x: Simple Converter<String, String> for conversion only
 * - Completion logic preserved in getCompletionValues() for ValueProvider use
 * - Auto-completion should be implemented via ValueProvider (separate concern)
 *
 * @since GemFire 7.0
 */
@Component
public class DiskStoreNameConverter implements Converter<String, String> {

  /**
   * Converts a disk store name string (passthrough conversion).
   *
   * @param source the disk store name
   * @return the same disk store name
   */
  @Override
  public String convert(@NonNull String source) {
    return source;
  }

  /**
   * Gets completion values for disk store names from the distributed system.
   *
   * <p>
   * This method is preserved for potential use by a ValueProvider in Spring Shell 3.x.
   * It queries the distributed system MXBean to get all available disk store names.
   *
   * @return set of disk store names available in the distributed system
   */
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
