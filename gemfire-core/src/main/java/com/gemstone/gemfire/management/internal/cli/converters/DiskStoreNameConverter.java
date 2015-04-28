/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.converters;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
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
