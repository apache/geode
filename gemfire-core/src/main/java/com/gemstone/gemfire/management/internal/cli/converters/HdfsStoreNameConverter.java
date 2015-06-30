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
 * @author Namrata Thanvi
 * 
 */

public class HdfsStoreNameConverter implements Converter<String> {

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    return String.class.equals(type) && ConverterHint.HDFSSTORE_ALL.equals(optionContext);
  }

  @Override
  public String convertFromText(String value, Class<?> targetType, String optionContext) {
    return value;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions, Class<?> targetType, String existingData,
      String optionContext, MethodTarget target) {
    if (String.class.equals(targetType) && ConverterHint.HDFSSTORE_ALL.equals(optionContext)) {
      Set<String> hdfsStoreNames = getHdfsStoreNames();

      for (String hdfsStoreName : hdfsStoreNames) {
        if (existingData != null) {
          if (hdfsStoreName.startsWith(existingData)) {
            completions.add(new Completion(hdfsStoreName));
          }
        }
        else {
          completions.add(new Completion(hdfsStoreName));
        }
      }
    }

    return !completions.isEmpty();
  }

  private Set<String> getHdfsStoreNames() {
    SortedSet<String> hdfsStoreNames = new TreeSet<String>();
    Gfsh gfsh = Gfsh.getCurrentInstance();

    if (gfsh != null && gfsh.isConnectedAndReady()) {
      Map<String, String[]> hdfsStoreInfo = gfsh.getOperationInvoker().getDistributedSystemMXBean()
          .listMemberHDFSStore();
      if (hdfsStoreInfo != null) {
        Set<Entry<String, String[]>> entries = hdfsStoreInfo.entrySet();

        for (Entry<String, String[]> entry : entries) {
          String[] value = entry.getValue();
          if (value != null) {
            hdfsStoreNames.addAll(Arrays.asList(value));
          }
        }

      }
    }

    return hdfsStoreNames;
  }

}
