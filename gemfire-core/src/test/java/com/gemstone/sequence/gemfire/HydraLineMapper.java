/*=========================================================================
 * Copyright (c) 2011-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.sequence.gemfire;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.gemstone.sequence.LineMapper;

/**
 * @author dsmith
 *
 */
public class HydraLineMapper implements LineMapper {
  private static final Pattern VM_NAME_PATTERN = Pattern.compile("(vm_\\d+).*_(\\d+)(_end)?\\.log");
  private static final Pattern DISK_DIR_PATTERN = Pattern.compile("vm_(\\d+).*_disk_1");
  private final Map<String, String> processIdToVMName = new HashMap<String, String>();
  private final DefaultLineMapper defaultMapper = new DefaultLineMapper();
  
  public HydraLineMapper(File[] graphFiles) {
    File firstFile = graphFiles[0];
    File directory = firstFile.getParentFile();
    if(directory == null || ! new File(directory, "latest.prop").exists()) {
      directory = new File(".");
    }
    String[] files = directory.list();
    for(String file : files) {
      Matcher matcher = VM_NAME_PATTERN.matcher(file);
      if(matcher.matches()) {
        processIdToVMName.put(matcher.group(2), matcher.group(1));
      }
    }
    
    for(String file : files) {
      Matcher matcher = DISK_DIR_PATTERN.matcher(file);
      if(matcher.matches()) {
        
        String storeId = getDiskStoreId(file);
        if(storeId != null) {
          processIdToVMName.put(storeId, "disk_" + matcher.group(1));
        }
      }
    }
    
    
  }

  private String getDiskStoreId(String diskStoreDir) {
    File dir = new File(diskStoreDir);
    String[] files = dir.list();
    for(String fileName : files) {
      if(fileName.endsWith(".if")) {
        try {
          return getDiskStoreIdFromInitFile(dir, fileName);
        } catch (Exception e) {
          return null;
        }
      }
    }
    
    return null;
  }

  private String getDiskStoreIdFromInitFile(File dir, String fileName)
      throws FileNotFoundException, IOException {
    FileInputStream fis = new FileInputStream(new File(dir, fileName));
    try {
      byte[] bytes = new byte[1 + 8 + 8];
      fis.read(bytes);
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      //Skip the record type.
      buffer.get();
      long least = buffer.getLong();
      long most = buffer.getLong();
      UUID id = new UUID(most, least);
      return id.toString();
    } finally {
      fis.close();
    }
  }

  public String getShortNameForLine(String lineName) {
    String name = defaultMapper.getShortNameForLine(lineName);
    if(processIdToVMName.containsKey(name)) {
      return processIdToVMName.get(name);
    } else {
      return name;
    }
  }
  
  public static boolean isInHydraRun(File[] graphFiles) {
    if(graphFiles.length == 0) {
      return false;
    }
    File firstFile = graphFiles[0];
    File parentFile = firstFile.getParentFile();
    for(File file : graphFiles) {
      if(parentFile == null && file.getParentFile() == null) {
        return true;
      }
      if (parentFile == null || file.getParentFile() == null
          || !file.getParentFile().equals(parentFile)) {
        return false;
      }
    }
    
    return new File(parentFile, "latest.prop").exists() 
      || new File("latest.prop").exists();
    
    
  }

}
