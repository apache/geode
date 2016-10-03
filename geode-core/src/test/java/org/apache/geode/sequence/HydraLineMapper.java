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
package org.apache.geode.sequence;

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

import org.apache.geode.sequence.LineMapper;

/**
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
