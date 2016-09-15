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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

import org.apache.geode.management.cli.ConverterHint;

/**
 * 
 * @since GemFire 7.0
 */
public class FilePathStringConverter implements Converter<String> {
  @Override
  public boolean supports(Class<?> type, String optionContext) {
//    System.out.println("FilePathConverter.supports() : type :: "+type+", optionContext :: "+optionContext);
    return String.class.equals(type) && ConverterHint.FILE_PATHSTRING.equals(optionContext);
  }

  @Override
  public String convertFromText(String value, Class<?> targetType, String optionContext) {
//    System.out.println("FilePathConverter.convertFromText() : optionContext :: "+optionContext);
    return value;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions,
      Class<?> targetType, String existingData, String optionContext,
      MethodTarget target) {
    // prefix is needed while comparing Completion Candidates as potential matches 
    String prefixToUse = "";
    boolean prependAbsolute = true;
    File   parentDir   = null; // directory to be searched for file(s)
    
    if (existingData != null) {
  //    System.out.println("FilePathConverter.getAllPossibleValues() : optionContext :: "+optionContext+", existingData : "+existingData);
      String[] completionValues = new String[0];
      
      if (ConverterHint.FILE_PATHSTRING.equals(optionContext)) {
        // if existingData is empty, start from root 
        if (existingData != null && existingData.trim().isEmpty()) {
          File[] listRoots = File.listRoots();
          completionValues = new String[listRoots.length];
          for (int i = 0; i < listRoots.length; i++) {
            completionValues[i] = listRoots[i].getPath();
          }
          prefixToUse = File.separator;
        } else {
          // Create a file from existing data
          File file = new File(existingData);
          if (file.isDirectory()) {
            // For a directory, list files/sub-dirsin the directory
            parentDir = file;
            completionValues = parentDir.list();
          } else if (!file.exists()) {
            parentDir = file.getParentFile();
            if (parentDir == null) {
              try {
                parentDir = file.getCanonicalFile().getParentFile();
              } catch (IOException e) {
                parentDir = null;
              }
            }
            if (parentDir != null) {
              completionValues = parentDir.list(new FileNameFilterImpl(parentDir, file.getName()));
            }
          }
          // whether the file path is absolute
          prependAbsolute = file.isAbsolute();
        }
      }
      
      if (completionValues.length > 0) {
        // use directory path as prefix for completion of names of the contained files
        if (parentDir != null) {
          if (existingData.startsWith(".")) { // handle . & ..
            prefixToUse = parentDir.getPath();
          } else if (prependAbsolute) {
            prefixToUse = parentDir.getAbsolutePath();
          }
        }
        // add File.separator in the end
        if (!prefixToUse.endsWith(File.separator) && (prependAbsolute || existingData.startsWith(".")) ) {
          prefixToUse += File.separator;
        }
        for (int i = 0; i < completionValues.length; i++) {
          completions.add(new Completion(prefixToUse+completionValues[i]));
        }
      }
    }
    
    return !completions.isEmpty();
  }
  
  class FileNameFilterImpl implements FilenameFilter {
    private File parentDirectory;
    private String userInput;
    
    public FileNameFilterImpl(File parentDirectory, String userInput) {
      this.parentDirectory = parentDirectory;
      this.userInput = userInput;
    }

    @Override
    public boolean accept(File dir, String name) {
      return parentDirectory.equals(dir) && name.startsWith(userInput);
    }
  }

}
