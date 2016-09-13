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
package org.apache.geode.management.internal.cli.util;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;

import org.apache.geode.internal.logging.MergeLogFiles;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

/**
 * 
 * @since GemFire 7.0
 */
 
public class MergeLogs {
/**
   * @param args
   */

  public static void main(String[] args) {
    if (args.length < 1 || args.length > 1) {
      throw new IllegalArgumentException(
          "Requires only 1  arguments : <targetDirName>");
    }
    try{
      String result = mergeLogFile(args[0]);
      System.out.println(result);
    }catch(Exception e){
      System.out.println(e.getMessage());
    }
    

  }
  
  static String mergeLogFile(String dirName) throws Exception{    
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");    
    File dir = new File(dirName);
    String[] logsToMerge = dir.list();
    InputStream[] logFiles = new FileInputStream[logsToMerge.length];
    String[] logFileNames = new String[logFiles.length];
    for (int i = 0; i < logsToMerge.length; i++) {
      try {
        logFiles[i] = new FileInputStream(dirName +File.separator+ logsToMerge[i]);
        logFileNames[i] =dirName +File.separator+ logsToMerge[i];        
      } catch (FileNotFoundException e) {        
        throw new Exception(logsToMerge[i] + " "+CliStrings.EXPORT_LOGS__MSG__FILE_DOES_NOT_EXIST);
      }      
    }
    
    PrintWriter mergedLog = null;
    try {
      String mergeLog = dirName+File.separator + "merge_"+sdf.format(new java.util.Date())+".log";
      mergedLog = new PrintWriter(mergeLog);
      boolean flag = MergeLogFiles.mergeLogFiles(logFiles, logFileNames, mergedLog);      
    } catch (FileNotFoundException e) {      
      throw new Exception("FileNotFoundException in creating PrintWriter in MergeLogFiles"+e.getMessage());
    } catch (Exception e) {      
      throw new Exception("Exception in creating PrintWriter in MergeLogFiles"+e.getMessage());
    }
       
    return "Sucessfully merged logs";
  }
  

}
