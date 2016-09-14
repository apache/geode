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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.management.internal.cli.GfshParser;

/**
 * 
 * 
 * @since GemFire 7.0
 */
public class ReadWriteFile {

  /**
   * @param args
   */
  public static void main(String[] args) {
    if (args.length < 6 || args.length > 6) {
      throw new IllegalArgumentException(
          "Requires only 6  arguments : <logInputFileName> <logOutputFileName> <LogLevel> <UptoLogLevel> <StartTime> <EndTime>");
    }
    String result = readWriteFile(args[0], args[1], args[2], args[3], args[4],
        args[5]);
    System.out.println(result);
  }

  public static String readWriteFile(String logFileName, String logToBeWritten,
      String logLevel, String onlyLogLevel, String startTime, String endTime) {
    try {
      long lineCount = 0;
      BufferedReader input = null;
      BufferedWriter output = null;
      File logFileNameFile = new File(logFileName);
      if(logFileNameFile.canRead() == false){
        return ("Cannot read logFileName="+logFileName);
      }
      input = new BufferedReader(new FileReader(logFileName));      
      String line = null;
      File logToBeWrittenToFile = new File(logToBeWritten);
      output = new BufferedWriter(new FileWriter(logToBeWrittenToFile));
      if (!logToBeWrittenToFile.exists()) {
        if (input != null) {
          input.close();
        }
        if (output != null) {
          output.flush();
          output.close();
        }        
        return (logToBeWritten + " doesn not exist");
      }
      if (!logToBeWrittenToFile.isFile()) {
        if (input != null) {
          input.close();
        }
        if (output != null) {
          output.flush();
          output.close();
        }
        return (logToBeWritten + " is not a file");
      }
      if (!logToBeWrittenToFile.canWrite()) {
        if (input != null) {
          input.close();
        }
        if (output != null) {
          output.flush();
          output.close();
        }
        return ("can not write file " + logToBeWritten);
      }

      // build possible log levels based on user input
      // get all the levels below the one mentioned by user
      List<String> logLevels = new ArrayList<String>();
      if (onlyLogLevel.toLowerCase().equals("false")) {
        int[] intLogLevels = LogWriterImpl.allLevels;
        for (int level : intLogLevels) {
          if (level >= LogWriterImpl.levelNameToCode(logLevel)) {
            logLevels.add(LogWriterImpl.levelToString(level).toLowerCase());
          }
        }
      } else {
        logLevels.add(logLevel);
      }     
      boolean timeRangeCheck = false;
      boolean foundLogLevelTag = false;
      boolean validateLogLevel = true;
      while ( input.ready() == true && (line = input.readLine()) != null ) {
        if(new File(logFileName).canRead() == false ){
          return ("Cannot read logFileName=" + logFileName); 
        }
        // boolean validateLogLevel = true;
        lineCount++;
        if (line.startsWith("[")) {          
          foundLogLevelTag = true;
        } else {
          foundLogLevelTag = false;
        }
        if (line.contains("[info ") && timeRangeCheck == false) {
          String stTime = "";
          int spaceCounter = 0;
          for(int i=line.indexOf("[info ")+6; i < line.length(); i++ ){
            if(line.charAt(i) == ' ' ){
              spaceCounter++;
            }
            if(spaceCounter > 2){
              break;
            }
            stTime = stTime+line.charAt(i);
          }
          SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
          Date d = df.parse(stTime.substring(0, stTime.length() - 4));
          Time fileStartTime = new Time(d.getTime());
          File inputFile = new File(logFileName);
          Time fileEndTime = new Time(inputFile.lastModified());
          Time longStart = new Time(Long.valueOf(startTime));
          Time longEnd = new Time(Long.valueOf(endTime));
          long userStartTime = longStart.getTime();
          long userEndTime = longEnd.getTime();
          if ((fileStartTime.getTime() >= userStartTime && fileStartTime
              .getTime() <= userEndTime)
              || (fileEndTime.getTime() >= userStartTime && fileEndTime
                  .getTime() <= userEndTime)
              || (fileStartTime.getTime() >= userStartTime
                  && fileStartTime.getTime() <= userEndTime
                  && fileEndTime.getTime() >= userStartTime && fileEndTime
                  .getTime() <= userEndTime)) {
            // set this so that no need to check time range for each line
            timeRangeCheck = true;
          } else {
            // dont take this log file as this does not fit in time range
            break;
          }
        }

        if (foundLogLevelTag == true) {
          validateLogLevel = checkLogLevel(line, logLevel, logLevels,
              foundLogLevelTag);
        }

        if (validateLogLevel) {
          output.append(line);
          output.newLine();
          // write 1000 lines and then save , this will reduce disk writing
          // frequency and at the same time will keep buffer under control
          if ((lineCount % 1000) == 0) {
            output.flush();
          }
        }
      }
      if (input != null) {
        input.close();
      }
      if (output != null) {
        output.flush();
        output.close();
      }
      return ("Sucessfully written file " + logFileName);
    } catch (FunctionException ex) {
      return ("readWriteFile FunctionException " + ex.getMessage());
    } catch (FileNotFoundException ex) {
      return ("readWriteFile FileNotFoundException " + ex.getMessage());
    } catch (IOException ex) {
      return ("readWriteFile FileNotFoundException " + ex.getMessage());
    } catch (Exception ex) {
      return ("readWriteFile Exception " + ex.getMessage());
    }
  }

  static boolean checkLogLevel(String line, String logLevel,
      List<String> logLevels, boolean foundLogLevelTag) {
    if (line == null) {
      return false;
    } else if (line != null && foundLogLevelTag == true) {
      if (logLevel.toLowerCase().equals("all")) {
        return true;
      } else if (line.equals(GfshParser.LINE_SEPARATOR)) {
        return true;
      } else {
        if (logLevels.size() > 0) {
          for (String permittedLogLevel : logLevels) {
            int indexFrom = line.indexOf('[');
            int indexTo = line.indexOf(' ');
            if (indexFrom > -1 && indexTo > -1 && indexTo > indexFrom) {
              boolean flag = line.substring(indexFrom + 1, indexTo)
                  .toLowerCase().contains(permittedLogLevel);             
              if (flag == true) {
                return flag;
              }
            }
          }
          return false;

        } else {
          return true;
        }
      }
    } else {
      return true;
    }
  }
}
