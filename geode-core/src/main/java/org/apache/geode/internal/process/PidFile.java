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
package com.gemstone.gemfire.internal.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.internal.util.StopWatch;

/**
 * File wrapper that adds support for reading process id (pid) from a pid file
 * written to disk by GemFire processes.
 * 
 * @since GemFire 8.2
 */
public class PidFile {

  private static final long SLEEP_INTERVAL_MILLIS = 10;
  
  private final File pidFile;
  
  /**
   * Constructs a PidFile for reading pid stored in a file.
   * 
   * @param file the file containing the pid of the process
   * 
   * @throws FileNotFoundException if the specified file name is not found within the directory
   */
  public PidFile(final File file) throws FileNotFoundException {
    if (!file.exists() || !file.isFile()) {
      throw new FileNotFoundException("Unable to find PID file '" + file + "'");
    }
    this.pidFile = file;
  }

  File getFile() {
    return this.pidFile;
  }
  
  /**
   * Constructs a PidFile for reading pid stored in a file.
   * 
   * @param directory directory containing a file of name pidFileName
   * @param filename name of the file containing the pid of the process to stop
   * 
   * @throws FileNotFoundException if the specified file name is not found within the directory
   * @throws IllegalStateException if dir is not an existing directory
   */
  public PidFile(final File directory, final String filename) throws FileNotFoundException {
    if (!directory.isDirectory() && directory.exists()) {
      throw new IllegalArgumentException("Argument '" + directory + "' must be an existing directory!");
    }

    final File file = new File(directory, filename);
    if (!file.exists() || file.isDirectory()) {
      throw new FileNotFoundException("Unable to find PID file '" + filename + "' in directory " + directory);
    }

    this.pidFile = file;
  }

  /**
   * Reads in the pid from the specified file. 
   * 
   * @return the process id (pid) contained within the pidFile
   * 
   * @throws IllegalArgumentException if the pid in the pidFile is not a positive integer
   * @throws IOException if unable to read from the specified file
   */
  public int readPid() throws IOException {
    BufferedReader fileReader = null;
    String pidValue = null;

    try {
      fileReader = new BufferedReader(new FileReader(this.pidFile));
      pidValue = fileReader.readLine();

      final int pid = Integer.parseInt(pidValue);

      if (pid < 1) {
        throw new IllegalArgumentException("Invalid pid '" + pid + "' found in " + this.pidFile.getCanonicalPath());
      }

      return pid;
    }
    catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid pid '" + pidValue + "' found in " + this.pidFile.getCanonicalPath());
    }
    finally {
      IOUtils.close(fileReader);
    }
  }

  /**
   * Reads in the pid from the specified file, retrying until the specified timeout.
   * 
   * @param timeout the maximum time to spend trying to read the pidFile
   * @param unit the unit of timeout
   * 
   * @return the process id (pid) contained within the pidFile
   * 
   * @throws IllegalArgumentException if the pid in the pidFile is not a positive integer
   * @throws IOException if unable to read from the specified file
   * @throws InterruptedException if interrupted
   * @throws TimeoutException if operation times out
   */
  public int readPid(final long timeout, final TimeUnit unit) throws IOException, InterruptedException, TimeoutException {
    IllegalArgumentException iae = null;
    IOException ioe = null;
    int pid = 0;
    
    final long timeoutMillis = unit.toMillis(timeout);
    final StopWatch stopWatch = new StopWatch(true);
    
    while (pid <= 0) {
      try {
        pid = readPid();
      } catch (IllegalArgumentException e) {
        iae = e;
      } catch (IOException e) {
        ioe = e;
      }
      if (stopWatch.elapsedTimeMillis() > timeoutMillis) {
        if (iae != null) {
          throw new TimeoutException(iae.getMessage());
        }
        if (ioe != null) {
          throw new TimeoutException(ioe.getMessage());
        }
      } else {
        try {
          Thread.sleep(SLEEP_INTERVAL_MILLIS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          if (iae != null) {
            throw new InterruptedException(iae.getMessage());
          }
          if (ioe != null) {
            throw new InterruptedException(ioe.getMessage());
          }
          throw e;
        }
      }
    }
    return pid;
  }
}
