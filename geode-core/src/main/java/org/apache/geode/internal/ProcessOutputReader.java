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

package org.apache.geode.internal;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


/**
 * A ProcessOutputReader will read both stdout and stderr from a {@link java.lang.Process} process.
 * The constructor does not return until all output from the process has been read and the process
 * has exited.
 *
 *
 */
public class ProcessOutputReader {
  private int exitCode;
  private String output;

  /**
   * Creates a process output reader for the given process.
   *
   * @param p the process whose output should be read.
   */
  public ProcessOutputReader(final Process p) {
    final List lines = Collections.synchronizedList(new ArrayList());

    class ProcessStreamReader extends Thread {
      private BufferedReader reader;
      public int linecount = 0;

      public ProcessStreamReader(InputStream stream) {
        reader = new BufferedReader(new InputStreamReader(stream));
      }

      @Override
      public void run() {
        try {
          String line;
          while ((line = reader.readLine()) != null) {
            linecount++;
            lines.add(line);
          }
          reader.close();
        } catch (Exception e) {
        }
      }
    };

    ProcessStreamReader stdout = new ProcessStreamReader(p.getInputStream());
    ProcessStreamReader stderr = new ProcessStreamReader(p.getErrorStream());
    stdout.start();
    stderr.start();
    try {
      stderr.join();
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
    }
    try {
      stdout.join();
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
    }
    this.exitCode = 0;
    int retryCount = 9;
    while (retryCount > 0) {
      retryCount--;
      try {
        exitCode = p.exitValue();
        break;
      } catch (IllegalThreadStateException e) {
        // due to bugs in Process we may not be able to get
        // a process's exit value.
        // We can't use Process.waitFor() because it can hang forever
        if (retryCount == 0) {
          if (stderr.linecount > 0) {
            // The process wrote to stderr so manufacture
            // an error exist code
            lines.add(
                "Failed to get exit status and it wrote to stderr so setting exit status to 1.");
            exitCode = 1;
          }
        } else {
          // We need to wait around to give a chance for
          // the child to be reaped.See bug 19682
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
            // TODO a cancellation check here?
          }
        }
      }
    }

    java.io.StringWriter sw = new java.io.StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    Iterator it = lines.iterator();
    while (it.hasNext()) {
      pw.println((String) it.next());
    }
    pw.close();
    try {
      sw.close();
    } catch (java.io.IOException ignore) {
    }
    StringBuffer buf = sw.getBuffer();
    if (buf != null && buf.length() > 0) {
      this.output = sw.toString();
    } else {
      this.output = "";
    }
  }

  /**
   * Gets the process's exit status code. A code equal to 0 indicates all is well.
   */
  public int getExitCode() {
    return exitCode;
  }

  /**
   * Gets everything the process wrote to both stdout and stderr.
   */
  public String getOutput() {
    return output;
  }
}
