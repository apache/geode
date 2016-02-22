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
package com.gemstone.gemfire.test.process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Queue;

/**
 * Reads the output from a process stream and stores it for test validation. 
 * </p>
 * Extracted from ProcessWrapper.
 * 
 */
public class ProcessStreamReader extends Thread {
  
  private volatile RuntimeException startStack;
  
  private final String command;
  private final BufferedReader reader;
  private final Queue<String> lineBuffer;
  private final List<String> allLines;

  public int linecount = 0;

  public ProcessStreamReader(final String command, final InputStream stream, final Queue<String> lineBuffer, final List<String> allLines) {
    this.command = command;
    this.reader = new BufferedReader(new InputStreamReader(stream));
    this.lineBuffer = lineBuffer;
    this.allLines = allLines;
  }

  @Override
  public void start() {
    this.startStack = new RuntimeException(this.command);
    super.start();
  }
  
  @Override
  public void run() {
    try {
      String line;
      while ((line = this.reader.readLine()) != null) {
        this.linecount++;
        this.lineBuffer.offer(line);
        this.allLines.add(line);
      }

      // EOF
      this.reader.close();
    } catch (IOException streamClosed) {
      this.startStack.initCause(streamClosed);
      throw this.startStack;
    }
  }
}
