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
package org.apache.geode.internal.process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.util.StopWatch;
import org.apache.geode.logging.internal.LogService;

/**
 * Reads the InputStream per-byte instead of per-line. Uses BufferedReader.ready() to ensure that
 * calls to read() will not block. Uses continueReadingMillis to continue reading after the Process
 * terminates in order to fully read the last of that Process' output (such as a stack trace).
 *
 * @since GemFire 8.2
 */
class NonBlockingProcessStreamReader extends ProcessStreamReader {
  private static final Logger logger = LogService.getLogger();

  /**
   * millis to continue reading after Process terminates in order to fully read the last of its
   * output
   */
  private final long continueReadingMillis;

  private final StopWatch continueReading;

  private StringBuilder stringBuilder;
  private int character;
  private boolean ready;

  NonBlockingProcessStreamReader(final Builder builder) {
    super(builder);

    this.continueReadingMillis = builder.continueReadingMillis;
    this.continueReading = new StopWatch();
    this.stringBuilder = new StringBuilder();
    this.character = 0;
    this.ready = false;
  }

  @Override
  public void run() {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      while (character != -1) {
        readWhileReady(reader);
        if (shouldTerminate()) {
          break;
        }
      }
    } catch (IOException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Failure reading from buffered input stream: {}", e.getMessage(), e);
      }
    } catch (InterruptedException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Interrupted reading from buffered input stream: {}", e.getMessage(), e);
      }
    }
  }

  private boolean shouldTerminate() throws InterruptedException {
    if (!ProcessUtils.isProcessAlive(process)) {
      if (!continueReading.isRunning()) {
        continueReading.start();
      } else if (continueReading.elapsedTimeMillis() > continueReadingMillis) {
        return true;
      }
    }
    Thread.sleep(10);
    return false;
  }

  /**
   * This is a hot reader while there are characters ready to read. As soon as there are no more
   * characters to read, it returns and the loop invokes shouldTerminate which has a 10 millisecond
   * sleep until there are more characters ready to read.
   */
  private void readWhileReady(BufferedReader reader) throws IOException {
    while ((ready = reader.ready()) && (character = reader.read()) != -1) {
      stringBuilder.append((char) character);
      if ((char) character == '\n') {
        this.inputListener.notifyInputLine(stringBuilder.toString());
        stringBuilder = new StringBuilder();
      }
    }
  }
}
