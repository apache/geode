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
package org.apache.geode.internal.process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;

/**
 * Reads the output stream of a Process. Default implementation performs blocking 
 * per-line reading of the InputStream.
 * 
 * Extracted from ProcessStreamReader.
 * 
 * @since GemFire 8.2
 */
public final class BlockingProcessStreamReader extends ProcessStreamReader {
  private static final Logger logger = LogService.getLogger();

  protected BlockingProcessStreamReader(final Builder builder) {
    super(builder);
  }

  @Override
  public void run() {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("Running {}", this);
    }
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(inputStream));
      String line;
      while ((line = reader.readLine()) != null) {
        this.inputListener.notifyInputLine(line);
      }
    } catch (IOException e) {
      if (isDebugEnabled) {
        logger.debug("Failure reading from buffered input stream: {}", e.getMessage(), e);
      }
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        if (isDebugEnabled) {
          logger.debug("Failure closing buffered input stream reader: {}", e.getMessage(), e);
        }
      }
      if (isDebugEnabled) {
        logger.debug("Terminating {}", this);
      }
    }
  }
}
