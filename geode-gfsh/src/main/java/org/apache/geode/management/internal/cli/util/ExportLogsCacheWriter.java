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
 *
 */

package org.apache.geode.management.internal.cli.util;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.util.CacheWriterAdapter;

public class ExportLogsCacheWriter extends CacheWriterAdapter<String, byte[]>
    implements Serializable {
  private Path currentFile;
  private boolean isEmpty = true;
  private BufferedOutputStream currentOutputStream;

  @Override
  public void beforeCreate(EntryEvent<String, byte[]> event) throws CacheWriterException {
    if (currentOutputStream == null) {
      throw new IllegalStateException(
          "No outputStream is open.  You must call startFile before sending data.");
    }

    try {
      currentOutputStream.write(event.getNewValue());
      isEmpty = false;
    } catch (IOException e) {
      throw new CacheWriterException(e);
    }
  }

  public void startFile(String memberId) throws IOException {
    if (currentFile != null || currentOutputStream != null) {
      throw new IllegalStateException("Cannot open more than one file at once");
    }

    currentFile = Files.createTempDirectory(memberId).resolve(memberId + ".zip");
    currentOutputStream = new BufferedOutputStream(new FileOutputStream(currentFile.toFile()));
    isEmpty = true;
  }

  public Path endFile() {
    Path completedFile = currentFile;

    try {
      if (null != currentOutputStream) {
        currentOutputStream.close();
      }
    } catch (IOException ignore) {
    }

    currentOutputStream = null;
    currentFile = null;
    if (isEmpty)
      return null;
    return completedFile;
  }
}
