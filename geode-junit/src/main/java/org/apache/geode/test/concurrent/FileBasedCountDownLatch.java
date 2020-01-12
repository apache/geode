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
package org.apache.geode.test.concurrent;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;

import com.google.common.base.Charsets;
import org.apache.commons.io.FileUtils;

import org.apache.geode.test.awaitility.GeodeAwaitility;

/**
 * This is an implementation of CountDownLatch that can be serialized and used across multiple DUnit
 * VMs. File locks are used to synchronize between the separate VMs. For an example usage, see
 * ShowDeadlockDUnitTest.
 */
public class FileBasedCountDownLatch implements Serializable {
  private final File lockFile;
  private final File dataFile;

  public FileBasedCountDownLatch(int count) throws IOException {
    lockFile = File.createTempFile("CountDownLatchLock", ".txt");
    dataFile = File.createTempFile("CountDownLatchData", ".txt");

    try (FileOutputStream out = new FileOutputStream(lockFile)) {
      java.nio.channels.FileLock lock = out.getChannel().lock();
      try {
        FileUtils.writeStringToFile(dataFile, String.valueOf(count), Charsets.UTF_8);
      } finally {
        lock.release();
      }
    }

    lockFile.deleteOnExit();
  }

  public void countDown() throws IOException {
    try (FileOutputStream out = new FileOutputStream(lockFile)) {
      java.nio.channels.FileLock lock = out.getChannel().lock();

      try {
        String fileContents = FileUtils.readFileToString(dataFile, Charsets.UTF_8);
        int currentValue = Integer.valueOf(fileContents);

        int newValue = currentValue - 1;
        FileUtils.writeStringToFile(dataFile, String.valueOf(newValue), Charsets.UTF_8);

      } finally {
        lock.release();
      }
    }
  }

  public void await() throws IOException {
    GeodeAwaitility.await().until(this::currentValue, is(equalTo(0)));
  }

  protected int currentValue() throws IOException {
    try (FileOutputStream out = new FileOutputStream(lockFile)) {
      java.nio.channels.FileLock lock = out.getChannel().lock();
      try {
        String fileContents = FileUtils.readFileToString(dataFile, Charsets.UTF_8);
        return Integer.valueOf(fileContents);
      } finally {
        lock.release();
      }
    }
  }
}
