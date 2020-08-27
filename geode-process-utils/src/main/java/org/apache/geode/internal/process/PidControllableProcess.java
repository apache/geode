/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.process;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.geode.internal.process.ControlFileWatchdog.ControlRequestHandler;
import org.apache.geode.launcher.ServerLauncherConfig.ServiceInfo;

public class PidControllableProcess implements ControllableProcess {
  private final File directory;
  private final LocalProcessLauncher launcher;
  // private final ControlFileWatchdog stopRequestFileWatchdog;
  private final ControlFileWatchdog statusRequestFileWatchdog;

  public PidControllableProcess(File directory, ProcessType type, boolean forcing,
      ControlNotificationHandler<ServiceInfo> statusHandler)
      throws IOException, PidUnavailableException, FileAlreadyExistsException {
    // if (!directory.exists() && !directory.createNewFile()) {
    // throw new IOException(
    // "Unable to create working directory '" + directory.getCanonicalPath() + "'");
    // }
    this.directory = directory;
    launcher = new LocalProcessLauncher(new File(directory, type.getPidFileName()), forcing);
    // stopRequestFileWatchdog = new ControlFileWatchdog(directory, type.getPidFileName(),
    // stopHandler, false);
    statusRequestFileWatchdog = createStatusRequestFileWatchdog(directory, type,
        createStatusHandler(statusHandler, directory, type));
    statusRequestFileWatchdog.start();
  }

  static String fetchStatusWithValidation(final ControlNotificationHandler<ServiceInfo> handler) {
    ServiceInfo state = handler.handleStatus();
    if (state == null) {
      throw new IllegalStateException("Null ServiceState is invalid");
    }

    String jsonContent = state.toJson();
    if (jsonContent == null) {
      throw new IllegalStateException("Null JSON for status is invalid");
    } else if (jsonContent.trim().isEmpty()) {
      throw new IllegalStateException("Empty JSON for status is invalid");
    }

    return jsonContent;
  }

  private static void deleteFileWithValidation(final File file, final String fileNameForMessage)
      throws IOException {
    if (file.exists()) {
      if (!file.delete()) {
        throw new IOException(
            "Unable to delete " + fileNameForMessage + "'" + file.getCanonicalPath() + "'");
      }
    }
  }

  private static ControlFileWatchdog createStatusRequestFileWatchdog(final File directory,
      final ProcessType processType,
      final ControlFileWatchdog.ControlRequestHandler statusHandler) {
    return new ControlFileWatchdog(directory, processType.getStatusRequestFileName(), statusHandler,
        false);
  }

  private static ControlRequestHandler createStatusHandler(
      final ControlNotificationHandler<ServiceInfo> handler,
      final File directory, final ProcessType processType) {
    return () -> {
      writeStatusToFile(fetchStatusWithValidation(handler), directory, processType);
    };
  }

  private static void writeStatusToFile(final String jsonContent, final File directory,
      final ProcessType processType) throws IOException {
    File statusFile = new File(directory, processType.getStatusFileName());
    File statusFileTmp = new File(directory, processType.getStatusFileName() + ".tmp");

    deleteFileWithValidation(statusFile, "statusFile");
    deleteFileWithValidation(statusFileTmp, "statusFileTmp");

    if (!statusFileTmp.createNewFile()) {
      throw new IOException(
          "Unable to create statusFileTmp '" + statusFileTmp.getCanonicalPath() + "'");
    }

    FileWriter writer = new FileWriter(statusFileTmp);
    writer.write(jsonContent);
    writer.flush();
    writer.close();

    if (!statusFileTmp.renameTo(statusFile)) {
      throw new IOException("Unable to rename statusFileTmp '" + statusFileTmp.getCanonicalPath()
          + "' to '" + statusFile.getCanonicalPath() + "'");
    }
  }

  @Override
  public int getPid() {
    return launcher.getPid();
  }

  @Override
  public File getPidFile() {
    return launcher.getPidFile();
  }

  @Override
  public File getDirectory() {
    return directory;
  }

  @Override
  public void stop() {
    try {
      statusRequestFileWatchdog.stop();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    launcher.close();
  }

  @Override
  public void stop(boolean deletePidFileOnStop) {
    launcher.close(deletePidFileOnStop);
  }
}
