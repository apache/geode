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

package org.apache.geode.management.internal.beans;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.healthmarketscience.rmiio.RemoteOutputStream;
import com.healthmarketscience.rmiio.RemoteOutputStreamMonitor;
import com.healthmarketscience.rmiio.RemoteOutputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteOutputStream;
import com.healthmarketscience.rmiio.exporter.RemoteStreamExporter;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.GemFireSecurityException;

public class FileUploader implements FileUploaderMBean {
  public static final String STAGED_DIR_PREFIX = "uploaded-";
  private static final Logger logger = LogService.getLogger();
  private final RemoteStreamExporter exporter;

  public static class RemoteFile implements Serializable {
    private final String filename;
    private final RemoteOutputStream outputStream;

    public RemoteFile(String filename, RemoteOutputStream outputStream) {
      this.filename = filename;
      this.outputStream = outputStream;
    }

    public String getFilename() {
      return filename;
    }

    public RemoteOutputStream getOutputStream() {
      return outputStream;
    }
  }

  public FileUploader(RemoteStreamExporter exporter) {
    this.exporter = exporter;
  }

  @Override
  public RemoteFile uploadFile(String filename) throws IOException {
    Path tempDir = createSecuredTempDirectory(STAGED_DIR_PREFIX);

    File stagedFile = new File(tempDir.toString(), filename);
    BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(stagedFile));

    RemoteOutputStreamMonitor monitor = new RemoteOutputStreamMonitor() {
      @Override
      public void closed(RemoteOutputStreamServer stream, boolean clean) {
        try {
          stream.close(true);
        } catch (IOException e) {
          logger.error("error closing RemoteOutputStreamServer", e);
        }
      }
    };

    RemoteOutputStreamServer server = new SimpleRemoteOutputStream(bos, monitor);
    RemoteOutputStream remoteStream = exporter.export(server);

    RemoteFile remoteFile = new RemoteFile(stagedFile.getAbsolutePath(), remoteStream);

    return remoteFile;
  }

  @Override
  public void deleteFiles(List<String> files) {
    if (files == null || files.isEmpty()) {
      return;
    }

    for (String filename : files) {
      File file = new File(filename);
      File parent = file.getParentFile();

      if (!parent.getName().startsWith(STAGED_DIR_PREFIX)) {
        throw new GemFireSecurityException(
            String.format("Cannot delete %s, not in the uploaded directory.", filename));
      }

      FileUtils.deleteQuietly(file);
      FileUtils.deleteQuietly(parent);
    }
  }

  public static Path createSecuredTempDirectory(String prefix) throws IOException {
    Path tempDir = Files.createTempDirectory(prefix);
    tempDir.toFile().setExecutable(true, true);
    tempDir.toFile().setWritable(true, true);
    tempDir.toFile().setReadable(true, true);

    return tempDir;
  }
}
