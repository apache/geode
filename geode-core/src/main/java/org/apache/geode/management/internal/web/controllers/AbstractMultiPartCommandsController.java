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
package org.apache.geode.management.internal.web.controllers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.util.IOUtils;

import org.springframework.web.multipart.MultipartFile;

/**
 * The AbstractMultiPartCommandsController class is a abstract base class encapsulating all common functionality for
 * handling multi-part (file upload) HTTP requests.
 * <p/>
 * @see org.apache.geode.management.internal.web.controllers.AbstractCommandsController
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public class AbstractMultiPartCommandsController extends AbstractCommandsController {

  protected static final String RESOURCES_REQUEST_PARAMETER = "resources";

  /**
   * Saves an array of File objects to this system's file system.
   * <p/>
   * @param files an array of MultipartFile objects to persist to the file system.
   * @throws IOException if I/O error occurs while saving the Files to the file system.
   * @see org.springframework.web.multipart.MultipartFile
   */
  protected static void save(final MultipartFile... files) throws IOException {
    if (files != null) {
      for (final MultipartFile file : files) {
        save(file);
      }
    }
  }

  /**
   * Saves a multi-part File to this system's file system.
   * <p/>
   * @param file the MultipartFile object to persist to the file system.
   * @throws IOException if I/O error occurs while saving the File to the file system.
   * @see org.springframework.web.multipart.MultipartFile
   */
  protected static void save(final MultipartFile file) throws IOException {
    final File saveFile = new File(SystemUtils.CURRENT_DIRECTORY, file.getOriginalFilename());

    FileOutputStream fileWriter = null;

    try {
      fileWriter = new FileOutputStream(saveFile, false);
      fileWriter.write(file.getBytes());
      fileWriter.flush();
    }
    finally {
      IOUtils.close(fileWriter);
    }
  }

}
