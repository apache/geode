/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.controllers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.gemstone.gemfire.internal.util.IOUtils;

import org.springframework.web.multipart.MultipartFile;

/**
 * The AbstractMultiPartCommandsController class is a abstract base class encapsulating all common functionality for
 * handling multi-part (file upload) HTTP requests.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @since 8.0
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
