/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.io;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.springframework.web.multipart.MultipartFile;

/**
 * The MultipartFileAdapter class is an Adapter built for extension in order to implement the MultipartFile interface
 * overriding behavior applicable to the implementation.
 * <p/>
 * @author John Blum
 * @see org.springframework.web.multipart.MultipartFile
 * @since 8.0
 */
@SuppressWarnings("unused")
public class MultipartFileAdapter implements MultipartFile {

  public byte[] getBytes() throws IOException {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  public String getContentType() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  public boolean isEmpty() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  public InputStream getInputStream() throws IOException {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  public String getName() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  public String getOriginalFilename() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  public long getSize() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  public void transferTo(final File dest) throws IOException, IllegalStateException {
    throw new UnsupportedOperationException("Not Implemented!");
  }

}
