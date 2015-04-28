/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.io;

import java.io.IOException;
import java.io.InputStream;

import org.springframework.core.io.AbstractResource;
import org.springframework.web.multipart.MultipartFile;

/**
 * The MultipartFileResourceAdapter class is an Adapter for adapting the MultipartFile interface into an instance of
 * the Resource interface in the context where a Resource object is required instead.
 * <p/>
 * @author John Blum
 * @see org.springframework.core.io.AbstractResource
 * @see org.springframework.core.io.Resource
 * @see org.springframework.web.multipart.MultipartFile
 * @since 8.0
 */
@SuppressWarnings("unused")
public class MultipartFileResourceAdapter extends AbstractResource {

  private final MultipartFile file;

  public MultipartFileResourceAdapter(final MultipartFile file) {
    assert file != null : "The multi-part file to adapt as a resource cannot be null!";
    this.file = file;
  }

  protected final MultipartFile getMultipartFile() {
    return file;
  }

  @Override
  public long contentLength() throws IOException {
    return getMultipartFile().getSize();
  }

  @Override
  public String getDescription() {
    return getMultipartFile().getName();
  }

  @Override
  public String getFilename() {
    return getMultipartFile().getOriginalFilename();
  }

  @Override public InputStream getInputStream() throws IOException {
    return getMultipartFile().getInputStream();
  }

}
