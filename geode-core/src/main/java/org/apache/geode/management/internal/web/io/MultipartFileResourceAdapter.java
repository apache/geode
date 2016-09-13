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
package com.gemstone.gemfire.management.internal.web.io;

import java.io.IOException;
import java.io.InputStream;

import org.springframework.core.io.AbstractResource;
import org.springframework.web.multipart.MultipartFile;

/**
 * The MultipartFileResourceAdapter class is an Adapter for adapting the MultipartFile interface into an instance of
 * the Resource interface in the context where a Resource object is required instead.
 * <p/>
 * @see org.springframework.core.io.AbstractResource
 * @see org.springframework.core.io.Resource
 * @see org.springframework.web.multipart.MultipartFile
 * @since GemFire 8.0
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
