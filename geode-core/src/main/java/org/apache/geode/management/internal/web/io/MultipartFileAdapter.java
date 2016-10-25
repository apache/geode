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
package org.apache.geode.management.internal.web.io;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.springframework.web.multipart.MultipartFile;

/**
 * The MultipartFileAdapter class is an Adapter built for extension in order to implement the MultipartFile interface
 * overriding behavior applicable to the implementation.
 * <p/>
 * @see org.springframework.web.multipart.MultipartFile
 * @since GemFire 8.0
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
