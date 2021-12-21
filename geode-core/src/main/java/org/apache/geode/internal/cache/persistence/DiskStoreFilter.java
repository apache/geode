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
package org.apache.geode.internal.cache.persistence;

import java.io.File;
import java.io.FilenameFilter;

import org.apache.geode.internal.cache.Oplog;

public class DiskStoreFilter implements FilenameFilter {

  private final String filterCondn;
  private final boolean includeKRF;

  public DiskStoreFilter(OplogType type, boolean includeKRF, String name) {
    this.includeKRF = includeKRF;
    filterCondn = new StringBuffer(type.getPrefix()).append(name).toString();
  }

  private boolean selected(String fileName) {
    if (includeKRF) {
      return (fileName.endsWith(Oplog.CRF_FILE_EXT) || fileName.endsWith(Oplog.KRF_FILE_EXT)
          || fileName.endsWith(Oplog.DRF_FILE_EXT));

    } else {
      return (fileName.endsWith(Oplog.CRF_FILE_EXT) || fileName.endsWith(Oplog.DRF_FILE_EXT));
    }
  }

  @Override
  public boolean accept(File f, String fileName) {
    if (selected(fileName)) {
      int positionOfLastDot = fileName.lastIndexOf('_');
      String filePathWithoutNumberAndExtension = fileName.substring(0, positionOfLastDot);
      boolean result = filterCondn.equals(filePathWithoutNumberAndExtension);
      return result;
    } else {
      return false;
    }
  }
}
