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
package org.apache.geode.cache.snapshot;

import java.io.File;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.snapshot.SnapshotFileMapper;
import org.apache.geode.test.dunit.VM;

public class TestSnapshotFileMapper implements SnapshotFileMapper {
  private volatile boolean shouldExplode;

  public void setShouldExplode(boolean shouldExplode) {
    this.shouldExplode = shouldExplode;
  }

  @Override
  public File mapExportPath(DistributedMember member, File snapshot) {
    if (shouldExplode) {
      throw new RuntimeException();
    }
    return new File(snapshot.getParentFile(), mapFilename(snapshot));
  }

  @Override
  public File[] mapImportPath(DistributedMember member, File snapshot) {
    if (shouldExplode) {
      throw new RuntimeException();
    }

    File f = new File(snapshot.getParentFile(), mapFilename(snapshot));
    return new File[] {f};
  }

  private String mapFilename(File snapshot) {
    String filename = snapshot.getName();
    int suffixLocation = filename.indexOf(RegionSnapshotService.SNAPSHOT_FILE_EXTENSION);
    return filename.substring(0, suffixLocation) + "-" + VM.getCurrentVMNum()
        + RegionSnapshotService.SNAPSHOT_FILE_EXTENSION;
  }
}
