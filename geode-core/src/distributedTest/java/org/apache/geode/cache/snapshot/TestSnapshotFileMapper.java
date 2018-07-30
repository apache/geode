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
    File directory = new File(snapshot.getParent(), Integer.toString(1 + VM.getCurrentVMNum()));
    directory.mkdirs();
    return new File(directory, mapFilename(snapshot));
  }

  @Override
  public File[] mapImportPath(DistributedMember member, File snapshot) {
    if (shouldExplode) {
      throw new RuntimeException();
    }
    File directory = new File(snapshot, Integer.toString(1 + VM.getCurrentVMNum()));
    return new File[] {directory};
  }

  private String mapFilename(File snapshot) {
    return snapshot.getName();
  }
}
