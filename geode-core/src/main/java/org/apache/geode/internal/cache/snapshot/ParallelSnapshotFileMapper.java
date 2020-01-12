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
package org.apache.geode.internal.cache.snapshot;

import java.io.File;

import org.apache.geode.cache.snapshot.RegionSnapshotService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class ParallelSnapshotFileMapper implements SnapshotFileMapper {

  private static final long serialVersionUID = 1L;

  @Override
  public File mapExportPath(DistributedMember member, File snapshot) {
    String baseName = getBaseName(snapshot);
    String memberUniqueId = createUniqueId((InternalDistributedMember) member);
    String fullName =
        baseName + "-" + memberUniqueId + RegionSnapshotService.SNAPSHOT_FILE_EXTENSION;
    return new File(snapshot.getParentFile(), fullName);
  }


  @Override
  public File[] mapImportPath(DistributedMember member, File snapshot) {
    return new File[] {snapshot};
  }

  private String getBaseName(File snapshot) {
    String filename = snapshot.getName();
    int suffixLocation = filename.indexOf(RegionSnapshotService.SNAPSHOT_FILE_EXTENSION);
    if (suffixLocation < 0) {
      throw new IllegalArgumentException(
          "Snapshot file '" + filename + "' missing backup file extension (.gfd)");
    }
    return filename.substring(0, suffixLocation);
  }

  /**
   * Combines the ip address and port of a distributed member to create a unique identifier for the
   * member. As this string will be used in file names, the periods (ipv4) and colons (ipv6) are
   * stripped out.
   *
   * @param member the member to create a unique id for
   * @return a String based on the ip address and host of the member
   */
  private String createUniqueId(InternalDistributedMember member) {
    String address = member.getInetAddress().getHostAddress();
    String alphanumericAddress = address.replaceAll("\\.|:", "");
    int port = member.getMembershipPort();
    return alphanumericAddress + port;
  }
}
