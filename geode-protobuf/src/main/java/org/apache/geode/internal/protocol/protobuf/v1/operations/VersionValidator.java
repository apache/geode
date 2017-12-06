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
package org.apache.geode.internal.protocol.protobuf.v1.operations;

import org.apache.geode.internal.protocol.protobuf.v1.ConnectionAPI;

public class VersionValidator {
  private int majorVersion;
  private int minorVersion;

  public VersionValidator() {
    this(ConnectionAPI.MajorVersions.CURRENT_MAJOR_VERSION_VALUE,
        ConnectionAPI.MinorVersions.CURRENT_MINOR_VERSION_VALUE);
  }

  VersionValidator(int majorVersion, int minorVersion) {
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
  }

  public boolean isValid(int majorVersion, int minorVersion) {
    if (majorVersion != ConnectionAPI.MajorVersions.INVALID_MAJOR_VERSION_VALUE
        && majorVersion == this.majorVersion) {
      if (minorVersion != ConnectionAPI.MinorVersions.INVALID_MINOR_VERSION_VALUE
          && minorVersion <= this.minorVersion) {
        return true;
      }
    }
    return false;
  }
}
