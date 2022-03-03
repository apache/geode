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

package org.apache.geode.management.runtime;

import java.util.Objects;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.JsonSerializable;

@Experimental
public class RuntimeRegionInfo extends RuntimeInfo implements JsonSerializable {
  private long entryCount;

  public long getEntryCount() {
    return entryCount;
  }

  public void setEntryCount(long entrySize) {
    entryCount = entrySize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RuntimeRegionInfo)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RuntimeRegionInfo that = (RuntimeRegionInfo) o;
    return getEntryCount() == that.getEntryCount();
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getEntryCount());
  }

  @Override
  public String toString() {
    return "RuntimeRegionInfo{" +
        "entryCount=" + entryCount +
        "} " + super.toString();
  }
}
