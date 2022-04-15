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
package org.apache.geode.internal.statistics;

/**
 * Descriptor containing all of the parameters required to construct a new instance of a
 * {@link StatArchiveWriter}. This describes the statistics archive.
 * <p>
 * This is a constructor parameter object for {@link StatArchiveWriter}.
 * <p>
 * {@link StatArchiveDescriptor.Builder} is used for constructing instances instead of a constructor
 * with many similar parameters (ie, multiple Strings which could easily be interposed with one
 * another).
 *
 * @since GemFire 7.0
 */
public class StatArchiveDescriptor {

  private final String archiveName;
  private final long systemId;
  private final long systemStartTime;
  private final String systemDirectoryPath;
  private final String productDescription;

  public String getArchiveName() {
    return archiveName;
  }

  public long getSystemId() {
    return systemId;
  }

  public long getSystemStartTime() {
    return systemStartTime;
  }

  public String getSystemDirectoryPath() {
    return systemDirectoryPath;
  }

  public String getProductDescription() {
    return productDescription;
  }

  @Override
  public String toString() {
    return getClass().getName() + "@" + System.identityHashCode(this) + "{"
        + "archiveName=" + archiveName
        + ", systemId=" + systemId
        + ", systemStartTime=" + systemStartTime
        + ", systemDirectoryPath=" + systemDirectoryPath
        + ", productDescription=" + productDescription
        + "}";
  }

  public static class Builder {
    private String archiveName;
    private long systemId = -1;
    private long systemStartTime = -1;
    private String systemDirectoryPath;
    private String productDescription;

    public Builder setArchiveName(String archiveName) {
      this.archiveName = archiveName;
      return this;
    }

    public Builder setSystemId(long systemId) {
      this.systemId = systemId;
      return this;
    }

    public Builder setSystemStartTime(long systemStartTime) {
      this.systemStartTime = systemStartTime;
      return this;
    }

    public Builder setSystemDirectoryPath(String systemDirectoryPath) {
      this.systemDirectoryPath = systemDirectoryPath;
      return this;
    }

    public Builder setProductDescription(String productDescription) {
      this.productDescription = productDescription;
      return this;
    }

    public StatArchiveDescriptor build() throws IllegalStateException {
      if (archiveName == null) {
        throw new IllegalStateException("archiveName must not be null");
      }
      if (systemId == -1) {
        throw new IllegalStateException("systemId must be a postive number");
      }
      if (systemStartTime == -1) {
        throw new IllegalStateException("systemStartTime must be a postive number");
      }
      if (systemDirectoryPath == null) {
        throw new IllegalStateException("systemDirectoryPath must not be null");
      }
      if (productDescription == null) {
        throw new IllegalStateException("productDescription must not be null");
      }
      return new StatArchiveDescriptor(this);
    }
  }

  private StatArchiveDescriptor(Builder builder) {
    archiveName = builder.archiveName;
    systemId = builder.systemId;
    systemStartTime = builder.systemStartTime;
    systemDirectoryPath = builder.systemDirectoryPath;
    productDescription = builder.productDescription;
  }
}
