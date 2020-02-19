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

public class DeploymentInfo extends RuntimeInfo {
  private String jarLocation;
  private String lastModified;

  public String getJarLocation() {
    return jarLocation;
  }

  public void setLastModified(String lastModified) {
    this.lastModified = lastModified;
  }

  public String getLastModified() {
    return lastModified;
  }

  public void setJarLocation(String jarLocation) {
    this.jarLocation = jarLocation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DeploymentInfo)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    DeploymentInfo that = (DeploymentInfo) o;
    return Objects.equals(getJarLocation(), that.getJarLocation());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getJarLocation());
  }
}
