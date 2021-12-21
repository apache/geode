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
package org.apache.geode.management.internal;

public class FilterParam {

  private final String[] inclusionList;
  private final String[] exclusionList;

  private boolean isDefaultExcludeFilter = false;

  private boolean isDefaultIncludeFilter = false;

  private final String DEFAULT_EXCLUDE_FILTER = "";

  private final String DEFAULT_INCLUDE_FILTER = "";

  public FilterParam(String[] inclusionList, String[] exclusionList) {

    this.exclusionList = exclusionList;
    this.inclusionList = inclusionList;
    if (exclusionList.length == 1 && exclusionList[0].equals(DEFAULT_EXCLUDE_FILTER)) {
      isDefaultExcludeFilter = true;
    }
    if (inclusionList.length == 1 && inclusionList[0].equals(DEFAULT_INCLUDE_FILTER)) {
      isDefaultIncludeFilter = true;
    }

  }

  public boolean isDefaultExcludeFilter() {
    return isDefaultExcludeFilter;
  }

  public boolean isDefaultIncludeFilter() {
    return isDefaultIncludeFilter;
  }

  public void setDefaultExcludeFilter(boolean isDefaultExcludeFilter) {
    this.isDefaultExcludeFilter = isDefaultExcludeFilter;
  }

  public void setDefaultIncludeFilter(boolean isDefaultIncludeFilter) {
    this.isDefaultIncludeFilter = isDefaultIncludeFilter;
  }

  public String[] getInclusionList() {
    return inclusionList;
  }

  public String[] getExclusionList() {
    return exclusionList;
  }

}
