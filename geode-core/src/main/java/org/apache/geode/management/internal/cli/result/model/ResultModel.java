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

package org.apache.geode.management.internal.cli.result.model;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang.NotImplementedException;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CommandResponse;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.ResultData;

public interface ResultModel extends Result, CommandResponse.Data, ResultData {

  String getType();

  Result.Status getStatus();

  void setStatus(final Status status);

  Object getContent();

  String getHeader();

  String getFooter();

  @JsonIgnore
  default GfJsonObject getGfJsonObject() {
    throw new UnsupportedOperationException(
        "This should never be called and only exists during migration from GfJsonObject to POJOs - use getContent() instead");
  }

  /*
   * Legacy methods required for API compatibility during transition. None of these should ever
   * be called.
   */

  default void resetToFirstLine() {
    throw new NotImplementedException("This method should not be called");
  }

  default boolean hasNextLine() {
    throw new NotImplementedException("This method should not be called");
  }

  default String nextLine() {
    throw new NotImplementedException("This method should not be called");
  }

  default boolean hasIncomingFiles() {
    throw new NotImplementedException("This method should not be called");
  }

  default void saveIncomingFiles(String directory) throws IOException {
    throw new NotImplementedException("This method should not be called");
  }

  default boolean failedToPersist() {
    throw new NotImplementedException("This method should not be called");
  }

  default void setCommandPersisted(boolean commandPersisted) {
    throw new NotImplementedException("This method should not be called");
  }
}
