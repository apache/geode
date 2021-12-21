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
package org.apache.geode.management.internal.cli;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.management.cli.CliMetaData;

/**
 * The CommandRequest class encapsulates information pertaining to the command the user entered in
 * Gfsh.
 * <p/>
 *
 * @see org.apache.geode.management.internal.cli.GfshParseResult
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public class CommandRequest {
  private final List<File> fileList;
  private final GfshParseResult parseResult;
  private final Map<String, String> env;
  private final boolean downloadFile;


  @VisibleForTesting
  public CommandRequest(final Map<String, String> env) {
    this.env = env;
    fileList = null;
    parseResult = null;
    downloadFile = false;
  }

  public CommandRequest(final Map<String, String> env, final List<File> fileList) {
    this.env = env;
    this.fileList = fileList;
    parseResult = null;
    downloadFile = false;
  }

  public CommandRequest(final GfshParseResult parseResult, final Map<String, String> env,
      final List<File> fileList) {
    assert parseResult != null : "The Gfsh ParseResult cannot be null!";
    assert env != null : "The reference to the Gfsh CLI environment cannot be null!";
    this.env = env;
    this.fileList = fileList;
    this.parseResult = parseResult;

    CliMetaData metaData = parseResult.getMethod().getDeclaredAnnotation(CliMetaData.class);
    downloadFile = (metaData != null && metaData.isFileDownloadOverHttp());
  }


  public boolean isDownloadFile() {
    return downloadFile;
  }


  public Map<String, String> getEnvironment() {
    return Collections.unmodifiableMap(env);
  }

  public List<File> getFileList() {
    return fileList;
  }

  public boolean hasFileList() {
    return (getFileList() != null && getFileList().size() > 0);
  }

  protected GfshParseResult getParseResult() {
    return parseResult;
  }

  public String getUserInput() {
    return getParseResult().getUserInput();
  }
}
