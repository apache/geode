/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.geode.internal.lang.StringUtils;

/**
 * The CommandRequest class encapsulates information pertaining to the command the user entered in Gfsh.
 * <p/>
 * @see org.apache.geode.management.internal.cli.GfshParseResult
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public class CommandRequest {

  protected static final String OPTION_SPECIFIER = "--";

  private final byte[][] fileData;

  private final GfshParseResult parseResult;

  private final Map<String, String> customParameters = new HashMap<String, String>();
  private final Map<String, String> env;

  private String customInput;

  public CommandRequest(final Map<String, String> env) {
    this.env = env;
    this.fileData = null;
    this.parseResult = null;
  }

  public CommandRequest(final Map<String, String> env, final byte[][] fileData) {
    this.env = env;
    this.fileData = fileData;
    this.parseResult = null;
  }

  public CommandRequest(final GfshParseResult parseResult, final Map<String, String> env) {
    this(parseResult, env, null);
  }

  public CommandRequest(final GfshParseResult parseResult, final Map<String, String> env, final byte[][] fileData) {
    assert parseResult != null : "The Gfsh ParseResult cannot be null!";
    assert env != null : "The reference to the Gfsh CLI environment cannot be null!";
    this.env = env;
    this.fileData = fileData;
    this.parseResult = parseResult;
  }

  public String getName() {
    if (getUserInput() != null) {
      final String[] userInputTokenized = getUserInput().split("\\s");
      final StringBuilder buffer = new StringBuilder();

      for (final String token : userInputTokenized) {
        if (!token.startsWith(OPTION_SPECIFIER)) {
          buffer.append(token).append(StringUtils.SPACE);
        }
      }

      return buffer.toString().trim();
    }
    else {
      return "unknown";
    }
  }

  public String getCustomInput() {
    return customInput;
  }

  public void setCustomInput(final String input) {
    this.customInput = input;
  }

  public Map<String, String> getCustomParameters() {
    return customParameters;
  }

  public Map<String, String> getEnvironment() {
    return Collections.unmodifiableMap(env);
  }

  public byte[][] getFileData() {
    return fileData;
  }

  public boolean hasFileData() {
    return (getFileData() != null);
  }

  public String getInput() {
    return StringUtils.defaultIfBlank(getCustomInput(), getUserInput());
  }

  public Map<String, String> getParameters() {
    final Map<String, String> parameters = new HashMap<>();
    for (Map.Entry<String, String> mapEntry : getUserParameters().entrySet()) {
      String key = mapEntry.getKey();
      String value = mapEntry.getValue();

      if (hasQuotesAroundNegativeNumber(value)){
        String trimmed = value.substring(1, value.length() - 1);
        parameters.put(key, trimmed);
      }
      else {
        parameters.put(key, value);
      }
    }
    parameters.putAll(getCustomParameters());
    return Collections.unmodifiableMap(parameters);
  }

  protected GfshParseResult getParseResult() {
    return parseResult;
  }

  public String getUserInput() {
    return getParseResult().getUserInput();
  }

  public Map<String, String> getUserParameters() {
    return getParseResult().getParamValueStrings();
  }

  private boolean hasQuotesAroundNegativeNumber(String value) {
    if (value == null) {
      return false;
    } else {
      return value.startsWith("\"") && value.endsWith("\"") && value.matches("\"-[0-9]+\"");
    }
  }

}
