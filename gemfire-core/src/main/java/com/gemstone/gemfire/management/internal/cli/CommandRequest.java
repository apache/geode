/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.internal.lang.StringUtils;

/**
 * The CommandRequest class encapsulates information pertaining to the command the user entered in Gfsh.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.GfshParseResult
 * @since 8.0
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
    final Map<String, String> parameters = new HashMap<String, String>(getUserParameters());
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

}
