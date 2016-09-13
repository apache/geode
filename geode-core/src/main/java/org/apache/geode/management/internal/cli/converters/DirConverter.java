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
package com.gemstone.gemfire.management.internal.cli.converters;

import java.io.File;
import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.MethodTarget;
import org.springframework.shell.support.util.FileUtils;
import org.springframework.util.Assert;

import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.internal.cli.MultipleValueAdapter;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

/**
 *
 * @since GemFire 7.0
 * 
 * 
 */
public class DirConverter extends MultipleValueAdapter<String[]> {

  private static final String HOME_DIRECTORY_SYMBOL = "~";
  // Constants
  private static final String home = System.getProperty("user.home");

  @Override
  public String[] convertFromText(String[] values, Class<?> targetType,
      String context) {
    return values;
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completions,
      Class<?> targetType, String[] existingData, String context,
      MethodTarget target) {
    String adjustedUserInput = convertUserInputIntoAFullyQualifiedPath((existingData != null) ? existingData[existingData.length - 1]
        : "");

    String directoryData = adjustedUserInput.substring(0,
        adjustedUserInput.lastIndexOf(File.separator) + 1);
    adjustedUserInput = adjustedUserInput.substring(adjustedUserInput
        .lastIndexOf(File.separator) + 1);

    populate(completions, adjustedUserInput,
        ((existingData != null) ? existingData[existingData.length - 1] : ""),
        directoryData);

    return true;
  }

  protected void populate(final List<Completion> completions,
      final String adjustedUserInput, final String originalUserInput,
      final String directoryData) {
    File directory = new File(directoryData);

    if (!directory.isDirectory()) {
      return;
    }

    for (File file : directory.listFiles()) {
      if (adjustedUserInput == null
          || adjustedUserInput.length() == 0
          || file.getName().toLowerCase()
              .startsWith(adjustedUserInput.toLowerCase())) {

        String completion = "";
        if (directoryData.length() > 0)
          completion += directoryData;
        completion += file.getName();

        completion = convertCompletionBackIntoUserInputStyle(originalUserInput,
            completion);

        if (file.isDirectory()) {
          completions.add(new Completion(completion + File.separator));
        }
      }
    }
  }

  private String convertCompletionBackIntoUserInputStyle(
      final String originalUserInput, final String completion) {
    if (FileUtils.denotesAbsolutePath(originalUserInput)) {
      // Input was originally as a fully-qualified path, so we just keep the
      // completion in that form
      return completion;
    }
    if (originalUserInput.startsWith(HOME_DIRECTORY_SYMBOL)) {
      // Input originally started with this symbol, so replace the user's home
      // directory with it again
      Assert.notNull(home,
          "Home directory could not be determined from system properties");
      return HOME_DIRECTORY_SYMBOL + completion.substring(home.length());
    }
    // The path was working directory specific, so strip the working directory
    // given the user never typed it
    return completion.substring(getWorkingDirectoryAsString().length());
  }

  /**
   * If the user input starts with a tilde character (~), replace the tilde
   * character with the user's home directory. If the user input does not start
   * with a tilde, simply return the original user input without any changes if
   * the input specifies an absolute path, or return an absolute path based on
   * the working directory if the input specifies a relative path.
   * 
   * @param userInput
   *          the user input, which may commence with a tilde (required)
   * @return a string that is guaranteed to no longer contain a tilde as the
   *         first character (never null)
   */
  private String convertUserInputIntoAFullyQualifiedPath(final String userInput) {
    if (FileUtils.denotesAbsolutePath(userInput)) {
      // Input is already in a fully-qualified path form
      return userInput;
    }
    if (userInput.startsWith(HOME_DIRECTORY_SYMBOL)) {
      // Replace this symbol with the user's actual home directory
      Assert.notNull(home,
          "Home directory could not be determined from system properties");
      if (userInput.length() > 1) {
        return home + userInput.substring(1);
      }
    }
    // The path is working directory specific, so prepend the working directory
    String fullPath = getWorkingDirectoryAsString() + userInput;
    return fullPath;
  }

  private String getWorkingDirectoryAsString() {
    try {
      return getWorkingDirectory().getCanonicalPath() + File.separator;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * @return the "current working directory" this {@link DirConverter} should
   *         use if the user fails to provide an explicit directory in their
   *         input (required)
   */
  private File getWorkingDirectory() {
    return Gfsh.getCurrentInstance().getHome();
  }

  @Override
  public boolean supports(Class<?> type, String optionContext) {
    if (String[].class.isAssignableFrom(type)
        && optionContext.equals(ConverterHint.DIRS)) {
      return true;
    }
    return false;
  }

}
