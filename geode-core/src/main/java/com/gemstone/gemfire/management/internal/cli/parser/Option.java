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
package com.gemstone.gemfire.management.internal.cli.parser;

import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.management.internal.cli.parser.preprocessor.PreprocessorUtils;

/**
 * Option of a Command
 * 
 * @since GemFire 7.0
 * 
 */
public class Option extends Parameter {

  private static final String NULL = "__NULL__";
  private static final char SHORT_OPTION_DEFAULT = '\u0000';
  // Used for Option Identification
  private char shortOption;
  private String longOption;
  private List<String> synonyms;
  private List<String> aggregate;

  // Option Value related
  private String specifiedDefaultValue;

  // Constraints on Option
  private boolean withRequiredArgs;
  private String valueSeparator;

  public Option() {
    aggregate = new ArrayList<String>();
  }
  
  public Option(char shortOption) {
    this(shortOption, null , null);
  }
  
  public Option(char shortOption, List<String> synonyms) {
    this(shortOption,null,synonyms);
  }
  
  public Option(String longOption) {
    this(SHORT_OPTION_DEFAULT,longOption,null);
  }
  
  public Option(String longOption, List<String> synonyms) {
    this(SHORT_OPTION_DEFAULT,longOption,synonyms);
  }
  
  public Option(char shortOption, String longOption) {
    this(shortOption,longOption,null);
  }
  
  public Option(char shortOption, String longOption, List<String> synonyms) {
    aggregate = new ArrayList<String>();
    this.shortOption = shortOption;
    this.longOption = longOption;
    this.synonyms = synonyms;
    if (shortOption != SHORT_OPTION_DEFAULT) {
      aggregate.add("" + shortOption);
    }
    if (longOption != null) {
      aggregate.add(longOption);
    }
    if (synonyms != null) {
      aggregate.addAll(synonyms);
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(Option.class.getSimpleName())
        .append("[longOption=" + longOption).append(",help=" + help)
        .append(",required=" + required + "]");
    return builder.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 41;
    int result = 1;
    result = prime * result
        + ((longOption == null) ? 0 : longOption.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Option option = (Option) obj;
    if (longOption == null) {
      if (option.getLongOption() != null) {
        return false;
      }
    } else if (!longOption.equals(option.getLongOption())) {
      return false;
    }
    return true;
  }

  public List<String> getAggregate() {
    return aggregate;
  }

  public char getShortOption() {
    return shortOption;
  }

  public boolean setShortOption(char shortOption) {
    if (shortOption != SHORT_OPTION_DEFAULT) {
      int index = aggregate.indexOf("" + this.shortOption);
      if (index != -1) {
        return false;
      } else {
        this.shortOption = shortOption;
        aggregate.add("" + shortOption);
        return true;
      }
    }
    return false;
  }

  public String getLongOption() {
    return longOption;
  }

  public boolean setLongOption(String longOption) {
    longOption = longOption.trim();
    if (!longOption.equals("")) {
      if (this.longOption == null) {
        int index = aggregate.indexOf(longOption);
        if (index != -1) {
          return false;
        } else {
          this.longOption = longOption;
          aggregate.add(longOption);
          return true;
        }
      }
    }
    return false;
  }

  public List<String> getSynonyms() {
    return synonyms;
  }

  public void setSynonyms(List<String> synonyms) {
    this.synonyms = new ArrayList<String>();
    for (String string : synonyms) {
      if (!string.equals("")) {
        this.synonyms.add(string);
      }
    }
    if (this.synonyms.size() > 0) {
      this.aggregate.addAll(this.synonyms);
    }
  }

  public boolean isWithRequiredArgs() {
    return withRequiredArgs;
  }

  public void setWithRequiredArgs(boolean withRequiredArgs) {
    this.withRequiredArgs = withRequiredArgs;
  }

  public String[] getStringArray() {
    String[] stringArray = new String[aggregate.size()];
    for (int i = 0; i < stringArray.length; i++) {
      stringArray[i] = aggregate.get(i);
    }
    return stringArray;
  }

  public String getSpecifiedDefaultValue() {
    if (specifiedDefaultValue.equals(NULL)) {
      return null;
    } else {
      return specifiedDefaultValue;
    }
  }

  public void setSpecifiedDefaultValue(String specifiedDefaultValue) {
    this.specifiedDefaultValue = PreprocessorUtils.trim(specifiedDefaultValue)
        .getString();
  }

  public String getValueSeparator() {
    return valueSeparator;
  }

  public void setValueSeparator(String valueSeparator) {
    this.valueSeparator = valueSeparator;
  }
}
