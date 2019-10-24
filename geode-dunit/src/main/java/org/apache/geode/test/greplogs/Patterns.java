/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.test.greplogs;

import static java.util.regex.Pattern.compile;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum Patterns {

  /** IgnoredException add or remove statement */
  IGNORED_EXCEPTION(compile("<ExpectedException action=(add|remove)>(.*)</ExpectedException>")),
  /** Log statement */
  LOG_STATEMENT(compile("^\\[(?:fatal|error|warn|info|debug|trace|severe|warning|fine|finer|finest)")),
  /** Blank line */
  BLANK(compile("^\\s*$")),
  /** WARN or less specific log level */
  WARN_OR_LESS_LOG_LEVEL(compile("^\\[(?:warn|warning|info|debug|trace|fine|finer|finest)")),
  /** ERROR or more specific log level */
  ERROR_OR_MORE_LOG_LEVEL(compile("^\\[(?:fatal|error|severe)")),
  /** "Caused by" literal */
  CAUSED_BY(compile("Caused by")),
  /** Short name of error ? */
  ERROR_SHORT_NAME(compile("^\\[[^\\]]+\\](.*)$", Pattern.MULTILINE | Pattern.DOTALL)),
  /** "debug.*Wrote exception:" literal */
  DEBUG_WROTE_EXCEPTION(compile("\\[debug.*Wrote exception:")),
  /** Hydra rmi warning statement */
  RMI_WARNING(compile("^WARNING: Failed to .*java.rmi.ConnectException: Connection refused to host: .*; nested exception is:")),
  /** "java.lang.Error" literal */
  JAVA_LANG_ERROR(compile("^java\\.lang\\.\\S+Error$")),
  /** "Exception:" literal */
  EXCEPTION(compile("Exception:")),
  /** "Exception:" matcher 2 */
  EXCEPTION_2(compile("( [\\w\\.]+Exception: (([\\S]+ ){0,6}))")),
  /** "Exception:" matcher 3 */
  EXCEPTION_3(compile("( [\\w\\.]+Exception)$")),
  /** "Exception:" matcher 4 */
  EXCEPTION_4(compile("^([^:]+: (([\\w\"]+ ){0,6}))")),
  /** Malformed i18n message */
  MALFORMED_I18N_MESSAGE(compile("[^\\d]\\{\\d+\\}")),
  /** RegionVersionVector bit set message */
  RVV_BIT_SET_MESSAGE(compile("RegionVersionVector.+bsv\\d+.+bs=\\{\\d+\\}")),
  /** "{}" literal which is probably unused Log4J parameter */
  MALFORMED_LOG4J_MESSAGE(compile("\\{\\}")),
  /** "{}" literal used for hydra master locators wildcard */
  HYDRA_MASTER_LOCATORS_WILDCARD(compile("hydra\\.MasterDescription\\.master\\.locators=\\{\\}"));

  private final Pattern pattern;

  Patterns(Pattern pattern) {
    this.pattern = pattern;
  }

  Matcher matcher(CharSequence input) {
    return pattern.matcher(input);
  }
}
