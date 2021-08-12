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
package org.apache.geode.internal.util.redaction;

import static java.util.Collections.unmodifiableList;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PREFIX;
import static org.apache.geode.distributed.internal.DistributionConfig.SSL_SYSTEM_PROPS_NAME;
import static org.apache.geode.distributed.internal.DistributionConfig.SYS_PROP_NAME;
import static org.apache.geode.internal.util.ArrayUtils.asList;

import java.util.List;

import org.apache.geode.annotations.Immutable;

/**
 * Default strings that indicate sensitive data requiring redaction.
 */
class RedactionDefaults {

  static final String REDACTED = "********";

  private static final String JAVA_OPTION_D = "-D";
  private static final String GFSH_OPTION_JD = "--J=-D";

  /**
   * Strings containing these substrings are flagged as sensitive.
   */
  @Immutable
  static final List<String> SENSITIVE_SUBSTRINGS =
      unmodifiableList(asList("password"));

  /**
   * Strings starting with these prefixes are flagged as sensitive.
   */
  @Immutable
  static final List<String> SENSITIVE_PREFIXES =
      unmodifiableList(asList(SYS_PROP_NAME,
          SSL_SYSTEM_PROPS_NAME,
          SECURITY_PREFIX,
          JAVA_OPTION_D + SYS_PROP_NAME,
          JAVA_OPTION_D + SSL_SYSTEM_PROPS_NAME,
          JAVA_OPTION_D + SECURITY_PREFIX,
          GFSH_OPTION_JD + SYS_PROP_NAME,
          GFSH_OPTION_JD + SSL_SYSTEM_PROPS_NAME,
          GFSH_OPTION_JD + SECURITY_PREFIX));
}
