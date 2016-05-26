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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.IllegalFormatException;
import java.util.Properties;

/**
 * @since GemFire 6.6
 */
public interface PropertyResolver {

  /** If system properties are overridden by Gemfire properties */
  public static final int SYSTEM_PROPERTIES_OVERRIDE = 0;

  /** if system properties are not overridden by any other properties */
  public static final int NO_SYSTEM_PROPERTIES_OVERRIDE = 1;

  /**
   * Resolves the given property string either from system properties or given
   * properties. and returns the replacement of the property found in available
   * properties. If no string replacement is found then
   * {@link IllegalFormatException} would be thrown based on
   * <code>ignoreUnresolvedProperties</code> flag being set by
   * {@link CacheXmlParser}.
   * 
   * @param replaceString
   * @return resolvedString
   */
  public String resolveReplaceString(String replaceString);

  public boolean isIgnoreUnresolvedProperties();

  public int getPropertyOverridden();

  public String processUnresolvableString(String stringWithPrefixAndSuffix, String prefix, String suffix);

  public String processUnresolvableString(String string);
}
