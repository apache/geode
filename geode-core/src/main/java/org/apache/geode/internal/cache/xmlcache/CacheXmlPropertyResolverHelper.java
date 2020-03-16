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

package org.apache.geode.internal.cache.xmlcache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Helper class for CacheXmlPropertyResolver. Helps in parsing ${...${}..}..${} strings.
 *
 * @since GemFire 6.6
 */
public class CacheXmlPropertyResolverHelper {
  private static final Logger logger = LogService.getLogger();

  public static final String DEFAULT_PROPERTY_STRING_PREFIX = "${";

  public static final String DEFAULT_PROPERTY_STRING_SUFFIX = "}";

  public static final String DEFAULT_PREFIX_FOR_SUFFIX = "{";
  /**
   * This <code>HashMap </code> contains valid suffixes and prefixes to be parsed by
   * {@link CacheXmlPropertyResolverHelper} like {}, [] or ().
   */
  @Immutable
  private static final Map<String, String> validSuffixAndPrefixes;

  static {
    Map<String, String> map = new HashMap<>();
    map.put("}", "{");
    map.put("]", "[");
    map.put(")", "(");
    validSuffixAndPrefixes = Collections.unmodifiableMap(map);
  }
  /* String specifying the suffice for property key prefix */
  private String propertyPrefix = DEFAULT_PROPERTY_STRING_PREFIX;

  /* String specifying the suffice for property key suffix */
  private String propertySuffix = DEFAULT_PROPERTY_STRING_SUFFIX;

  private String prefixForSuffix = DEFAULT_PREFIX_FOR_SUFFIX;

  public CacheXmlPropertyResolverHelper(String propPrefix, String propSuffix) {
    if (propPrefix != null && propSuffix != null) {
      String validPrefix = validSuffixAndPrefixes.get(propSuffix);
      if (validPrefix != null && propPrefix.endsWith(validPrefix)) {
        this.prefixForSuffix = validPrefix;
      } else {
        this.prefixForSuffix = propPrefix;
      }
      this.propertyPrefix = propPrefix;

      this.propertySuffix = propSuffix;
    }
  }

  /**
   * Parses the given string which are supposed to be like ${} for system and/or Gemfire properties
   * to be replaced. This will return property.name from ${property.name}.
   *
   */
  protected String parseResolvablePropString(String unparsedString, PropertyResolver resolver,
      Set<String> visitedReplaceableStrings) {
    StringBuilder buf = new StringBuilder(unparsedString);
    int prefixIndex = buf.indexOf(propertyPrefix);

    while (prefixIndex != -1) {
      int suffixIndex = findSuffixIndex(buf, prefixIndex + propertyPrefix.length());
      if (suffixIndex != -1) {
        String replaceableString =
            buf.substring(prefixIndex + propertyPrefix.length(), suffixIndex);
        // Check for circular references
        if (!visitedReplaceableStrings.add(replaceableString)) {
          logger.info(
              "Some still unresolved string {} was replaced by resolver, leading to circular references.",
              replaceableString);
          throw new IllegalArgumentException("Some still unresolved string " + replaceableString
              + " was replaced by resolver, leading to circular references.");
        }
        /* Find the replacement using given <code>resolver</code> */
        replaceableString =
            parseResolvablePropString(replaceableString, resolver, visitedReplaceableStrings);
        String replacement = resolver.resolveReplaceString(replaceableString);

        if (replacement != null) {
          /*
           * put replacement in <code>unparsedString</code> and call
           * <code>parseResolvablePropString</code> recursively to find more unparsedStrings in the
           * replaced value of given unparsedString.
           */
          replacement = parseResolvablePropString(replacement, resolver, visitedReplaceableStrings);
          buf.replace(prefixIndex, suffixIndex + propertySuffix.length(), replacement);
          prefixIndex = buf.indexOf(propertyPrefix, prefixIndex + replacement.length());
        } else if (resolver.isIgnoreUnresolvedProperties()) {
          /* Look for more replaceable strings in given <code>unparsedString</code>. */
          prefixIndex = buf.indexOf(propertyPrefix, suffixIndex + propertySuffix.length());
        } else {
          throw new IllegalArgumentException(
              "No replacement found for property : " + replaceableString);
        }
        // Before iterating again remove replaceable string from visitedReplaceableStrings as it can
        // appear again.
        visitedReplaceableStrings.remove(replaceableString);
      } else {
        prefixIndex = -1;
      }
    }

    return buf.toString();
  }

  /**
   * Finds index of suffix in a string from a specified index. Like finds index of "}" in string
   * "${my.prop.name}" starting from index 2, which is 14.
   *
   */
  private int findSuffixIndex(StringBuilder buf, int index) {
    int inNestedProperty = 0;
    while (index < buf.length()) {
      if (buf.substring(index, index + this.propertySuffix.length())
          .equalsIgnoreCase(this.propertySuffix)) {
        if (inNestedProperty > 0) {
          inNestedProperty--;
          index = index + this.propertySuffix.length();
        } else {
          return index;
        }
      } else if (buf.substring(index, index + this.prefixForSuffix.length())
          .equalsIgnoreCase(this.prefixForSuffix)) {
        inNestedProperty++;
        index = index + this.prefixForSuffix.length();
      } else {
        index++;
      }
    }
    return -1;
  }
}
