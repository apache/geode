/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.IllegalFormatException;
import java.util.Properties;

/**
 * @author Shobhit Agarwal
 * @since 6.6
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
