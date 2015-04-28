/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.HashSet;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * Property resolver for resolving ${} like strings with system or Gemfire
 * properties in Cache.xml
 * 
 * @author Shobhit Agarwal
 * @since 6.6
 */
public class CacheXmlPropertyResolver implements PropertyResolver{
  private static final Logger logger = LogService.getLogger();

  /**
   * Would not throw any exception {@link IllegalArgumentException} if property
   * could not be resolved
   */
  private boolean ignoreUnresolvedProperties = false;

  /** Would not throw any exception if property could not be resolved */
  private int propertyOverridden = SYSTEM_PROPERTIES_OVERRIDE;

  private CacheXmlPropertyResolverHelper helper = null;

  /**
   * A properties object to hold all user specified properties passed through
   * .properties file or passed as {@link Properties} objects. It will be set
   * by <code>CacheXmlParser</code> if gemfire.properties has to be included during
   * replacement.
   */
  private Properties props = null;

  public CacheXmlPropertyResolver(boolean ignoreUnresolvedProperties,
      int propertyOverridden, Properties props) {
    super();
    this.ignoreUnresolvedProperties = ignoreUnresolvedProperties;
    this.propertyOverridden = propertyOverridden;
    this.props = props;
  }

  public int getPropertyOverridden() {
    return propertyOverridden;
  }

  /**
   * Sets <code>propertyOverridden</code> with one of the constants specified in
   * this {@link CacheXmlPropertyResolver} class.
   * 
   * @param propertyOverridden
   */
  public void setPropertyOverridden(int propertyOverridden) {
    this.propertyOverridden = propertyOverridden;
  }

  public void setIgnoreUnresolvedProperties(boolean ignoreUnresolvedProperties) {
    this.ignoreUnresolvedProperties = ignoreUnresolvedProperties;
  }

  public boolean isIgnoreUnresolvedProperties() {
    return ignoreUnresolvedProperties;
  }

  /**
   * Resolves the given property string either from system properties or given
   * properties. and returns the replacement of the property found in available
   * properties. If no string replacement is found then {@link IllegalArgumentException}
   * would be thrown based on <code>ignoreUnresolvedProperties</code> flag being set by
   * {@link CacheXmlParser}.
   * 
   * @param replaceString
   * @return resolvedString
   */
  public String resolveReplaceString(String replaceString) {
    String replacement = null;

    //Get System property first.
    replacement = System.getProperty(replaceString);

    //Override system property if replacement is null or we want to override system property.
    if((replacement == null || getPropertyOverridden() == SYSTEM_PROPERTIES_OVERRIDE) && props != null){
      String userDefined = props.getProperty(replaceString);
      if(userDefined != null){
        replacement = userDefined;
      }
    }
    return replacement;
  }

  /**
   * @param stringWithPrefixAndSuffix
   * @param prefix
   * @param suffix
   * @return string
   */
  public String processUnresolvableString(String stringWithPrefixAndSuffix, String prefix, String suffix){
    String resolvedString = null;
    try{
      if (helper == null){
        helper = new CacheXmlPropertyResolverHelper(prefix, suffix);
      }
      /** A <code>resolvedString</code> can be same as <code>stringWithPrefixAndSuffix</code> if 
       * <code>ignoreUnresolvedProperties</code> is set true and we just return it as is.
       */
      resolvedString = helper.parseResolvablePropString(stringWithPrefixAndSuffix, this, new HashSet<String>());
    } catch (IllegalArgumentException e) {
      if(ignoreUnresolvedProperties) {
        //Do Nothing
      } else {
        logger.error(LocalizedMessage.create(LocalizedStrings.CacheXmlPropertyResolver_UNSEROLVAVLE_STRING_FORMAT_ERROR__0, stringWithPrefixAndSuffix));
      }      
    }
    return resolvedString;
  }

  public void setProps(Properties props) {
    this.props = props;
  }

  public String processUnresolvableString(String stringWithPrefixAndSuffix) {
    return processUnresolvableString(stringWithPrefixAndSuffix, null, null);
  }
}
