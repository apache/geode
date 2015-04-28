/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.org.jgroups.util;

/**
 * This class forms the basis of the i18n strategy. Its primary function is to
 * be used as a key to be passed to an instance of StringIdResourceBundle.
 * @author kbanks
 * @since 6.0 
 */
public interface StringId {
  /**
   * Accessor for the raw (unformatted) text of this StringId
   * @return unformated text
   **/ 
  public String getRawText();
  
  /**
   * @return the English translation of this StringId
   **/ 
  @Override
  public String toString();


  /**
   * Substitutes parameter Objects into the text
   * @see java.text.MessageFormat
   * @return the English translation of this StringId
   **/ 
  public String toString(Object ... params);

  /**
   * @return the translation of this StringId based on the current {@link java.util.Locale}
   **/ 
  public String toLocalizedString();
  
  /**
   * Substitutes parameter Objects into the text
   * @see java.text.MessageFormat
   * @return the translation of this StringId based on the current {@link java.util.Locale}
   **/ 
  public String toLocalizedString(Object ... params);
}
