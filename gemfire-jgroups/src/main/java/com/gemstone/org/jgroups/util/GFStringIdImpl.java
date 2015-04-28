package com.gemstone.org.jgroups.util;

import java.text.MessageFormat;
import java.util.Locale;

public class GFStringIdImpl implements StringId {

  /** A unique identifier for this stringId
   */
  public final int id;
  /** the English translation of text */
  private final String text;

  /** A GFStringId to allow users to log a literal String using the {@link com.gemstone.gemfire.i18n.LogWriterI18n} */
  public static final StringId LITERAL = new GFStringIdImpl(1, "{0}"); 
  
  /** 
   * Gemstone internal constructor, customers have no need to  
   * create instances of this class.
   */  
  public GFStringIdImpl(int id, String text) {
    this.id = id;
    this.text = text;
  }
  
  /**
   * Accessor for the raw (unformatted) text of this GFStringId
   * @return unformated text
   **/ 
  public String getRawText() {
        return this.text;
  }  
  
  /**
   * @return the English translation of this GFStringId
   **/ 
  @Override
  public String toString() {
    return MessageFormat.format(this.text, (Object[])null);
  }


  /**
   * Substitutes parameter Objects into the text
   * @see java.text.MessageFormat
   * @return the English translation of this GFStringId
   **/ 
  public String toString(Object ... params) {
  return MessageFormat.format(this.text, params);
  }

  /**
   * @return the translation of this GFStringId based on the current {@link java.util.Locale}
   **/ 
  public String toLocalizedString() {
    return MessageFormat.format(this.text, (Object[])null);
  }
  
  /**
   * Substitutes parameter Objects into the text
   * @see java.text.MessageFormat
   * @return the translation of this GFStringId based on the current {@link java.util.Locale}
   **/ 
  public String toLocalizedString(Object ... params) {
    return MessageFormat.format(this.text, params);
  }
  
}
