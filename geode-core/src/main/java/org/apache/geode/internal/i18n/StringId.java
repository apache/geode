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

package org.apache.geode.internal.i18n;

/**
 * This class forms the basis of the i18n strategy. Its primary function is to
 * be used as a key to be passed to an instance of StringIdResourceBundle.
 * @since GemFire 6.0
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
