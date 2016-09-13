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
package org.apache.geode.cache;

import org.apache.geode.GemFireException;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.InternalGemFireError;

/**
 * @deprecated this class is no longer in use
 */
@Deprecated
public class EntryNotFoundInRegion extends GemFireException {
  private static final long serialVersionUID = 5572550909947420405L;

  /**
   * Generates an {@link InternalGemFireError}
   * @param msg the detail message
   * @deprecated Do not create instances of this class.
   */
  @Deprecated
  public EntryNotFoundInRegion(String msg) {
    throw new InternalGemFireError(LocalizedStrings.EntryNotFoundInRegion_THIS_CLASS_IS_DEPRECATED.toLocalizedString());
  }

  /**
   * Generates an {@link InternalGemFireError}
   * @param msg the detail message
   * @param cause the causal Throwable
   * @deprecated do not create instances of this class.
   */
  @Deprecated
  public EntryNotFoundInRegion(String msg, Throwable cause) {
    throw new InternalGemFireError(LocalizedStrings.EntryNotFoundInRegion_THIS_CLASS_IS_DEPRECATED.toLocalizedString());
  }
}
