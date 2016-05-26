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
package com.gemstone.gemfire.pdx;

import com.gemstone.gemfire.GemFireException;

/**
 * Thrown if the type of a PDX field was changed or the wrong type was used.
 * PDX field types can not be changed. New fields can be added.
 * Existing fields can be removed. But once a field is added
 * its type can not be changed.
 * The writeXXX methods on {@link PdxWriter} define the field type.
 * <p>This exception can also be caused by {@link WritablePdxInstance#setField(String, Object) setField}
 * trying to set a value whose type is not compatible with the field.
 * @since GemFire 6.6
 *
 */
public class PdxFieldTypeMismatchException extends GemFireException {
  private static final long serialVersionUID = -829617162170742740L;

  /**
   * Constructs a new exception with the given message
   * @param message the message of the new exception
   */
  public PdxFieldTypeMismatchException(String message) {
    super(message);
  }
}
