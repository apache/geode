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
package org.apache.geode.pdx;

import org.apache.geode.GemFireException;

/**
 * Thrown when writing a field if the named field already exists.
 * <p>This is usually caused by the same field name being written more than once.
 * <p>It can also be caused by a field name being spelled one way when written
 * and a different way when read. Field names are case sensitive.
 * <p>It can also be caused by {@link PdxWriter#writeUnreadFields(PdxUnreadFields) writeUnreadFields}
 * being called after a field is written.
 * 
 * @since GemFire 6.6
 *
 */
public class PdxFieldAlreadyExistsException extends GemFireException {
  private static final long serialVersionUID = -6989799940994503713L;

  /**
   * Constructs a new exception with the given message.
   * @param message the message of the new exception
   */
  public PdxFieldAlreadyExistsException(String message) {
    super(message);
  }
}
