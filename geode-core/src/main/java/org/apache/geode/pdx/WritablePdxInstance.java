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
package org.apache.geode.pdx;


/**
 * WritablePdxInstance is a {@link PdxInstance} that also supports field modification 
 * using the {@link #setField setField} method. 
 * To get a WritablePdxInstance call {@link PdxInstance#createWriter createWriter}.
 * 
 * @since GemFire 6.6
 */

public interface WritablePdxInstance extends PdxInstance {
  /**
   * Set the existing named field to the given value.
   * The setField method has copy-on-write semantics.
   *  So for the modifications to be stored in the cache the WritablePdxInstance 
   * must be put into a region after setField has been called one or more times.
   * 
   * @param fieldName
   *          name of the field whose value will be set
   * @param value
   *          value that will be assigned to the field
   * @throws PdxFieldDoesNotExistException if the named field does not exist
   * @throws PdxFieldTypeMismatchException if the type of the value is not compatible with the field
   */
  public void setField(String fieldName, Object value);
}
