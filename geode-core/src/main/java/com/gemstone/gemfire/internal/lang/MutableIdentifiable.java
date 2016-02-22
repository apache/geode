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
package com.gemstone.gemfire.internal.lang;

import com.gemstone.gemfire.lang.Identifiable;

/**
 * The MutableIdentifiable interface defines a contract for classes whose mutable Object instances can
 * be uniquely identified relative to other Object instances within the same class type hierarchy.
 * <p/>
 * @param <T> the class type of the identifier.
 * @see java.lang.Comparable
 * @since 7.0
 */
public interface MutableIdentifiable<T>  extends Identifiable {

  /**
   * Set the identifier uniquely identifying this Object instance.
   * <p/>
   * @param id an identifier uniquely identifying this Object.
   */
  public void setId(T id);

}
