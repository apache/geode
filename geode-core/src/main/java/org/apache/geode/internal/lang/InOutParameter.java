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

package org.apache.geode.internal.lang;

/**
 * The InOutParameter class is a utility class useful for creating methods with in/out parameters.  This class
 * constitutes a wrapper around the value it encapsulates.  In essence, an instance of this class is the same thing
 * as it's value, as determined by the equals method and so this class just serves as a kind of holder for it's value.
 * <p/>
 * @param <T> the expected Class type of the parameter's value.
 * @since GemFire 6.8
 */
public class InOutParameter<T> {

  private T value;

  /**
   * Default constructor creating an instance of the InOutParameter with a null initial value.
   */
  public InOutParameter() {
  }

  /**
   * Constructs an instance of the InOutParameter with the specified value.
   * <p/>
   * @param value the initial value of this parameter.
   */
  public InOutParameter(final T value) {
    this.value = value;
  }

  /**
   * Gets the value of this in/out parameter.
   * <p/>
   * @return the value of this in/out parameter.
   */
  public T getValue() {
    return value;
  }

  /**
   * Sets the value of this in/out parameter.
   * <p/>
   * @param value the Object value to set this in/out parameter to.
   */
  public void setValue(final T value) {
    this.value = value;
  }

  /**
   * Determines whether the in/out parameter value is equal in value to the specified Object.  Note, this is not
   * typically how an equals method should be coded, but then this is not your typical class either!
   * <p/>
   * @param obj the Object value being compared for equality with this in/out parameter value.
   * @return boolean value indicating whether this in/out parameter value is equal to the specified Object.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof InOutParameter) {
      obj = ((InOutParameter) obj).getValue();
    }

    return ((obj == value) || (value != null && value.equals(obj)));
  }

  /**
   * Computes the hash value of this in/out parameter value.
   * <p/>
   * @return an integer value constituting the computed hash value of this in/out parameter.
   */
  @Override
  public int hashCode() {
    return (value == null ? 0 : value.hashCode());
  }

  /**
   * Gets the String representation of this in/out parameter value.
   * <p/>
   * @return a String value representing the value of this in/out parameter.
   */
  @Override
  public String toString() {
    return String.valueOf(value);
  }

}
