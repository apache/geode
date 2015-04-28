/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.internal.lang;

/**
 * The InOutParameter class is a utility class useful for creating methods with in/out parameters.  This class
 * constitutes a wrapper around the value it encapsulates.  In essence, an instance of this class is the same thing
 * as it's value, as determined by the equals method and so this class just serves as a kind of holder for it's value.
 * <p/>
 * @author John Blum
 * @param <T> the expected Class type of the parameter's value.
 * @since 6.8
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
