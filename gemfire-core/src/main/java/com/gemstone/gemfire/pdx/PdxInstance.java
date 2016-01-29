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
package com.gemstone.gemfire.pdx;

import java.util.List;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;

/**
 * PdxInstance provides run time access to the fields of a PDX without 
 * deserializing the PDX. Preventing deserialization saves time
 * and memory and does not require the domain class.
 * This interface is implemented by GemFire. The PdxInstance implementation 
 * is a light weight wrapper that simply refers to the raw bytes of the PDX 
 * that are kept in the cache.
 * Applications can choose to access PdxInstances instead of Java objects by 
 * configuring the Cache to prefer PDX instances during deserialization. 
 * This can be done in <code>cache.xml</code> by setting the attribute <code>read-serialized</code> 
 * to true on the <code>pdx</code> element. Or it can be done programmatically using
 * either the {@link CacheFactory#setPdxReadSerialized(boolean) setPdxReadSerialized}
 * or {@link ClientCacheFactory#setPdxReadSerialized(boolean) client setPdxReadSerialized}
 * method. Once this preference is configured, then any time deserialization of a 
 * PDX is done it will deserialize into a PdxInstance.
 * <P>PdxInstances are immutable. If you want to change one call
 * {@link #createWriter}.
 * <p>
 * A PdxInstance's fields will always be those of the version it represents.
 * So if you add a field to your domain class you can end up with a PdxInstance
 * for version 1 (that does not have the field) and a PdxInstance for version 2.
 * The PdxInstance for version 1 will not have the added field and the PdxInstance
 * for version 2 will have the field. This differs from deserialization of a pdx
 * back to a domain class. In that case if version 2 is deserializing version 1
 * PdxReader will return a default value for the added field even though version 1
 * has no knowledge of it.
 * 
 * @author darrel
 * @since 6.6
 */
public interface PdxInstance extends java.io.Serializable {

  /**
   * Return the full name of the class that this pdx instance represents.
   * @return the name of the class that this pdx instance represents.
   * @since 6.6.2
   */
  public String getClassName();
  /**
   * Returns true if this instance represents an enum.
   * Enum's have a String field named "name" and an int field named "ordinal".
   * It is ok to cast a PdxInstance that represents an enum to {@link java.lang.Comparable}. 
   * PdxInstances representing enums are not writable.
   * @return true if this instance represents an enum.
   */
  public boolean isEnum();
  /**
   * Deserializes and returns the domain object that this instance represents.
   * 
   * @return the deserialized domain object.
   * @throws PdxSerializationException if the instance could not be deserialized
   */
  public Object getObject();

  /**
   * Checks if the named field exists and returns the result.
   * <p>This can be useful when writing code that handles more than one version of
   * a PDX class.
   * @param fieldName the name of the field to check
   * @return <code>true</code> if the named field exists; otherwise <code>false</code>
   */
  public boolean hasField(String fieldName);
  
  /**
   * Return an unmodifiable list of the field names on this PdxInstance.
   * @return an unmodifiable list of the field names on this PdxInstance
   */
  public List<String> getFieldNames();
  
  /**
   * Checks if the named field was {@link PdxWriter#markIdentityField(String) marked} as an identity field.
   * <p>Note that if no fields have been marked then all the fields are used as identity fields even though
   * this method will return <code>false</code> since none of them have been <em>marked</em>.
   * @param fieldName the name of the field to check
   * @return <code>true</code> if the named field exists and was marked as an identify field; otherwise <code>false</code>
   */
public boolean isIdentityField(String fieldName);

  /**
   * Reads the named field and returns its value. If the field does
   * not exist <code>null</code> is returned.
   * <p>A <code>null</code> result indicates that the field does not exist
   * or that it exists and its value is currently <code>null</code>.
   * The {@link #hasField(String) hasField} method can be used to figure out
   * which if these two cases is true.
   * <p>If an Object[] is deserialized by this call then that array's component
   * type will be <code>Object.class</code> instead of the original class that
   * the array had when it was serialized. This is done so that PdxInstance objects
   * can be added to the array.
   *  
   * @param fieldName
   *          name of the field to read
   * 
   * @return If this instance has the named field then the field's value is returned,
   * otherwise <code>null</code> is returned.
   * @throws PdxSerializationException if the field could not be deserialized
   */
  public Object getField(String fieldName);

  /**
   * Returns true if the given object is equals to this instance.
   * <p>If <code>other</code> is not a PdxInstance then it is not equal to this instance.
   * NOTE: Even if <code>other</code> is the result of calling {@link #getObject()} it will not
   * be equal to this instance.
   * <p>Otherwise equality of two PdxInstances is determined as follows:
   * <ol>
   * <li>The domain class name must be equal for both PdxInstances
   * <li>Each identity field must be equal.
   * </ol>
   * If one of the instances does not have a field that the other one does then equals will assume it
   * has the field with a default value.
   * If a PdxInstance has marked identity fields using {@link PdxWriter#markIdentityField(String) markIdentityField}
   * then only the marked identity fields are its identity fields.
   * Otherwise all its fields are identity fields.
   * <P>An identity field is equal if all the following are true:
   * <ol>
   * <li>The field name is equal.
   * <li>The field type is equal.
   * <li>The field value is equal.
   * </ol>
   * <P>If a field's type is <code>OBJECT</code> then its value must be deserialized to determine if it is equals. If the deserialized object is an array then {@link java.util.Arrays#deepEquals(Object[], Object[]) deepEquals} is used to determine equality. Otherwise {@link Object#equals(Object) equals} is used.
   * <P>If a field's type is <code>OBJECT[]</code> then its value must be deserialized and {@link java.util.Arrays#deepEquals(Object[], Object[]) deepEquals} is used to determine equality.
   * <P>For all other field types then the value does not need to be deserialized. Instead the serialized raw bytes are compared and used to determine equality.
   * <P>Note that any fields that have objects that do not override {@link Object#equals(Object) equals} will cause equals to return false when you might have expected it to return true.
   * The only exceptions to this are those that call {@link java.util.Arrays#deepEquals(Object[], Object[]) deepEquals} as noted above. You should either override equals and hashCode in these cases
   * or mark other fields as your identity fields.  
   * @param other the other instance to compare to this.
   * @return <code>true</code> if this instance is equal to <code>other</code>.
   */
  public boolean equals(Object other);

  /**
   * Generates a hashCode based on the identity fields of
   * this PdxInstance. 
   * <p>If a PdxInstance has marked identity fields using {@link PdxWriter#markIdentityField(String) markIdentityField}
   * then only the marked identity fields are its identity fields.
   * Otherwise all its fields are identity fields.
   * <p>
   * If an identity field is of type <code>OBJECT</code> then it is deserialized. If the deserialized object is an array then {@link java.util.Arrays#deepHashCode(Object[]) deepHashCode} is used. Otherwise {@link Object#hashCode() hashCode} is used.
   * <p>If an identity field is of type <code>OBJECT[]</code> this it is deserialized and {@link java.util.Arrays#deepHashCode(Object[]) deepHashCode} is used.
   * <p>Otherwise the field is not deserialized and the raw bytes of its value are used to compute the hash code.
   * <p>
   * The algorithm used to compute the hashCode is:
   * hashCode = 1;
   * foreach (field: sortedIdentityFields()) {
   *   if (field.isDefaultValue())
   *     continue;
   *   if (field.isArray()) {
   *     hashCode = hashCode*31 + Arrays.deepHashCode(field);
   *   } else {
   *     hashCode = hashCode*31 + field.hashCode();
   *   }
   * }
   * if (hashCode == 0) {
   *   hashCode = 1;
   * }
   */
  public int hashCode();

  /**
   * Prints out all of the identity fields of this PdxInstance.
   * <p>If a PdxInstance has marked identity fields using {@link PdxWriter#markIdentityField(String) markIdentityField}
   * then only the marked identity fields are its identity fields.
   * Otherwise all its fields are identity fields.
   */
  public String toString();
  
  /**
   * Creates and returns a {@link WritablePdxInstance} whose initial
   * values are those of this PdxInstance.
   * This call returns a copy of the current field values so modifications
   * made to the returned value will not modify this PdxInstance.
   * 
   * @return a {@link WritablePdxInstance}
   * @throws IllegalStateException if the PdxInstance is an enum.
   */
  public WritablePdxInstance createWriter();

}
