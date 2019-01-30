/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.cache.configuration;

import java.io.Serializable;
import java.util.Objects;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 * <p>
 * Java class for anonymous complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;choice>
 *         &lt;element name="string" type="{http://geode.apache.org/schema/cache}string-type"/>
 *         &lt;element name="declarable" type="{http://geode.apache.org/schema/cache}declarable-type"/>
 *       &lt;/choice>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 * ObjectType represents either a string or an object represented by the DeclarableType
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {"string", "declarable"})
public class ObjectType implements Serializable {
  @XmlElement(name = "string", namespace = "http://geode.apache.org/schema/cache")
  protected String string;
  @XmlElement(name = "declarable", namespace = "http://geode.apache.org/schema/cache")
  protected DeclarableType declarable;

  public ObjectType() {}

  public ObjectType(String string) {
    this.string = string;
  }

  public ObjectType(DeclarableType declarable) {
    this.declarable = declarable;
  }

  /**
   * Gets the value of the string property.
   */
  public String getString() {
    return string;
  }

  /**
   * Sets the value of the string property.
   */
  public void setString(String string) {
    this.string = string;
  }

  /**
   * Gets the value of the declarable property.
   *
   * possible object is {@link DeclarableType }
   */
  public DeclarableType getDeclarable() {
    return declarable;
  }

  /**
   * Sets the value of the declarable property.
   *
   * allowed object is {@link DeclarableType }
   */
  public void setDeclarable(DeclarableType value) {
    this.declarable = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ObjectType that = (ObjectType) o;
    return Objects.equals(string, that.string) && Objects.equals(declarable, that.declarable);
  }

  @Override
  public int hashCode() {
    return Objects.hash(string, declarable);
  }

  @Override
  public String toString() {
    if (string != null) {
      return string;
    }

    if (declarable != null) {
      return declarable.toString();
    }
    return "";
  }

}
