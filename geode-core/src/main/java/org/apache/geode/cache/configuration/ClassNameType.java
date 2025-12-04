/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.cache.configuration;

import java.io.Serializable;
import java.util.Objects;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlType;

/**
 * <p>
 * Java class for anonymous complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
 *       &lt;/sequence&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {"className"})
public class ClassNameType implements Serializable {

  @XmlElement(name = "class-name", namespace = "http://geode.apache.org/schema/cache",
      required = true)
  protected String className;

  public ClassNameType() {}

  public ClassNameType(String className) {
    this.className = className;
  }

  /**
   * Gets the value of the className property.
   *
   * possible object is
   * {@link String }
   *
   * @return the class name.
   */
  public String getClassName() {
    return className;
  }

  /**
   * Sets the value of the className property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the class name.
   */
  public void setClassName(String value) {
    className = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClassNameType that = (ClassNameType) o;
    return Objects.equals(className, that.className);
  }

  @Override
  public String toString() {
    return className;
  }

  @Override
  public int hashCode() {
    return Objects.hash(className);
  }
}
