
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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;

import org.apache.geode.annotations.Experimental;


/**
 *
 * A "disk-dir" element specifies one of a region or diskstore's disk directories.
 *
 *
 * <p>
 * Java class for disk-dir-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="disk-dir-type">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;attribute name="dir-size" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "disk-dir-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"content"})
@Experimental
public class DiskDirType {

  @XmlValue
  protected String content;
  @XmlAttribute(name = "dir-size")
  protected String dirSize;

  /**
   *
   * A "disk-dir" element specifies one of a region or diskstore's disk directories.
   *
   *
   * possible object is
   * {@link String }
   *
   */
  public String getContent() {
    return content;
  }

  /**
   * Sets the value of the content property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setContent(String value) {
    this.content = value;
  }

  /**
   * Gets the value of the dirSize property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getDirSize() {
    return dirSize;
  }

  /**
   * Sets the value of the dirSize property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setDirSize(String value) {
    this.dirSize = value;
  }

}
