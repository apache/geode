
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
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


/**
 *
 * A "disk-dirs" element specifies the region's disk directories.
 *
 *
 * <p>
 * Java class for disk-dirs-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="disk-dirs-type">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="disk-dir" type="{http://geode.apache.org/schema/cache}disk-dir-type" maxOccurs="unbounded"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "disk-dirs-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"diskDirs"})
@Experimental
public class DiskDirsType implements Serializable {

  @XmlElement(name = "disk-dir", namespace = "http://geode.apache.org/schema/cache",
      required = true)
  protected List<DiskDirType> diskDirs;

  /**
   * Gets the value of the diskDir property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the diskDir property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getDiskDirs().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link DiskDirType }
   *
   *
   */
  public List<DiskDirType> getDiskDirs() {
    if (diskDirs == null) {
      diskDirs = new ArrayList<>();
    }
    return this.diskDirs;
  }

}
