
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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


/**
 *
 * dynamic-region-factory is deprecated. Use functions to create regions dynamically
 * instead.
 *
 * A "dynamic-region-factory" element configures a dynamic region factory for
 * this cache. If this optional element is missing then the cache does not
 * support dynamic regions.
 *
 * The optional "disk-dir" sub-element can be used to specify the directory to
 * store the persistent files that are used for dynamic region bookkeeping.
 * It defaults to the current directory.
 *
 * The pool-name attribute can be used to set the name of the connection pool used
 * by client applications in a client/server cache configuration. It should not be
 * specified in servers or peers.
 *
 *
 * <p>
 * Java class for dynamic-region-factory-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="dynamic-region-factory-type"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="disk-dir" type="{http://geode.apache.org/schema/cache}disk-dir-type" minOccurs="0"/&gt;
 *       &lt;/sequence&gt;
 *       &lt;attribute name="disable-persist-backup" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="disable-register-interest" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="pool-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "dynamic-region-factory-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"diskDir"})
@Experimental
public class DynamicRegionFactoryType {

  @XmlElement(name = "disk-dir", namespace = "http://geode.apache.org/schema/cache")
  protected DiskDirType diskDir;
  @XmlAttribute(name = "disable-persist-backup")
  protected Boolean disablePersistBackup;
  @XmlAttribute(name = "disable-register-interest")
  protected Boolean disableRegisterInterest;
  @XmlAttribute(name = "pool-name")
  protected String poolName;

  /**
   * Gets the value of the diskDir property.
   *
   * possible object is
   * {@link DiskDirType }
   *
   * @return the {@link DiskDirType}.
   */
  public DiskDirType getDiskDir() {
    return diskDir;
  }

  /**
   * Sets the value of the diskDir property.
   *
   * allowed object is
   * {@link DiskDirType }
   *
   * @param value the {@link DiskDirType}.
   */
  public void setDiskDir(DiskDirType value) {
    diskDir = value;
  }

  /**
   * Gets the value of the disablePersistBackup property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return true if persistent backup is disabled, false otherwise.
   */
  public Boolean isDisablePersistBackup() {
    return disablePersistBackup;
  }

  /**
   * Sets the value of the disablePersistBackup property.
   *
   * allowed object is
   * {@link Boolean }
   *
   * @param value true to disable persist backup, false to enable it.
   */
  public void setDisablePersistBackup(Boolean value) {
    disablePersistBackup = value;
  }

  /**
   * Gets the value of the disableRegisterInterest property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return true if register interest is disabled.
   */
  public Boolean isDisableRegisterInterest() {
    return disableRegisterInterest;
  }

  /**
   * Sets the value of the disableRegisterInterest property.
   *
   * allowed object is
   * {@link Boolean }
   *
   * @param value true to enable register interest, false to enable it.
   */
  public void setDisableRegisterInterest(Boolean value) {
    disableRegisterInterest = value;
  }

  /**
   * Gets the value of the poolName property.
   *
   * possible object is
   * {@link String }
   *
   * @return the pool name.
   */
  public String getPoolName() {
    return poolName;
  }

  /**
   * Sets the value of the poolName property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the pool name.
   */
  public void setPoolName(String value) {
    poolName = value;
  }

}
