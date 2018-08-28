
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
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


/**
 *
 * A "disk-store" element specifies a DiskStore for persistence.
 *
 *
 * <p>
 * Java class for disk-store-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="disk-store-type">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="disk-dirs" type="{http://geode.apache.org/schema/cache}disk-dirs-type" minOccurs="0"/>
 *       &lt;/sequence>
 *       &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="auto-compact" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="compaction-threshold" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="allow-force-compaction" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="max-oplog-size" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="time-interval" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="write-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="queue-size" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="disk-usage-warning-percentage" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="disk-usage-critical-percentage" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "disk-store-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"diskDirs"})
@Experimental
public class DiskStoreType implements CacheElement {

  @XmlElement(name = "disk-dirs", namespace = "http://geode.apache.org/schema/cache")
  protected DiskDirsType diskDirs;
  @XmlAttribute(name = "name", required = true)
  protected String name;
  @XmlAttribute(name = "auto-compact")
  protected Boolean autoCompact;
  @XmlAttribute(name = "compaction-threshold")
  protected String compactionThreshold;
  @XmlAttribute(name = "allow-force-compaction")
  protected Boolean allowForceCompaction;
  @XmlAttribute(name = "max-oplog-size")
  protected String maxOplogSize;
  @XmlAttribute(name = "time-interval")
  protected String timeInterval;
  @XmlAttribute(name = "write-buffer-size")
  protected String writeBufferSize;
  @XmlAttribute(name = "queue-size")
  protected String queueSize;
  @XmlAttribute(name = "disk-usage-warning-percentage")
  protected String diskUsageWarningPercentage;
  @XmlAttribute(name = "disk-usage-critical-percentage")
  protected String diskUsageCriticalPercentage;

  @Override
  public String getId() {
    return getName();
  }

  /**
   * Gets the value of the diskDirs property.
   *
   * possible object is
   * {@link DiskDirsType }
   *
   */
  public DiskDirsType getDiskDirs() {
    return diskDirs;
  }

  /**
   * Sets the value of the diskDirs property.
   *
   * allowed object is
   * {@link DiskDirsType }
   *
   */
  public void setDiskDirs(DiskDirsType value) {
    this.diskDirs = value;
  }

  /**
   * Gets the value of the name property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the value of the name property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setName(String value) {
    this.name = value;
  }

  /**
   * Gets the value of the autoCompact property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isAutoCompact() {
    return autoCompact;
  }

  /**
   * Sets the value of the autoCompact property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setAutoCompact(Boolean value) {
    this.autoCompact = value;
  }

  /**
   * Gets the value of the compactionThreshold property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getCompactionThreshold() {
    return compactionThreshold;
  }

  /**
   * Sets the value of the compactionThreshold property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setCompactionThreshold(String value) {
    this.compactionThreshold = value;
  }

  /**
   * Gets the value of the allowForceCompaction property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isAllowForceCompaction() {
    return allowForceCompaction;
  }

  /**
   * Sets the value of the allowForceCompaction property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setAllowForceCompaction(Boolean value) {
    this.allowForceCompaction = value;
  }

  /**
   * Gets the value of the maxOplogSize property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getMaxOplogSize() {
    return maxOplogSize;
  }

  /**
   * Sets the value of the maxOplogSize property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setMaxOplogSize(String value) {
    this.maxOplogSize = value;
  }

  /**
   * Gets the value of the timeInterval property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getTimeInterval() {
    return timeInterval;
  }

  /**
   * Sets the value of the timeInterval property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setTimeInterval(String value) {
    this.timeInterval = value;
  }

  /**
   * Gets the value of the writeBufferSize property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getWriteBufferSize() {
    return writeBufferSize;
  }

  /**
   * Sets the value of the writeBufferSize property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setWriteBufferSize(String value) {
    this.writeBufferSize = value;
  }

  /**
   * Gets the value of the queueSize property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getQueueSize() {
    return queueSize;
  }

  /**
   * Sets the value of the queueSize property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setQueueSize(String value) {
    this.queueSize = value;
  }

  /**
   * Gets the value of the diskUsageWarningPercentage property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getDiskUsageWarningPercentage() {
    return diskUsageWarningPercentage;
  }

  /**
   * Sets the value of the diskUsageWarningPercentage property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setDiskUsageWarningPercentage(String value) {
    this.diskUsageWarningPercentage = value;
  }

  /**
   * Gets the value of the diskUsageCriticalPercentage property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getDiskUsageCriticalPercentage() {
    return diskUsageCriticalPercentage;
  }

  /**
   * Sets the value of the diskUsageCriticalPercentage property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setDiskUsageCriticalPercentage(String value) {
    this.diskUsageCriticalPercentage = value;
  }

}
