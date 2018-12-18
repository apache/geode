
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
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


/**
 * <p>
 * Java class for region-attributes-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="region-attributes-type">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="key-constraint" type="{http://www.w3.org/2001/XMLSchema}anyType" minOccurs="0"/>
 *         &lt;element name="value-constraint" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="region-time-to-live" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="expiration-attributes" type="{http://geode.apache.org/schema/cache}expiration-attributes-type"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="region-idle-time" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="expiration-attributes" type="{http://geode.apache.org/schema/cache}expiration-attributes-type"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="entry-time-to-live" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="expiration-attributes" type="{http://geode.apache.org/schema/cache}expiration-attributes-type"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="entry-idle-time" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="expiration-attributes" type="{http://geode.apache.org/schema/cache}expiration-attributes-type"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="disk-write-attributes" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;choice>
 *                   &lt;element name="asynchronous-writes">
 *                     &lt;complexType>
 *                       &lt;complexContent>
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                           &lt;attribute name="bytes-threshold" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                           &lt;attribute name="time-interval" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                         &lt;/restriction>
 *                       &lt;/complexContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                   &lt;element name="synchronous-writes" type="{http://www.w3.org/2001/XMLSchema}anyType"/>
 *                 &lt;/choice>
 *                 &lt;attribute name="max-oplog-size" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="roll-oplogs" type="{http://www.w3.org/2001/XMLSchema}string" />
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="disk-dirs" type="{http://geode.apache.org/schema/cache}disk-dirs-type" minOccurs="0"/>
 *         &lt;element name="partition-attributes" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="partition-resolver" minOccurs="0">
 *                     &lt;complexType>
 *                       &lt;complexContent>
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                           &lt;sequence>
 *                             &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
 *                             &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
 *                           &lt;/sequence>
 *                           &lt;attribute name="name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                         &lt;/restriction>
 *                       &lt;/complexContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                   &lt;element name="partition-listener" maxOccurs="unbounded" minOccurs="0">
 *                     &lt;complexType>
 *                       &lt;complexContent>
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                           &lt;sequence>
 *                             &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
 *                             &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
 *                           &lt;/sequence>
 *                         &lt;/restriction>
 *                       &lt;/complexContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                   &lt;element name="fixed-partition-attributes" maxOccurs="unbounded" minOccurs="0">
 *                     &lt;complexType>
 *                       &lt;complexContent>
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                           &lt;attribute name="partition-name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                           &lt;attribute name="is-primary" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                           &lt;attribute name="num-buckets" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                         &lt;/restriction>
 *                       &lt;/complexContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                 &lt;/sequence>
 *                 &lt;attribute name="local-max-memory" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="recovery-delay" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="redundant-copies" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="startup-recovery-delay" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="total-max-memory" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="total-num-buckets" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="colocated-with" type="{http://www.w3.org/2001/XMLSchema}string" />
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="membership-attributes" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="required-role" maxOccurs="unbounded" minOccurs="0">
 *                     &lt;complexType>
 *                       &lt;complexContent>
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                           &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                         &lt;/restriction>
 *                       &lt;/complexContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                 &lt;/sequence>
 *                 &lt;attribute name="loss-action">
 *                   &lt;simpleType>
 *                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *                       &lt;enumeration value="full-access"/>
 *                       &lt;enumeration value="limited-access"/>
 *                       &lt;enumeration value="no-access"/>
 *                       &lt;enumeration value="reconnect"/>
 *                     &lt;/restriction>
 *                   &lt;/simpleType>
 *                 &lt;/attribute>
 *                 &lt;attribute name="resumption-action">
 *                   &lt;simpleType>
 *                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *                       &lt;enumeration value="none"/>
 *                       &lt;enumeration value="reinitialize"/>
 *                     &lt;/restriction>
 *                   &lt;/simpleType>
 *                 &lt;/attribute>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="subscription-attributes" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;attribute name="interest-policy">
 *                   &lt;simpleType>
 *                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *                       &lt;enumeration value="all"/>
 *                       &lt;enumeration value="cache-content"/>
 *                     &lt;/restriction>
 *                   &lt;/simpleType>
 *                 &lt;/attribute>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="cache-loader" type="{http://geode.apache.org/schema/cache}cache-loader-type" minOccurs="0"/>
 *         &lt;element name="cache-writer" type="{http://geode.apache.org/schema/cache}cache-writer-type" minOccurs="0"/>
 *         &lt;element name="cache-listener" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
 *                   &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="compressor" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="eviction-attributes" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;choice>
 *                   &lt;element name="lru-entry-count">
 *                     &lt;complexType>
 *                       &lt;complexContent>
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                           &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" />
 *                           &lt;attribute name="maximum" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                         &lt;/restriction>
 *                       &lt;/complexContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                   &lt;element name="lru-heap-percentage">
 *                     &lt;complexType>
 *                       &lt;complexContent>
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                           &lt;sequence minOccurs="0">
 *                             &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
 *                             &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
 *                           &lt;/sequence>
 *                           &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" />
 *                         &lt;/restriction>
 *                       &lt;/complexContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                   &lt;element name="lru-memory-size">
 *                     &lt;complexType>
 *                       &lt;complexContent>
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                           &lt;sequence minOccurs="0">
 *                             &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
 *                             &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
 *                           &lt;/sequence>
 *                           &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" />
 *                           &lt;attribute name="maximum" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                         &lt;/restriction>
 *                       &lt;/complexContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                 &lt;/choice>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *       &lt;/sequence>
 *       &lt;attribute name="concurrency-level" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="data-policy" type="{http://geode.apache.org/schema/cache}region-attributesData-policy" />
 *       &lt;attribute name="early-ack" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="enable-async-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="enable-gateway" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="enable-subscription-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="gateway-sender-ids" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="async-event-queue-ids" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="hub-id" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="id" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="ignore-jta" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="index-update-type" type="{http://geode.apache.org/schema/cache}region-attributesIndex-update-type" />
 *       &lt;attribute name="initial-capacity" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="is-lock-grantor" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="load-factor" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="mirror-type" type="{http://geode.apache.org/schema/cache}region-attributesMirror-type" />
 *       &lt;attribute name="multicast-enabled" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="persist-backup" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="pool-name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="disk-synchronous" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="publisher" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="refid" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="scope" type="{http://geode.apache.org/schema/cache}region-attributesScope" />
 *       &lt;attribute name="statistics-enabled" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="cloning-enabled" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="concurrency-checks-enabled" type="{http://www.w3.org/2001/XMLSchema}boolean" default="true" />
 *       &lt;attribute name="off-heap" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "region-attributes-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"keyConstraint", "valueConstraint", "regionTimeToLive", "regionIdleTime",
        "entryTimeToLive", "entryIdleTime", "diskWriteAttributes", "diskDirs",
        "partitionAttributes", "membershipAttributes", "subscriptionAttributes", "cacheLoader",
        "cacheWriter", "cacheListeners", "compressor", "evictionAttributes"})
@Experimental
public class RegionAttributesType implements Serializable {

  @XmlElement(name = "key-constraint", namespace = "http://geode.apache.org/schema/cache")
  protected Object keyConstraint;
  @XmlElement(name = "value-constraint", namespace = "http://geode.apache.org/schema/cache")
  protected String valueConstraint;
  @XmlElement(name = "region-time-to-live", namespace = "http://geode.apache.org/schema/cache")
  protected ExpirationAttributesType regionTimeToLive;
  @XmlElement(name = "region-idle-time", namespace = "http://geode.apache.org/schema/cache")
  protected ExpirationAttributesType regionIdleTime;
  @XmlElement(name = "entry-time-to-live", namespace = "http://geode.apache.org/schema/cache")
  protected ExpirationAttributesType entryTimeToLive;
  @XmlElement(name = "entry-idle-time", namespace = "http://geode.apache.org/schema/cache")
  protected ExpirationAttributesType entryIdleTime;
  @XmlElement(name = "disk-write-attributes", namespace = "http://geode.apache.org/schema/cache")
  protected RegionAttributesType.DiskWriteAttributes diskWriteAttributes;
  @XmlElement(name = "disk-dirs", namespace = "http://geode.apache.org/schema/cache")
  protected DiskDirsType diskDirs;
  @XmlElement(name = "partition-attributes", namespace = "http://geode.apache.org/schema/cache")
  protected RegionAttributesType.PartitionAttributes partitionAttributes;
  @XmlElement(name = "membership-attributes", namespace = "http://geode.apache.org/schema/cache")
  protected RegionAttributesType.MembershipAttributes membershipAttributes;
  @XmlElement(name = "subscription-attributes", namespace = "http://geode.apache.org/schema/cache")
  protected RegionAttributesType.SubscriptionAttributes subscriptionAttributes;
  @XmlElement(name = "cache-loader", namespace = "http://geode.apache.org/schema/cache")
  protected DeclarableType cacheLoader;
  @XmlElement(name = "cache-writer", namespace = "http://geode.apache.org/schema/cache")
  protected DeclarableType cacheWriter;
  @XmlElement(name = "cache-listener", namespace = "http://geode.apache.org/schema/cache")
  protected List<DeclarableType> cacheListeners;
  @XmlElement(namespace = "http://geode.apache.org/schema/cache")
  protected ClassNameType compressor;
  @XmlElement(name = "eviction-attributes", namespace = "http://geode.apache.org/schema/cache")
  protected RegionAttributesType.EvictionAttributes evictionAttributes;
  @XmlAttribute(name = "concurrency-level")
  protected String concurrencyLevel;
  @XmlAttribute(name = "data-policy")
  protected RegionAttributesDataPolicy dataPolicy;
  @XmlAttribute(name = "early-ack")
  protected Boolean earlyAck;
  @XmlAttribute(name = "enable-async-conflation")
  protected Boolean enableAsyncConflation;
  @XmlAttribute(name = "enable-gateway")
  protected Boolean enableGateway;
  @XmlAttribute(name = "enable-subscription-conflation")
  protected Boolean enableSubscriptionConflation;
  @XmlAttribute(name = "gateway-sender-ids")
  protected String gatewaySenderIds;
  @XmlAttribute(name = "async-event-queue-ids")
  protected String asyncEventQueueIds;
  @XmlAttribute(name = "hub-id")
  protected String hubId;
  @XmlAttribute(name = "id")
  protected String id;
  @XmlAttribute(name = "ignore-jta")
  protected Boolean ignoreJta;
  @XmlAttribute(name = "index-update-type")
  protected RegionAttributesIndexUpdateType indexUpdateType;
  @XmlAttribute(name = "initial-capacity")
  protected String initialCapacity;
  @XmlAttribute(name = "is-lock-grantor")
  protected Boolean isLockGrantor;
  @XmlAttribute(name = "load-factor")
  protected String loadFactor;
  @XmlAttribute(name = "mirror-type")
  protected RegionAttributesMirrorType mirrorType;
  @XmlAttribute(name = "multicast-enabled")
  protected Boolean multicastEnabled;
  @XmlAttribute(name = "persist-backup")
  protected Boolean persistBackup;
  @XmlAttribute(name = "pool-name")
  protected String poolName;
  @XmlAttribute(name = "disk-store-name")
  protected String diskStoreName;
  @XmlAttribute(name = "disk-synchronous")
  protected Boolean diskSynchronous;
  @XmlAttribute(name = "publisher")
  protected Boolean publisher;
  @XmlAttribute(name = "refid")
  protected String refid;
  @XmlAttribute(name = "scope")
  protected RegionAttributesScope scope;
  @XmlAttribute(name = "statistics-enabled")
  protected Boolean statisticsEnabled;
  @XmlAttribute(name = "cloning-enabled")
  protected Boolean cloningEnabled;
  @XmlAttribute(name = "concurrency-checks-enabled")
  protected Boolean concurrencyChecksEnabled;
  @XmlAttribute(name = "off-heap")
  protected Boolean offHeap;

  /**
   * Gets the value of the keyConstraint property.
   *
   * possible object is
   * {@link Object }
   *
   */
  public Object getKeyConstraint() {
    return keyConstraint;
  }

  /**
   * Sets the value of the keyConstraint property.
   *
   * allowed object is
   * {@link Object }
   *
   */
  public void setKeyConstraint(Object value) {
    this.keyConstraint = value;
  }

  /**
   * Gets the value of the valueConstraint property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getValueConstraint() {
    return valueConstraint;
  }

  /**
   * Sets the value of the valueConstraint property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setValueConstraint(String value) {
    this.valueConstraint = value;
  }

  /**
   * Gets the value of the regionTimeToLive property.
   *
   * possible object is
   * {@link RegionAttributesType.RegionTimeToLive }
   *
   */
  public ExpirationAttributesType getRegionTimeToLive() {
    return regionTimeToLive;
  }

  /**
   * Sets the value of the regionTimeToLive property.
   *
   * allowed object is
   * {@link RegionAttributesType.RegionTimeToLive }
   *
   */
  public void setRegionTimeToLive(ExpirationAttributesType value) {
    this.regionTimeToLive = value;
  }

  /**
   * Gets the value of the regionIdleTime property.
   *
   * possible object is
   * {@link RegionAttributesType.RegionIdleTime }
   *
   */
  public ExpirationAttributesType getRegionIdleTime() {
    return regionIdleTime;
  }

  /**
   * Sets the value of the regionIdleTime property.
   *
   * allowed object is
   * {@link RegionAttributesType.RegionIdleTime }
   *
   */
  public void setRegionIdleTime(ExpirationAttributesType value) {
    this.regionIdleTime = value;
  }

  /**
   * Gets the value of the entryTimeToLive property.
   *
   * possible object is
   * {@link RegionAttributesType.EntryTimeToLive }
   *
   */
  public ExpirationAttributesType getEntryTimeToLive() {
    return entryTimeToLive;
  }

  /**
   * Sets the value of the entryTimeToLive property.
   *
   * allowed object is
   * {@link RegionAttributesType.EntryTimeToLive }
   *
   */
  public void setEntryTimeToLive(ExpirationAttributesType value) {
    this.entryTimeToLive = value;
  }

  /**
   * Gets the value of the entryIdleTime property.
   *
   * possible object is
   * {@link RegionAttributesType.EntryIdleTime }
   *
   */
  public ExpirationAttributesType getEntryIdleTime() {
    return entryIdleTime;
  }

  /**
   * Sets the value of the entryIdleTime property.
   *
   * allowed object is
   * {@link RegionAttributesType.EntryIdleTime }
   *
   */
  public void setEntryIdleTime(ExpirationAttributesType value) {
    this.entryIdleTime = value;
  }

  /**
   * Gets the value of the diskWriteAttributes property.
   *
   * possible object is
   * {@link RegionAttributesType.DiskWriteAttributes }
   *
   */
  public RegionAttributesType.DiskWriteAttributes getDiskWriteAttributes() {
    return diskWriteAttributes;
  }

  /**
   * Sets the value of the diskWriteAttributes property.
   *
   * allowed object is
   * {@link RegionAttributesType.DiskWriteAttributes }
   *
   */
  public void setDiskWriteAttributes(RegionAttributesType.DiskWriteAttributes value) {
    this.diskWriteAttributes = value;
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
   * Gets the value of the partitionAttributes property.
   *
   * possible object is
   * {@link RegionAttributesType.PartitionAttributes }
   *
   */
  public RegionAttributesType.PartitionAttributes getPartitionAttributes() {
    return partitionAttributes;
  }

  /**
   * Sets the value of the partitionAttributes property.
   *
   * allowed object is
   * {@link RegionAttributesType.PartitionAttributes }
   *
   */
  public void setPartitionAttributes(RegionAttributesType.PartitionAttributes value) {
    this.partitionAttributes = value;
  }

  /**
   * Gets the value of the membershipAttributes property.
   *
   * possible object is
   * {@link RegionAttributesType.MembershipAttributes }
   *
   */
  public RegionAttributesType.MembershipAttributes getMembershipAttributes() {
    return membershipAttributes;
  }

  /**
   * Sets the value of the membershipAttributes property.
   *
   * allowed object is
   * {@link RegionAttributesType.MembershipAttributes }
   *
   */
  public void setMembershipAttributes(RegionAttributesType.MembershipAttributes value) {
    this.membershipAttributes = value;
  }

  /**
   * Gets the value of the subscriptionAttributes property.
   *
   * possible object is
   * {@link RegionAttributesType.SubscriptionAttributes }
   *
   */
  public RegionAttributesType.SubscriptionAttributes getSubscriptionAttributes() {
    return subscriptionAttributes;
  }

  /**
   * Sets the value of the subscriptionAttributes property.
   *
   * allowed object is
   * {@link RegionAttributesType.SubscriptionAttributes }
   *
   */
  public void setSubscriptionAttributes(RegionAttributesType.SubscriptionAttributes value) {
    this.subscriptionAttributes = value;
  }

  /**
   * Gets the value of the cacheLoader property.
   *
   * possible object is
   * {@link DeclarableType }
   *
   */
  public DeclarableType getCacheLoader() {
    return cacheLoader;
  }

  /**
   * Sets the value of the cacheLoader property.
   *
   * allowed object is
   * {@link DeclarableType }
   *
   */
  public void setCacheLoader(DeclarableType value) {
    this.cacheLoader = value;
  }

  /**
   * Gets the value of the cacheWriter property.
   *
   * possible object is
   * {@link DeclarableType }
   *
   */
  public DeclarableType getCacheWriter() {
    return cacheWriter;
  }

  /**
   * Sets the value of the cacheWriter property.
   *
   * allowed object is
   * {@link DeclarableType }
   *
   */
  public void setCacheWriter(DeclarableType value) {
    this.cacheWriter = value;
  }

  /**
   * Gets the value of the cacheListener property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the cacheListener property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getCacheListeners().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link DeclarableType }
   *
   *
   */
  public List<DeclarableType> getCacheListeners() {
    if (cacheListeners == null) {
      cacheListeners = new ArrayList<DeclarableType>();
    }
    return this.cacheListeners;
  }

  /**
   * Gets the value of the compressor property.
   *
   * possible object is
   * {@link ClassNameType }
   *
   */
  public ClassNameType getCompressor() {
    return compressor;
  }

  /**
   * Sets the value of the compressor property.
   *
   * allowed object is
   * {@link ClassNameType }
   *
   */
  public void setCompressor(ClassNameType value) {
    this.compressor = value;
  }

  /**
   * Gets the value of the evictionAttributes property.
   *
   * possible object is
   * {@link RegionAttributesType.EvictionAttributes }
   *
   */
  public RegionAttributesType.EvictionAttributes getEvictionAttributes() {
    return evictionAttributes;
  }

  /**
   * Sets the value of the evictionAttributes property.
   *
   * allowed object is
   * {@link RegionAttributesType.EvictionAttributes }
   *
   */
  public void setEvictionAttributes(RegionAttributesType.EvictionAttributes value) {
    this.evictionAttributes = value;
  }

  /**
   * Gets the value of the concurrencyLevel property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getConcurrencyLevel() {
    return concurrencyLevel;
  }

  /**
   * Sets the value of the concurrencyLevel property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setConcurrencyLevel(String value) {
    this.concurrencyLevel = value;
  }

  /**
   * Gets the value of the dataPolicy property.
   *
   * possible object is
   * {@link RegionAttributesDataPolicy }
   *
   */
  public RegionAttributesDataPolicy getDataPolicy() {
    return dataPolicy;
  }

  /**
   * Sets the value of the dataPolicy property.
   *
   * allowed object is
   * {@link RegionAttributesDataPolicy }
   *
   */
  public void setDataPolicy(RegionAttributesDataPolicy value) {
    this.dataPolicy = value;
  }

  /**
   * Gets the value of the earlyAck property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isEarlyAck() {
    return earlyAck;
  }

  /**
   * Sets the value of the earlyAck property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setEarlyAck(Boolean value) {
    this.earlyAck = value;
  }

  /**
   * Gets the value of the enableAsyncConflation property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isEnableAsyncConflation() {
    return enableAsyncConflation;
  }

  /**
   * Sets the value of the enableAsyncConflation property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setEnableAsyncConflation(Boolean value) {
    this.enableAsyncConflation = value;
  }

  /**
   * Gets the value of the enableGateway property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isEnableGateway() {
    return enableGateway;
  }

  /**
   * Sets the value of the enableGateway property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setEnableGateway(Boolean value) {
    this.enableGateway = value;
  }

  /**
   * Gets the value of the enableSubscriptionConflation property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isEnableSubscriptionConflation() {
    return enableSubscriptionConflation;
  }

  /**
   * Sets the value of the enableSubscriptionConflation property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setEnableSubscriptionConflation(Boolean value) {
    this.enableSubscriptionConflation = value;
  }

  /**
   * Gets the value of the gatewaySenderIds property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getGatewaySenderIds() {
    return gatewaySenderIds;
  }

  /**
   * Sets the value of the gatewaySenderIds property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setGatewaySenderIds(String value) {
    this.gatewaySenderIds = value;
  }

  /**
   * Gets the value of the asyncEventQueueIds property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getAsyncEventQueueIds() {
    return asyncEventQueueIds;
  }

  /**
   * Sets the value of the asyncEventQueueIds property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setAsyncEventQueueIds(String value) {
    this.asyncEventQueueIds = value;
  }

  /**
   * Gets the value of the hubId property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getHubId() {
    return hubId;
  }

  /**
   * Sets the value of the hubId property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setHubId(String value) {
    this.hubId = value;
  }

  /**
   * Gets the value of the id property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getId() {
    return id;
  }

  /**
   * Sets the value of the id property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setId(String value) {
    this.id = value;
  }

  /**
   * Gets the value of the ignoreJta property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isIgnoreJta() {
    return ignoreJta;
  }

  /**
   * Sets the value of the ignoreJta property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setIgnoreJta(Boolean value) {
    this.ignoreJta = value;
  }

  /**
   * Gets the value of the indexUpdateType property.
   *
   * possible object is
   * {@link RegionAttributesIndexUpdateType }
   *
   */
  public RegionAttributesIndexUpdateType getIndexUpdateType() {
    return indexUpdateType;
  }

  /**
   * Sets the value of the indexUpdateType property.
   *
   * allowed object is
   * {@link RegionAttributesIndexUpdateType }
   *
   */
  public void setIndexUpdateType(RegionAttributesIndexUpdateType value) {
    this.indexUpdateType = value;
  }

  /**
   * Gets the value of the initialCapacity property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getInitialCapacity() {
    return initialCapacity;
  }

  /**
   * Sets the value of the initialCapacity property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setInitialCapacity(String value) {
    this.initialCapacity = value;
  }

  /**
   * Gets the value of the isLockGrantor property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isIsLockGrantor() {
    return isLockGrantor;
  }

  /**
   * Sets the value of the isLockGrantor property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setIsLockGrantor(Boolean value) {
    this.isLockGrantor = value;
  }

  /**
   * Gets the value of the loadFactor property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getLoadFactor() {
    return loadFactor;
  }

  /**
   * Sets the value of the loadFactor property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setLoadFactor(String value) {
    this.loadFactor = value;
  }

  /**
   * Gets the value of the mirrorType property.
   *
   * possible object is
   * {@link RegionAttributesMirrorType }
   *
   */
  public RegionAttributesMirrorType getMirrorType() {
    return mirrorType;
  }

  /**
   * Sets the value of the mirrorType property.
   *
   * allowed object is
   * {@link RegionAttributesMirrorType }
   *
   */
  public void setMirrorType(RegionAttributesMirrorType value) {
    this.mirrorType = value;
  }

  /**
   * Gets the value of the multicastEnabled property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isMulticastEnabled() {
    return multicastEnabled;
  }

  /**
   * Sets the value of the multicastEnabled property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setMulticastEnabled(Boolean value) {
    this.multicastEnabled = value;
  }

  /**
   * Gets the value of the persistBackup property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isPersistBackup() {
    return persistBackup;
  }

  /**
   * Sets the value of the persistBackup property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setPersistBackup(Boolean value) {
    this.persistBackup = value;
  }

  /**
   * Gets the value of the poolName property.
   *
   * possible object is
   * {@link String }
   *
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
   */
  public void setPoolName(String value) {
    this.poolName = value;
  }

  /**
   * Gets the value of the diskStoreName property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getDiskStoreName() {
    return diskStoreName;
  }

  /**
   * Sets the value of the diskStoreName property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setDiskStoreName(String value) {
    this.diskStoreName = value;
  }

  /**
   * Gets the value of the diskSynchronous property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isDiskSynchronous() {
    return diskSynchronous;
  }

  /**
   * Sets the value of the diskSynchronous property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setDiskSynchronous(Boolean value) {
    this.diskSynchronous = value;
  }

  /**
   * Gets the value of the publisher property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isPublisher() {
    return publisher;
  }

  /**
   * Sets the value of the publisher property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setPublisher(Boolean value) {
    this.publisher = value;
  }

  /**
   * Gets the value of the refid property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getRefid() {
    return refid;
  }

  /**
   * Sets the value of the refid property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setRefid(String value) {
    this.refid = value;
  }

  /**
   * Gets the value of the scope property.
   *
   * possible object is
   * {@link RegionAttributesScope }
   *
   */
  public RegionAttributesScope getScope() {
    return scope;
  }

  /**
   * Sets the value of the scope property.
   *
   * allowed object is
   * {@link RegionAttributesScope }
   *
   */
  public void setScope(RegionAttributesScope value) {
    this.scope = value;
  }

  /**
   * Gets the value of the statisticsEnabled property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isStatisticsEnabled() {
    return statisticsEnabled;
  }

  /**
   * Sets the value of the statisticsEnabled property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setStatisticsEnabled(Boolean value) {
    this.statisticsEnabled = value;
  }

  /**
   * Gets the value of the cloningEnabled property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isCloningEnabled() {
    return cloningEnabled;
  }

  /**
   * Sets the value of the cloningEnabled property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setCloningEnabled(Boolean value) {
    this.cloningEnabled = value;
  }

  /**
   * Gets the value of the concurrencyChecksEnabled property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public boolean isConcurrencyChecksEnabled() {
    if (concurrencyChecksEnabled == null) {
      return true;
    } else {
      return concurrencyChecksEnabled;
    }
  }

  /**
   * Sets the value of the concurrencyChecksEnabled property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setConcurrencyChecksEnabled(Boolean value) {
    this.concurrencyChecksEnabled = value;
  }

  /**
   * Gets the value of the offHeap property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isOffHeap() {
    return offHeap;
  }

  /**
   * Sets the value of the offHeap property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setOffHeap(Boolean value) {
    this.offHeap = value;
  }


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
   *         &lt;element name="asynchronous-writes">
   *           &lt;complexType>
   *             &lt;complexContent>
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                 &lt;attribute name="bytes-threshold" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                 &lt;attribute name="time-interval" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *               &lt;/restriction>
   *             &lt;/complexContent>
   *           &lt;/complexType>
   *         &lt;/element>
   *         &lt;element name="synchronous-writes" type="{http://www.w3.org/2001/XMLSchema}anyType"/>
   *       &lt;/choice>
   *       &lt;attribute name="max-oplog-size" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="roll-oplogs" type="{http://www.w3.org/2001/XMLSchema}string" />
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"asynchronousWrites", "synchronousWrites"})
  public static class DiskWriteAttributes {

    @XmlElement(name = "asynchronous-writes", namespace = "http://geode.apache.org/schema/cache")
    protected RegionAttributesType.DiskWriteAttributes.AsynchronousWrites asynchronousWrites;
    @XmlElement(name = "synchronous-writes", namespace = "http://geode.apache.org/schema/cache")
    protected Object synchronousWrites;
    @XmlAttribute(name = "max-oplog-size")
    protected String maxOplogSize;
    @XmlAttribute(name = "roll-oplogs")
    protected String rollOplogs;

    /**
     * Gets the value of the asynchronousWrites property.
     *
     * possible object is
     * {@link RegionAttributesType.DiskWriteAttributes.AsynchronousWrites }
     *
     */
    public RegionAttributesType.DiskWriteAttributes.AsynchronousWrites getAsynchronousWrites() {
      return asynchronousWrites;
    }

    /**
     * Sets the value of the asynchronousWrites property.
     *
     * allowed object is
     * {@link RegionAttributesType.DiskWriteAttributes.AsynchronousWrites }
     *
     */
    public void setAsynchronousWrites(
        RegionAttributesType.DiskWriteAttributes.AsynchronousWrites value) {
      this.asynchronousWrites = value;
    }

    /**
     * Gets the value of the synchronousWrites property.
     *
     * possible object is
     * {@link Object }
     *
     */
    public Object getSynchronousWrites() {
      return synchronousWrites;
    }

    /**
     * Sets the value of the synchronousWrites property.
     *
     * allowed object is
     * {@link Object }
     *
     */
    public void setSynchronousWrites(Object value) {
      this.synchronousWrites = value;
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
     * Gets the value of the rollOplogs property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getRollOplogs() {
      return rollOplogs;
    }

    /**
     * Sets the value of the rollOplogs property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setRollOplogs(String value) {
      this.rollOplogs = value;
    }


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
     *       &lt;attribute name="bytes-threshold" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
     *       &lt;attribute name="time-interval" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
     *     &lt;/restriction>
     *   &lt;/complexContent>
     * &lt;/complexType>
     * </pre>
     *
     *
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "")
    public static class AsynchronousWrites {

      @XmlAttribute(name = "bytes-threshold", required = true)
      protected String bytesThreshold;
      @XmlAttribute(name = "time-interval", required = true)
      protected String timeInterval;

      /**
       * Gets the value of the bytesThreshold property.
       *
       * possible object is
       * {@link String }
       *
       */
      public String getBytesThreshold() {
        return bytesThreshold;
      }

      /**
       * Sets the value of the bytesThreshold property.
       *
       * allowed object is
       * {@link String }
       *
       */
      public void setBytesThreshold(String value) {
        this.bytesThreshold = value;
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

    }
  }


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
   *       &lt;sequence>
   *         &lt;element name="expiration-attributes" type="{http://geode.apache.org/schema/cache}expiration-attributes-type"/>
   *       &lt;/sequence>
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"expirationAttributes"})
  public static class ExpirationAttributesType implements Serializable {

    @XmlElement(name = "expiration-attributes", namespace = "http://geode.apache.org/schema/cache",
        required = true)
    protected ExpirationAttributesDetail expirationAttributes = new ExpirationAttributesDetail();

    public DeclarableType getCustomExpiry() {
      return expirationAttributes.getCustomExpiry();
    }

    /**
     * Sets the value of the customExpiry property.
     *
     * allowed object is
     * {@link DeclarableType }
     *
     */
    public void setCustomExpiry(DeclarableType value) {
      expirationAttributes.setCustomExpiry(value);
    }

    /**
     * Gets the value of the action property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getAction() {
      return expirationAttributes.getAction();
    }

    /**
     * Sets the value of the action property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setAction(String value) {
      expirationAttributes.setAction(value);
    }

    /**
     * Gets the value of the timeout property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getTimeout() {
      return expirationAttributes.getTimeout();
    }

    /**
     * Sets the value of the timeout property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setTimeout(String value) {
      expirationAttributes.setTimeout(value);
    }

  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "expiration-attributes-type", namespace = "http://geode.apache.org/schema/cache",
      propOrder = {"customExpiry"})
  @Experimental
  public static class ExpirationAttributesDetail implements Serializable {
    @XmlElement(name = "custom-expiry", namespace = "http://geode.apache.org/schema/cache")
    protected DeclarableType customExpiry;
    @XmlAttribute(name = "action")
    protected String action;
    @XmlAttribute(name = "timeout", required = true)
    protected String timeout;

    /**
     * Gets the value of the customExpiry property.
     *
     * possible object is
     * {@link DeclarableType }
     *
     */
    public DeclarableType getCustomExpiry() {
      return customExpiry;
    }

    /**
     * Sets the value of the customExpiry property.
     *
     * allowed object is
     * {@link DeclarableType }
     *
     */
    public void setCustomExpiry(DeclarableType value) {
      this.customExpiry = value;
    }

    /**
     * Gets the value of the action property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getAction() {
      return action;
    }

    /**
     * Sets the value of the action property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setAction(String value) {
      this.action = value;
    }

    /**
     * Gets the value of the timeout property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getTimeout() {
      return timeout;
    }

    /**
     * Sets the value of the timeout property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setTimeout(String value) {
      this.timeout = value;
    }
  }

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
   *         &lt;element name="lru-entry-count">
   *           &lt;complexType>
   *             &lt;complexContent>
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                 &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" />
   *                 &lt;attribute name="maximum" type="{http://www.w3.org/2001/XMLSchema}string" />
   *               &lt;/restriction>
   *             &lt;/complexContent>
   *           &lt;/complexType>
   *         &lt;/element>
   *         &lt;element name="lru-heap-percentage">
   *           &lt;complexType>
   *             &lt;complexContent>
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                 &lt;sequence minOccurs="0">
   *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
   *                   &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
   *                 &lt;/sequence>
   *                 &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" />
   *               &lt;/restriction>
   *             &lt;/complexContent>
   *           &lt;/complexType>
   *         &lt;/element>
   *         &lt;element name="lru-memory-size">
   *           &lt;complexType>
   *             &lt;complexContent>
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                 &lt;sequence minOccurs="0">
   *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
   *                   &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
   *                 &lt;/sequence>
   *                 &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" />
   *                 &lt;attribute name="maximum" type="{http://www.w3.org/2001/XMLSchema}string" />
   *               &lt;/restriction>
   *             &lt;/complexContent>
   *           &lt;/complexType>
   *         &lt;/element>
   *       &lt;/choice>
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"lruEntryCount", "lruHeapPercentage", "lruMemorySize"})
  public static class EvictionAttributes implements Serializable {

    @XmlElement(name = "lru-entry-count", namespace = "http://geode.apache.org/schema/cache")
    protected RegionAttributesType.EvictionAttributes.LruEntryCount lruEntryCount;
    @XmlElement(name = "lru-heap-percentage", namespace = "http://geode.apache.org/schema/cache")
    protected RegionAttributesType.EvictionAttributes.LruHeapPercentage lruHeapPercentage;
    @XmlElement(name = "lru-memory-size", namespace = "http://geode.apache.org/schema/cache")
    protected RegionAttributesType.EvictionAttributes.LruMemorySize lruMemorySize;

    public String toStringRep() {
      return "lru-entry-count: " +
          this.lruEntryCount.getMaximum() + ", " +
          this.lruEntryCount.getAction().toString() + ", " +
          "\nlru-heap-percentage: " +
          this.lruHeapPercentage.getAction().toString() +
          "\nlru-memory-size: " +
          this.lruMemorySize.getMaximum() +
          this.lruMemorySize.getAction().toString();
    }

    /**
     * Gets the value of the lruEntryCount property.
     *
     * possible object is
     * {@link RegionAttributesType.EvictionAttributes.LruEntryCount }
     *
     */
    public RegionAttributesType.EvictionAttributes.LruEntryCount getLruEntryCount() {
      return lruEntryCount;
    }

    /**
     * Sets the value of the lruEntryCount property.
     *
     * allowed object is
     * {@link RegionAttributesType.EvictionAttributes.LruEntryCount }
     *
     */
    public void setLruEntryCount(RegionAttributesType.EvictionAttributes.LruEntryCount value) {
      this.lruEntryCount = value;
    }

    /**
     * Gets the value of the lruHeapPercentage property.
     *
     * possible object is
     * {@link RegionAttributesType.EvictionAttributes.LruHeapPercentage }
     *
     */
    public RegionAttributesType.EvictionAttributes.LruHeapPercentage getLruHeapPercentage() {
      return lruHeapPercentage;
    }

    /**
     * Sets the value of the lruHeapPercentage property.
     *
     * allowed object is
     * {@link RegionAttributesType.EvictionAttributes.LruHeapPercentage }
     *
     */
    public void setLruHeapPercentage(
        RegionAttributesType.EvictionAttributes.LruHeapPercentage value) {
      this.lruHeapPercentage = value;
    }

    /**
     * Gets the value of the lruMemorySize property.
     *
     * possible object is
     * {@link RegionAttributesType.EvictionAttributes.LruMemorySize }
     *
     */
    public RegionAttributesType.EvictionAttributes.LruMemorySize getLruMemorySize() {
      return lruMemorySize;
    }

    /**
     * Sets the value of the lruMemorySize property.
     *
     * allowed object is
     * {@link RegionAttributesType.EvictionAttributes.LruMemorySize }
     *
     */
    public void setLruMemorySize(RegionAttributesType.EvictionAttributes.LruMemorySize value) {
      this.lruMemorySize = value;
    }


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
     *       &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" />
     *       &lt;attribute name="maximum" type="{http://www.w3.org/2001/XMLSchema}string" />
     *     &lt;/restriction>
     *   &lt;/complexContent>
     * &lt;/complexType>
     * </pre>
     *
     *
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "")
    public static class LruEntryCount implements Serializable {

      @XmlAttribute(name = "action")
      protected EnumActionDestroyOverflow action;
      @XmlAttribute(name = "maximum")
      protected String maximum;

      /**
       * Gets the value of the action property.
       *
       * possible object is
       * {@link EnumActionDestroyOverflow }
       *
       */
      public EnumActionDestroyOverflow getAction() {
        return action;
      }

      /**
       * Sets the value of the action property.
       *
       * allowed object is
       * {@link EnumActionDestroyOverflow }
       *
       */
      public void setAction(EnumActionDestroyOverflow value) {
        this.action = value;
      }

      /**
       * Gets the value of the maximum property.
       *
       * possible object is
       * {@link String }
       *
       */
      public String getMaximum() {
        return maximum;
      }

      /**
       * Sets the value of the maximum property.
       *
       * allowed object is
       * {@link String }
       *
       */
      public void setMaximum(String value) {
        this.maximum = value;
      }

    }


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
     *       &lt;sequence minOccurs="0">
     *         &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
     *         &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
     *       &lt;/sequence>
     *       &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" />
     *     &lt;/restriction>
     *   &lt;/complexContent>
     * &lt;/complexType>
     * </pre>
     *
     *
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class LruHeapPercentage extends DeclarableType {
      @XmlAttribute(name = "action")
      protected EnumActionDestroyOverflow action;

      /**
       * Gets the value of the action property.
       *
       * possible object is
       * {@link EnumActionDestroyOverflow }
       *
       */
      public EnumActionDestroyOverflow getAction() {
        return action;
      }

      /**
       * Sets the value of the action property.
       *
       * allowed object is
       * {@link EnumActionDestroyOverflow }
       *
       */
      public void setAction(EnumActionDestroyOverflow value) {
        this.action = value;
      }

    }


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
     *       &lt;sequence minOccurs="0">
     *         &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
     *         &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
     *       &lt;/sequence>
     *       &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" />
     *       &lt;attribute name="maximum" type="{http://www.w3.org/2001/XMLSchema}string" />
     *     &lt;/restriction>
     *   &lt;/complexContent>
     * &lt;/complexType>
     * </pre>
     *
     *
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class LruMemorySize extends LruHeapPercentage {
      @XmlAttribute(name = "maximum")
      protected String maximum;

      /**
       * Gets the value of the maximum property.
       *
       * possible object is
       * {@link String }
       *
       */
      public String getMaximum() {
        return maximum;
      }

      /**
       * Sets the value of the maximum property.
       *
       * allowed object is
       * {@link String }
       *
       */
      public void setMaximum(String value) {
        this.maximum = value;
      }

    }

  }


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
   *       &lt;sequence>
   *         &lt;element name="required-role" maxOccurs="unbounded" minOccurs="0">
   *           &lt;complexType>
   *             &lt;complexContent>
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                 &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *               &lt;/restriction>
   *             &lt;/complexContent>
   *           &lt;/complexType>
   *         &lt;/element>
   *       &lt;/sequence>
   *       &lt;attribute name="loss-action">
   *         &lt;simpleType>
   *           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
   *             &lt;enumeration value="full-access"/>
   *             &lt;enumeration value="limited-access"/>
   *             &lt;enumeration value="no-access"/>
   *             &lt;enumeration value="reconnect"/>
   *           &lt;/restriction>
   *         &lt;/simpleType>
   *       &lt;/attribute>
   *       &lt;attribute name="resumption-action">
   *         &lt;simpleType>
   *           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
   *             &lt;enumeration value="none"/>
   *             &lt;enumeration value="reinitialize"/>
   *           &lt;/restriction>
   *         &lt;/simpleType>
   *       &lt;/attribute>
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"requiredRoles"})
  public static class MembershipAttributes implements Serializable {

    @XmlElement(name = "required-role", namespace = "http://geode.apache.org/schema/cache")
    protected List<RegionAttributesType.MembershipAttributes.RequiredRole> requiredRoles;
    @XmlAttribute(name = "loss-action")
    protected String lossAction;
    @XmlAttribute(name = "resumption-action")
    protected String resumptionAction;

    /**
     * Gets the value of the requiredRole property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the requiredRole property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getRequiredRoles().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link RegionAttributesType.MembershipAttributes.RequiredRole }
     *
     *
     */
    public List<RegionAttributesType.MembershipAttributes.RequiredRole> getRequiredRoles() {
      if (requiredRoles == null) {
        requiredRoles = new ArrayList<RegionAttributesType.MembershipAttributes.RequiredRole>();
      }
      return this.requiredRoles;
    }

    /**
     * Gets the value of the lossAction property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getLossAction() {
      return lossAction;
    }

    /**
     * Sets the value of the lossAction property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setLossAction(String value) {
      this.lossAction = value;
    }

    /**
     * Gets the value of the resumptionAction property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getResumptionAction() {
      return resumptionAction;
    }

    /**
     * Sets the value of the resumptionAction property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setResumptionAction(String value) {
      this.resumptionAction = value;
    }


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
     *       &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
     *     &lt;/restriction>
     *   &lt;/complexContent>
     * &lt;/complexType>
     * </pre>
     *
     *
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "")
    public static class RequiredRole {

      @XmlAttribute(name = "name", required = true)
      protected String name;

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

    }

  }


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
   *       &lt;sequence>
   *         &lt;element name="partition-resolver" minOccurs="0">
   *           &lt;complexType>
   *             &lt;complexContent>
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                 &lt;sequence>
   *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
   *                   &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
   *                 &lt;/sequence>
   *                 &lt;attribute name="name" type="{http://www.w3.org/2001/XMLSchema}string" />
   *               &lt;/restriction>
   *             &lt;/complexContent>
   *           &lt;/complexType>
   *         &lt;/element>
   *         &lt;element name="partition-listener" maxOccurs="unbounded" minOccurs="0">
   *           &lt;complexType>
   *             &lt;complexContent>
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                 &lt;sequence>
   *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
   *                   &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
   *                 &lt;/sequence>
   *               &lt;/restriction>
   *             &lt;/complexContent>
   *           &lt;/complexType>
   *         &lt;/element>
   *         &lt;element name="fixed-partition-attributes" maxOccurs="unbounded" minOccurs="0">
   *           &lt;complexType>
   *             &lt;complexContent>
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                 &lt;attribute name="partition-name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                 &lt;attribute name="is-primary" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *                 &lt;attribute name="num-buckets" type="{http://www.w3.org/2001/XMLSchema}string" />
   *               &lt;/restriction>
   *             &lt;/complexContent>
   *           &lt;/complexType>
   *         &lt;/element>
   *       &lt;/sequence>
   *       &lt;attribute name="local-max-memory" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="recovery-delay" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="redundant-copies" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="startup-recovery-delay" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="total-max-memory" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="total-num-buckets" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="colocated-with" type="{http://www.w3.org/2001/XMLSchema}string" />
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "",
      propOrder = {"partitionResolver", "partitionListeners", "fixedPartitionAttributes"})
  public static class PartitionAttributes implements Serializable {

    @XmlElement(name = "partition-resolver", namespace = "http://geode.apache.org/schema/cache")
    protected DeclarableType partitionResolver;
    @XmlElement(name = "partition-listener", namespace = "http://geode.apache.org/schema/cache")
    protected List<DeclarableType> partitionListeners;
    @XmlElement(name = "fixed-partition-attributes",
        namespace = "http://geode.apache.org/schema/cache")
    protected List<RegionAttributesType.PartitionAttributes.FixedPartitionAttributes> fixedPartitionAttributes;
    @XmlAttribute(name = "local-max-memory")
    protected String localMaxMemory;
    @XmlAttribute(name = "recovery-delay")
    protected String recoveryDelay;
    @XmlAttribute(name = "redundant-copies")
    protected String redundantCopies;
    @XmlAttribute(name = "startup-recovery-delay")
    protected String startupRecoveryDelay;
    @XmlAttribute(name = "total-max-memory")
    protected String totalMaxMemory;
    @XmlAttribute(name = "total-num-buckets")
    protected String totalNumBuckets;
    @XmlAttribute(name = "colocated-with")
    protected String colocatedWith;

    /**
     * Gets the value of the partitionResolver property.
     *
     * possible object is
     * {@link DeclarableType }
     *
     */
    public DeclarableType getPartitionResolver() {
      return partitionResolver;
    }

    /**
     * Sets the value of the partitionResolver property.
     *
     * allowed object is
     * {@link DeclarableType }
     *
     */
    public void setPartitionResolver(DeclarableType value) {
      this.partitionResolver = value;
    }

    /**
     * Gets the value of the partitionListener property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the partitionListener property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getPartitionListeners().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link DeclarableType }
     *
     *
     */
    public List<DeclarableType> getPartitionListeners() {
      if (partitionListeners == null) {
        partitionListeners = new ArrayList<DeclarableType>();
      }
      return this.partitionListeners;
    }

    /**
     * Gets the value of the fixedPartitionAttributes property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the fixedPartitionAttributes property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getFixedPartitionAttributes().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link RegionAttributesType.PartitionAttributes.FixedPartitionAttributes }
     *
     *
     */
    public List<RegionAttributesType.PartitionAttributes.FixedPartitionAttributes> getFixedPartitionAttributes() {
      if (fixedPartitionAttributes == null) {
        fixedPartitionAttributes =
            new ArrayList<RegionAttributesType.PartitionAttributes.FixedPartitionAttributes>();
      }
      return this.fixedPartitionAttributes;
    }

    /**
     * Gets the value of the localMaxMemory property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getLocalMaxMemory() {
      return localMaxMemory;
    }

    /**
     * Sets the value of the localMaxMemory property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setLocalMaxMemory(String value) {
      this.localMaxMemory = value;
    }

    /**
     * Gets the value of the recoveryDelay property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getRecoveryDelay() {
      return recoveryDelay;
    }

    /**
     * Sets the value of the recoveryDelay property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setRecoveryDelay(String value) {
      this.recoveryDelay = value;
    }

    /**
     * Gets the value of the redundantCopies property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getRedundantCopies() {
      return redundantCopies;
    }

    /**
     * Sets the value of the redundantCopies property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setRedundantCopies(String value) {
      this.redundantCopies = value;
    }

    /**
     * Gets the value of the startupRecoveryDelay property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getStartupRecoveryDelay() {
      return startupRecoveryDelay;
    }

    /**
     * Sets the value of the startupRecoveryDelay property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setStartupRecoveryDelay(String value) {
      this.startupRecoveryDelay = value;
    }

    /**
     * Gets the value of the totalMaxMemory property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getTotalMaxMemory() {
      return totalMaxMemory;
    }

    /**
     * Sets the value of the totalMaxMemory property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setTotalMaxMemory(String value) {
      this.totalMaxMemory = value;
    }

    /**
     * Gets the value of the totalNumBuckets property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getTotalNumBuckets() {
      return totalNumBuckets;
    }

    /**
     * Sets the value of the totalNumBuckets property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setTotalNumBuckets(String value) {
      this.totalNumBuckets = value;
    }

    /**
     * Gets the value of the colocatedWith property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getColocatedWith() {
      return colocatedWith;
    }

    /**
     * Sets the value of the colocatedWith property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setColocatedWith(String value) {
      this.colocatedWith = value;
    }


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
     *       &lt;attribute name="partition-name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
     *       &lt;attribute name="is-primary" type="{http://www.w3.org/2001/XMLSchema}boolean" />
     *       &lt;attribute name="num-buckets" type="{http://www.w3.org/2001/XMLSchema}string" />
     *     &lt;/restriction>
     *   &lt;/complexContent>
     * &lt;/complexType>
     * </pre>
     *
     *
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "")
    public static class FixedPartitionAttributes {

      @XmlAttribute(name = "partition-name", required = true)
      protected String partitionName;
      @XmlAttribute(name = "is-primary")
      protected Boolean isPrimary;
      @XmlAttribute(name = "num-buckets")
      protected String numBuckets;

      /**
       * Gets the value of the partitionName property.
       *
       * possible object is
       * {@link String }
       *
       */
      public String getPartitionName() {
        return partitionName;
      }

      /**
       * Sets the value of the partitionName property.
       *
       * allowed object is
       * {@link String }
       *
       */
      public void setPartitionName(String value) {
        this.partitionName = value;
      }

      /**
       * Gets the value of the isPrimary property.
       *
       * possible object is
       * {@link Boolean }
       *
       */
      public Boolean isIsPrimary() {
        return isPrimary;
      }

      /**
       * Sets the value of the isPrimary property.
       *
       * allowed object is
       * {@link Boolean }
       *
       */
      public void setIsPrimary(Boolean value) {
        this.isPrimary = value;
      }

      /**
       * Gets the value of the numBuckets property.
       *
       * possible object is
       * {@link String }
       *
       */
      public String getNumBuckets() {
        return numBuckets;
      }

      /**
       * Sets the value of the numBuckets property.
       *
       * allowed object is
       * {@link String }
       *
       */
      public void setNumBuckets(String value) {
        this.numBuckets = value;
      }

    }
  }

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
   *       &lt;attribute name="interest-policy">
   *         &lt;simpleType>
   *           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
   *             &lt;enumeration value="all"/>
   *             &lt;enumeration value="cache-content"/>
   *           &lt;/restriction>
   *         &lt;/simpleType>
   *       &lt;/attribute>
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "")
  public static class SubscriptionAttributes implements Serializable {

    @XmlAttribute(name = "interest-policy")
    protected String interestPolicy;

    /**
     * Gets the value of the interestPolicy property.
     *
     * possible object is
     * {@link String }
     *
     */
    public String getInterestPolicy() {
      return interestPolicy;
    }

    /**
     * Sets the value of the interestPolicy property.
     *
     * allowed object is
     * {@link String }
     *
     */
    public void setInterestPolicy(String value) {
      this.interestPolicy = value;
    }

  }

}
