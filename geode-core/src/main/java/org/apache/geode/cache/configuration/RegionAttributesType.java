
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.management.configuration.ClassName;


/**
 * <p>
 * Java class for region-attributes-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="region-attributes-type"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="key-constraint" type="{http://www.w3.org/2001/XMLSchema}anyType" minOccurs="0"/&gt;
 *         &lt;element name="value-constraint" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/&gt;
 *         &lt;element name="region-time-to-live" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="expiration-attributes" type="{http://geode.apache.org/schema/cache}expiration-attributes-type"/&gt;
 *                 &lt;/sequence&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="region-idle-time" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="expiration-attributes" type="{http://geode.apache.org/schema/cache}expiration-attributes-type"/&gt;
 *                 &lt;/sequence&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="entry-time-to-live" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="expiration-attributes" type="{http://geode.apache.org/schema/cache}expiration-attributes-type"/&gt;
 *                 &lt;/sequence&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="entry-idle-time" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="expiration-attributes" type="{http://geode.apache.org/schema/cache}expiration-attributes-type"/&gt;
 *                 &lt;/sequence&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="disk-write-attributes" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;choice&gt;
 *                   &lt;element name="asynchronous-writes"&gt;
 *                     &lt;complexType&gt;
 *                       &lt;complexContent&gt;
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                           &lt;attribute name="bytes-threshold" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                           &lt;attribute name="time-interval" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                         &lt;/restriction&gt;
 *                       &lt;/complexContent&gt;
 *                     &lt;/complexType&gt;
 *                   &lt;/element&gt;
 *                   &lt;element name="synchronous-writes" type="{http://www.w3.org/2001/XMLSchema}anytype"/&gt;
 *                 &lt;/choice&gt;
 *                 &lt;attribute name="max-oplog-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="roll-oplogs" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="disk-dirs" type="{http://geode.apache.org/schema/cache}disk-dirs-type" minOccurs="0"/&gt;
 *         &lt;element name="partition-attributes" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="partition-resolver" minOccurs="0"&gt;
 *                     &lt;complexType&gt;
 *                       &lt;complexContent&gt;
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                           &lt;sequence&gt;
 *                             &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
 *                             &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *                           &lt;/sequence&gt;
 *                           &lt;attribute name="name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                         &lt;/restriction&gt;
 *                       &lt;/complexContent&gt;
 *                     &lt;/complexType&gt;
 *                   &lt;/element&gt;
 *                   &lt;element name="partition-listener" maxOccurs="unbounded" minOccurs="0"&gt;
 *                     &lt;complexType&gt;
 *                       &lt;complexContent&gt;
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                           &lt;sequence&gt;
 *                             &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
 *                             &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *                           &lt;/sequence&gt;
 *                         &lt;/restriction&gt;
 *                       &lt;/complexContent&gt;
 *                     &lt;/complexType&gt;
 *                   &lt;/element&gt;
 *                   &lt;element name="fixed-partition-attributes" maxOccurs="unbounded" minOccurs="0"&gt;
 *                     &lt;complexType&gt;
 *                       &lt;complexContent&gt;
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                           &lt;attribute name="partition-name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                           &lt;attribute name="is-primary" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                           &lt;attribute name="num-buckets" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                         &lt;/restriction&gt;
 *                       &lt;/complexContent&gt;
 *                     &lt;/complexType&gt;
 *                   &lt;/element&gt;
 *                 &lt;/sequence&gt;
 *                 &lt;attribute name="local-max-memory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="recovery-delay" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="redundant-copies" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="startup-recovery-delay" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="total-max-memory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="total-num-buckets" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="colocated-with" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="membership-attributes" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="required-role" maxOccurs="unbounded" minOccurs="0"&gt;
 *                     &lt;complexType&gt;
 *                       &lt;complexContent&gt;
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                           &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                         &lt;/restriction&gt;
 *                       &lt;/complexContent&gt;
 *                     &lt;/complexType&gt;
 *                   &lt;/element&gt;
 *                 &lt;/sequence&gt;
 *                 &lt;attribute name="loss-action"&gt;
 *                   &lt;simpleType&gt;
 *                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
 *                       &lt;enumeration value="full-access"/&gt;
 *                       &lt;enumeration value="limited-access"/&gt;
 *                       &lt;enumeration value="no-access"/&gt;
 *                       &lt;enumeration value="reconnect"/&gt;
 *                     &lt;/restriction&gt;
 *                   &lt;/simpleType&gt;
 *                 &lt;/attribute&gt;
 *                 &lt;attribute name="resumption-action"&gt;
 *                   &lt;simpleType&gt;
 *                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
 *                       &lt;enumeration value="none"/&gt;
 *                       &lt;enumeration value="reinitialize"/&gt;
 *                     &lt;/restriction&gt;
 *                   &lt;/simpleType&gt;
 *                 &lt;/attribute&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="subscription-attributes" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;attribute name="interest-policy"&gt;
 *                   &lt;simpleType&gt;
 *                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
 *                       &lt;enumeration value="all"/&gt;
 *                       &lt;enumeration value="cache-content"/&gt;
 *                     &lt;/restriction&gt;
 *                   &lt;/simpleType&gt;
 *                 &lt;/attribute&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="cache-loader" type="{http://geode.apache.org/schema/cache}cache-loader-type" minOccurs="0"/&gt;
 *         &lt;element name="cache-writer" type="{http://geode.apache.org/schema/cache}cache-writer-type" minOccurs="0"/&gt;
 *         &lt;element name="cache-listener" maxOccurs="unbounded" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
 *                   &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *                 &lt;/sequence&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="compressor" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
 *                 &lt;/sequence&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="eviction-attributes" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;choice&gt;
 *                   &lt;element name="lru-entry-count"&gt;
 *                     &lt;complexType&gt;
 *                       &lt;complexContent&gt;
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                           &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" /&gt;
 *                           &lt;attribute name="maximum" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                         &lt;/restriction&gt;
 *                       &lt;/complexContent&gt;
 *                     &lt;/complexType&gt;
 *                   &lt;/element&gt;
 *                   &lt;element name="lru-heap-percentage"&gt;
 *                     &lt;complexType&gt;
 *                       &lt;complexContent&gt;
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                           &lt;sequence minOccurs="0"&gt;
 *                             &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
 *                             &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *                           &lt;/sequence&gt;
 *                           &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" /&gt;
 *                         &lt;/restriction&gt;
 *                       &lt;/complexContent&gt;
 *                     &lt;/complexType&gt;
 *                   &lt;/element&gt;
 *                   &lt;element name="lru-memory-size"&gt;
 *                     &lt;complexType&gt;
 *                       &lt;complexContent&gt;
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                           &lt;sequence minOccurs="0"&gt;
 *                             &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
 *                             &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *                           &lt;/sequence&gt;
 *                           &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" /&gt;
 *                           &lt;attribute name="maximum" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                         &lt;/restriction&gt;
 *                       &lt;/complexContent&gt;
 *                     &lt;/complexType&gt;
 *                   &lt;/element&gt;
 *                 &lt;/choice&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *       &lt;/sequence&gt;
 *       &lt;attribute name="concurrency-level" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="data-policy" type="{http://geode.apache.org/schema/cache}region-attributesData-policy" /&gt;
 *       &lt;attribute name="early-ack" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="enable-async-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="enable-gateway" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="enable-subscription-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="gateway-sender-ids" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="async-event-queue-ids" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="hub-id" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="id" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="ignore-jta" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="index-update-type" type="{http://geode.apache.org/schema/cache}region-attributesIndex-update-type" /&gt;
 *       &lt;attribute name="initial-capacity" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="is-lock-grantor" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="load-factor" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="mirror-type" type="{http://geode.apache.org/schema/cache}region-attributesMirror-type" /&gt;
 *       &lt;attribute name="multicast-enabled" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="persist-backup" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="pool-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="disk-synchronous" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="publisher" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="refid" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="scope" type="{http://geode.apache.org/schema/cache}region-attributesScope" /&gt;
 *       &lt;attribute name="statistics-enabled" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="cloning-enabled" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="concurrency-checks-enabled" type="{http://www.w3.org/2001/XMLSchema}boolean" default="true" /&gt;
 *       &lt;attribute name="off-heap" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
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
  protected String keyConstraint;
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
   * @return the value of the keyConstraint property
   */
  public String getKeyConstraint() {
    return keyConstraint;
  }

  /**
   * Sets the value of the keyConstraint property.
   *
   * allowed object is
   * {@link Object }
   *
   * @param value the value of the keyConstraint property
   */
  public void setKeyConstraint(String value) {
    keyConstraint = value;
  }

  /**
   * Gets the value of the valueConstraint property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the valueConstraint property
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
   * @param value the value of the valueConstraint property
   */
  public void setValueConstraint(String value) {
    valueConstraint = value;
  }

  /**
   * Gets the value of the regionTimeToLive property.
   *
   * possible object is
   * {@link RegionAttributesType.ExpirationAttributesType }
   *
   * @return the value of the regionTimeToLive property
   */
  public ExpirationAttributesType getRegionTimeToLive() {
    return regionTimeToLive;
  }

  /**
   * Sets the value of the regionTimeToLive property.
   *
   * allowed object is
   * {@link RegionAttributesType.ExpirationAttributesType }
   *
   * @param value the value of the regionTimeToLive property
   */
  public void setRegionTimeToLive(ExpirationAttributesType value) {
    regionTimeToLive = value;
  }

  /**
   * update the region time to live using timeout, action or expiry. If all three are null, there
   * would be no update to the existing value
   *
   * @param timeout could be null
   * @param action could be null
   * @param expiry could be null
   */
  public void updateRegionTimeToLive(Integer timeout,
      String action, ClassName expiry) {
    regionTimeToLive = ExpirationAttributesType.combine(regionTimeToLive,
        ExpirationAttributesType.generate(timeout, action, expiry));
  }

  /**
   * Gets the value of the regionIdleTime property.
   *
   * possible object is
   * {@link RegionAttributesType.ExpirationAttributesType }
   *
   * @return the value of the regionIdleTime property
   */
  public ExpirationAttributesType getRegionIdleTime() {
    return regionIdleTime;
  }

  /**
   * Sets the value of the regionIdleTime property.
   *
   * allowed object is
   * {@link RegionAttributesType.ExpirationAttributesType }
   *
   * @param value the value of the regionIdleTime property
   */
  public void setRegionIdleTime(ExpirationAttributesType value) {
    regionIdleTime = value;
  }

  /**
   * update the region idle time using timeout, action or expiry. If all three are null, there
   * would be no update to the existing value
   *
   * @param timeout could be null
   * @param action could be null
   * @param expiry could be null
   */
  public void updateRegionIdleTime(Integer timeout,
      String action, ClassName expiry) {
    regionIdleTime = ExpirationAttributesType.combine(regionIdleTime,
        ExpirationAttributesType.generate(timeout, action, expiry));
  }


  /**
   * Gets the value of the entryTimeToLive property.
   *
   * possible object is
   * {@link RegionAttributesType.ExpirationAttributesType }
   *
   * @return the value of the entryTimeToLive property
   */
  public ExpirationAttributesType getEntryTimeToLive() {
    return entryTimeToLive;
  }

  /**
   * Sets the value of the entryTimeToLive property.
   *
   * allowed object is
   * {@link RegionAttributesType.ExpirationAttributesType }
   *
   * @param value the value of the entryTimeToLive property
   */
  public void setEntryTimeToLive(ExpirationAttributesType value) {
    entryTimeToLive = value;
  }

  /**
   * update the entry time to live using timeout, action or expiry. If all three are null, there
   * would be no update to the existing value
   *
   * @param timeout could be null
   * @param action could be null
   * @param expiry could be null
   */
  public void updateEntryTimeToLive(Integer timeout,
      String action, ClassName expiry) {
    entryTimeToLive = ExpirationAttributesType.combine(entryTimeToLive,
        ExpirationAttributesType.generate(timeout, action, expiry));
  }

  /**
   * Gets the value of the entryIdleTime property.
   *
   * possible object is
   * {@link RegionAttributesType.ExpirationAttributesType }
   *
   * @return the value of the entryIdleTime property
   */
  public ExpirationAttributesType getEntryIdleTime() {
    return entryIdleTime;
  }

  /**
   * Sets the value of the entryIdleTime property.
   *
   * allowed object is
   * {@link RegionAttributesType.ExpirationAttributesType }
   *
   * @param value the value of the entryIdleTime property
   */
  public void setEntryIdleTime(ExpirationAttributesType value) {
    entryIdleTime = value;
  }

  /**
   * update the entry idle time using timeout, action or expiry. If all three are null, there
   * would be no update to the existing value
   *
   * @param timeout could be null
   * @param action could be null
   * @param expiry could be null
   */
  public void updateEntryIdleTime(Integer timeout,
      String action, ClassName expiry) {
    entryIdleTime = ExpirationAttributesType.combine(entryIdleTime,
        ExpirationAttributesType.generate(timeout, action, expiry));
  }

  /**
   * Gets the value of the diskWriteAttributes property.
   *
   * possible object is
   * {@link RegionAttributesType.DiskWriteAttributes }
   *
   * @return the value of the diskWriteAttributes property
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
   * @param value the value of the diskWriteAttributes property
   */
  public void setDiskWriteAttributes(RegionAttributesType.DiskWriteAttributes value) {
    diskWriteAttributes = value;
  }

  /**
   * Gets the value of the diskDirs property.
   *
   * possible object is
   * {@link DiskDirsType }
   *
   * @return the value of the diskDirs property
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
   * @param value the value of the diskDirs property
   */
  public void setDiskDirs(DiskDirsType value) {
    diskDirs = value;
  }

  /**
   * Gets the value of the partitionAttributes property.
   *
   * possible object is
   * {@link RegionAttributesType.PartitionAttributes }
   *
   * @return the value of the partitionAttributes property
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
   * @param value the value of the partitionAttributes property
   */
  public void setPartitionAttributes(RegionAttributesType.PartitionAttributes value) {
    partitionAttributes = value;
  }

  /**
   * Gets the value of the membershipAttributes property.
   *
   * possible object is
   * {@link RegionAttributesType.MembershipAttributes }
   *
   * @return the value of the membershipAttributes property
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
   * @param value the value of the membershipAttributes property
   */
  public void setMembershipAttributes(RegionAttributesType.MembershipAttributes value) {
    membershipAttributes = value;
  }

  /**
   * Gets the value of the subscriptionAttributes property.
   *
   * possible object is
   * {@link RegionAttributesType.SubscriptionAttributes }
   *
   * @return the value of the subscriptionAttributes property
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
   * @param value the value of the subscriptionAttributes property
   */
  public void setSubscriptionAttributes(RegionAttributesType.SubscriptionAttributes value) {
    subscriptionAttributes = value;
  }

  /**
   * Gets the value of the cacheLoader property.
   *
   * possible object is
   * {@link DeclarableType }
   *
   * @return the value of the cacheLoader property
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
   * @param value the value of the cacheLoader property
   */
  public void setCacheLoader(DeclarableType value) {
    cacheLoader = value;
  }

  /**
   * Gets the value of the cacheWriter property.
   *
   * possible object is
   * {@link DeclarableType }
   *
   * @return the value of the cacheWriter property
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
   * @param value the value of the cacheWriter property
   */
  public void setCacheWriter(DeclarableType value) {
    cacheWriter = value;
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
   * @return the value of the cacheListener property
   */
  public List<DeclarableType> getCacheListeners() {
    if (cacheListeners == null) {
      cacheListeners = new ArrayList<>();
    }
    return cacheListeners;
  }

  /**
   * Gets the value of the compressor property.
   *
   * possible object is
   * {@link ClassNameType }
   *
   * @return the value of the compressor property
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
   * @param value the value of the compressor property
   */
  public void setCompressor(ClassNameType value) {
    compressor = value;
  }

  /**
   * Gets the value of the evictionAttributes property.
   *
   * possible object is
   * {@link RegionAttributesType.EvictionAttributes }
   *
   * @return the value of the evictionAttributes property
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
   * @param value the value of the evictionAttributes property
   */
  public void setEvictionAttributes(RegionAttributesType.EvictionAttributes value) {
    evictionAttributes = value;
  }

  /**
   * Gets the value of the concurrencyLevel property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the concurrencyLevel property
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
   * @param value the value of the concurrencyLevel property
   */
  public void setConcurrencyLevel(String value) {
    concurrencyLevel = value;
  }

  /**
   * Gets the value of the dataPolicy property.
   *
   * possible object is
   * {@link RegionAttributesDataPolicy }
   *
   * @return the value of the dataPolicy property
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
   * @param value the value of the dataPolicy property
   */
  public void setDataPolicy(RegionAttributesDataPolicy value) {
    dataPolicy = value;
  }

  /**
   * Gets the value of the earlyAck property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the earlyAck property
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
   * @param value the value of the earlyAck property
   */
  public void setEarlyAck(Boolean value) {
    earlyAck = value;
  }

  /**
   * Gets the value of the enableAsyncConflation property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the enableAsyncConflation property
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
   * @param value the value of the enableAsyncConflation property
   */
  public void setEnableAsyncConflation(Boolean value) {
    enableAsyncConflation = value;
  }

  /**
   * Gets the value of the enableGateway property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the enableGateway property
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
   * @param value the value of the enableGateway property
   */
  public void setEnableGateway(Boolean value) {
    enableGateway = value;
  }

  /**
   * Gets the value of the enableSubscriptionConflation property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the enableSubscriptionConflation property
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
   * @param value the value of the enableSubscriptionConflation property
   */
  public void setEnableSubscriptionConflation(Boolean value) {
    enableSubscriptionConflation = value;
  }

  /**
   * Gets the value of the gatewaySenderIds property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the gatewaySenderIds property
   */
  public String getGatewaySenderIds() {
    return gatewaySenderIds;
  }

  /**
   * turn the comma separated ids into a set of id
   *
   * @return a set of gatewaySenderIds
   */
  @JsonIgnore
  public Set<String> getGatewaySenderIdsAsSet() {
    if (gatewaySenderIds == null) {
      return null;
    }
    return Arrays.stream(gatewaySenderIds.split(","))
        .filter(StringUtils::isNotBlank)
        .collect(Collectors.toSet());
  }

  /**
   * Sets the value of the gatewaySenderIds property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the value of the gatewaySenderIds property
   */
  public void setGatewaySenderIds(String value) {
    gatewaySenderIds = value;
  }

  /**
   * Gets the value of the asyncEventQueueIds property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the asyncEventQueueIds property
   */
  public String getAsyncEventQueueIds() {
    return asyncEventQueueIds;
  }

  /**
   * turn the comma separated id into a set of ids
   *
   * @return a set of asyncEventQueueIds
   */
  @JsonIgnore
  public Set<String> getAsyncEventQueueIdsAsSet() {
    if (asyncEventQueueIds == null) {
      return null;
    }
    return Arrays.stream(asyncEventQueueIds.split(","))
        .filter(StringUtils::isNotBlank)
        .collect(Collectors.toSet());
  }

  /**
   * Sets the value of the asyncEventQueueIds property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the value of the asyncEventQueueIds property
   */
  public void setAsyncEventQueueIds(String value) {
    asyncEventQueueIds = value;
  }

  /**
   * Gets the value of the hubId property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the hubId property
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
   * @param value the value of the hubId property
   */
  public void setHubId(String value) {
    hubId = value;
  }

  /**
   * Gets the value of the id property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the id property
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
   * @param value the value of the id property
   */
  public void setId(String value) {
    id = value;
  }

  /**
   * Gets the value of the ignoreJta property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the ignoreJta property
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
   * @param value the value of the ignoreJta property
   */
  public void setIgnoreJta(Boolean value) {
    ignoreJta = value;
  }

  /**
   * Gets the value of the indexUpdateType property.
   *
   * possible object is
   * {@link RegionAttributesIndexUpdateType }
   *
   * @return the value of the indexUpdateType property
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
   * @param value the value of the indexUpdateType property
   */
  public void setIndexUpdateType(RegionAttributesIndexUpdateType value) {
    indexUpdateType = value;
  }

  /**
   * Gets the value of the initialCapacity property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the initialCapacity property
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
   * @param value the value of the initialCapacity property
   */
  public void setInitialCapacity(String value) {
    initialCapacity = value;
  }

  /**
   * Gets the value of the isLockGrantor property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the isLockGrantor property
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
   * @param value the value of the isLockGrantor property
   */
  public void setIsLockGrantor(Boolean value) {
    isLockGrantor = value;
  }

  /**
   * Gets the value of the loadFactor property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the loadFactor property
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
   * @param value the value of the loadFactor property
   */
  public void setLoadFactor(String value) {
    loadFactor = value;
  }

  /**
   * Gets the value of the mirrorType property.
   *
   * possible object is
   * {@link RegionAttributesMirrorType }
   *
   * @return the value of the mirrorType property
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
   * @param value the value of the mirrorType property
   */
  public void setMirrorType(RegionAttributesMirrorType value) {
    mirrorType = value;
  }

  /**
   * Gets the value of the multicastEnabled property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the multicastEnabled property
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
   * @param value the value of the multicastEnabled property
   */
  public void setMulticastEnabled(Boolean value) {
    multicastEnabled = value;
  }

  /**
   * Gets the value of the persistBackup property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the persistBackup property
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
   * @param value the value of the persistBackup property
   */
  public void setPersistBackup(Boolean value) {
    persistBackup = value;
  }

  /**
   * Gets the value of the poolName property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the poolName property
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
   * @param value the value of the poolName property
   */
  public void setPoolName(String value) {
    poolName = value;
  }

  /**
   * Gets the value of the diskStoreName property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the diskStoreName property
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
   * @param value the value of the diskStoreName property
   */
  public void setDiskStoreName(String value) {
    diskStoreName = value;
  }

  /**
   * Gets the value of the diskSynchronous property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the diskSynchronous property
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
   * @param value the value of the diskSynchronous property
   */
  public void setDiskSynchronous(Boolean value) {
    diskSynchronous = value;
  }

  /**
   * Gets the value of the publisher property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the publisher property
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
   * @param value the value of the publisher property
   */
  public void setPublisher(Boolean value) {
    publisher = value;
  }

  /**
   * Gets the value of the refid property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the refid property
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
   * @param value the value of the refid property
   */
  public void setRefid(String value) {
    refid = value;
  }

  /**
   * Gets the value of the scope property.
   *
   * possible object is
   * {@link RegionAttributesScope }
   *
   * @return the value of the scope property
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
   * @param value the value of the scope property
   */
  public void setScope(RegionAttributesScope value) {
    scope = value;
  }

  /**
   * Gets the value of the statisticsEnabled property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the statisticsEnabled property
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
   * @param value the value of the statisticsEnabled property
   */
  public void setStatisticsEnabled(Boolean value) {
    statisticsEnabled = value;
  }

  /**
   * Gets the value of the cloningEnabled property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the cloningEnabled property
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
   * @param value the value of the cloningEnabled property
   */
  public void setCloningEnabled(Boolean value) {
    cloningEnabled = value;
  }

  /**
   * Gets the value of the concurrencyChecksEnabled property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the concurrencyChecksEnabled property
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
   * @param value the value of the concurrencyChecksEnabled property
   */
  public void setConcurrencyChecksEnabled(Boolean value) {
    concurrencyChecksEnabled = value;
  }

  /**
   * Gets the value of the offHeap property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the offHeap property
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
   * @param value the value of the offHeap property
   */
  public void setOffHeap(Boolean value) {
    offHeap = value;
  }

  public void setLruHeapPercentageEvictionAction(EnumActionDestroyOverflow action) {
    if (evictionAttributes == null) {
      evictionAttributes = new EvictionAttributes();
    }
    EvictionAttributes.LruHeapPercentage lruHeapPercentage =
        new EvictionAttributes.LruHeapPercentage();
    lruHeapPercentage.setAction(action);
    evictionAttributes.setLruHeapPercentage(lruHeapPercentage);
  }

  public void setInterestPolicy(String interestPolicy) {
    if (subscriptionAttributes == null) {
      subscriptionAttributes = new SubscriptionAttributes();
    }
    subscriptionAttributes.setInterestPolicy(interestPolicy);
  }

  public void setRedundantCopy(String copies) {
    if (partitionAttributes == null) {
      partitionAttributes = new PartitionAttributes();
    }
    partitionAttributes.setRedundantCopies(copies);
  }

  public void setLocalMaxMemory(String maxMemory) {
    if (partitionAttributes == null) {
      partitionAttributes = new PartitionAttributes();
    }
    partitionAttributes.setLocalMaxMemory(maxMemory);
  }


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
   *       &lt;choice&gt;
   *         &lt;element name="asynchronous-writes"&gt;
   *           &lt;complexType&gt;
   *             &lt;complexContent&gt;
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                 &lt;attribute name="bytes-threshold" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                 &lt;attribute name="time-interval" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *               &lt;/restriction&gt;
   *             &lt;/complexContent&gt;
   *           &lt;/complexType&gt;
   *         &lt;/element&gt;
   *         &lt;element name="synchronous-writes" type="{http://www.w3.org/2001/XMLSchema}anytype"/&gt;
   *       &lt;/choice&gt;
   *       &lt;attribute name="max-oplog-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="roll-oplogs" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *     &lt;/restriction&gt;
   *   &lt;/complexContent&gt;
   * &lt;/complexType&gt;
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"asynchronousWrites", "synchronousWrites"})
  public static class DiskWriteAttributes implements Serializable {

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
     * @return the value of the asynchronousWrites property
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
     * @param value the value of the asynchronousWrites property
     */
    public void setAsynchronousWrites(
        RegionAttributesType.DiskWriteAttributes.AsynchronousWrites value) {
      asynchronousWrites = value;
    }

    /**
     * Gets the value of the synchronousWrites property.
     *
     * possible object is
     * {@link Object }
     *
     * @return the value of the synchronousWrites property
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
     * @param value the value of the synchronousWrites property
     */
    public void setSynchronousWrites(Object value) {
      synchronousWrites = value;
    }

    /**
     * Gets the value of the maxOplogSize property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the maxOplogSize property
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
     * @param value the value of the maxOplogSize property
     */
    public void setMaxOplogSize(String value) {
      maxOplogSize = value;
    }

    /**
     * Gets the value of the rollOplogs property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the rollOplogs property
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
     * @param value the value of the rollOplogs property
     */
    public void setRollOplogs(String value) {
      rollOplogs = value;
    }


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
     *       &lt;attribute name="bytes-threshold" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *       &lt;attribute name="time-interval" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *     &lt;/restriction&gt;
     *   &lt;/complexContent&gt;
     * &lt;/complexType&gt;
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
       * @return the value of the bytesThreshold property
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
       * @param value the value of the bytesThreshold property
       */
      public void setBytesThreshold(String value) {
        bytesThreshold = value;
      }

      /**
       * Gets the value of the timeInterval property.
       *
       * possible object is
       * {@link String }
       *
       * @return the value of the timeInterval property
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
       * @param value the value of the timeInterval property
       */
      public void setTimeInterval(String value) {
        timeInterval = value;
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
   * &lt;complexType&gt;
   *   &lt;complexContent&gt;
   *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *       &lt;sequence&gt;
   *         &lt;element name="expiration-attributes" type="{http://geode.apache.org/schema/cache}expiration-attributes-type"/&gt;
   *       &lt;/sequence&gt;
   *     &lt;/restriction&gt;
   *   &lt;/complexContent&gt;
   * &lt;/complexType&gt;
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

    public ExpirationAttributesType() {}

    public ExpirationAttributesType(Integer timeout, String action, String expiry,
        Properties iniProps) {
      expirationAttributes.setTimeout(Objects.toString(timeout, null));
      if (action != null) {
        expirationAttributes.setAction(action);
      }
      if (expiry != null) {
        expirationAttributes.setCustomExpiry(new DeclarableType(expiry, iniProps));
      }
    }

    public static ExpirationAttributesType generate(Integer timeout,
        String action, ClassName expiry) {
      if (timeout == null && action == null && expiry == null) {
        return null;
      }
      if (expiry != null) {
        return new ExpirationAttributesType(timeout, action,
            expiry.getClassName(), expiry.getInitProperties());
      } else {
        return new ExpirationAttributesType(timeout, action, null, null);
      }
    }

    // this is a helper method to combine the existing with the delta ExpirationAttributesType
    public static ExpirationAttributesType combine(ExpirationAttributesType existing,
        ExpirationAttributesType delta) {
      if (delta == null) {
        return existing;
      }

      if (existing == null) {
        existing = new ExpirationAttributesType();
        existing.setAction("invalidate");
        existing.setTimeout("0");
      }

      if (delta.getTimeout() != null) {
        existing.setTimeout(delta.getTimeout());
      }
      if (delta.getAction() != null) {
        existing.setAction(delta.getAction());
      }
      if (delta.getCustomExpiry() != null) {
        if (delta.getCustomExpiry().equals(DeclarableType.EMPTY)) {
          existing.setCustomExpiry(null);
        } else {
          existing.setCustomExpiry(delta.getCustomExpiry());
        }
      }
      return existing;
    }

    /**
     * @return true if timeout or action is specified
     */
    public boolean hasTimoutOrAction() {
      return (getTimeout() != null || getAction() != null);
    }

    /**
     * @return true if custom expiry class is specified
     */
    public boolean hasCustomExpiry() {
      return getCustomExpiry() != null;
    }

    /**
     * @return the custom expiry declarable
     */
    public DeclarableType getCustomExpiry() {
      return expirationAttributes.getCustomExpiry();
    }

    /**
     * Sets the value of the customExpiry property.
     *
     * allowed object is
     * {@link DeclarableType }
     *
     * @param value the value of the customExpiry property
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
     * @return the value of the action property
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
     * @param value the value of the action property
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
     * @return the value of the timeout property
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
     * @param value the value of the timeout property
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

    @Immutable
    private static final List<String> ALLOWED_ACTIONS = Collections.unmodifiableList(
        Arrays.asList("invalidate", "destroy", "local-invalidate", "local-destroy"));

    /**
     * Gets the value of the customExpiry property.
     *
     * possible object is
     * {@link DeclarableType }
     *
     * @return the value of the customExpiry property
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
     * @param value the value of the customExpiry property
     */
    public void setCustomExpiry(DeclarableType value) {
      customExpiry = value;
    }

    /**
     * Gets the value of the action property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the action property
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
     * @param value the value of the action property
     */
    public void setAction(String value) {
      if (!ALLOWED_ACTIONS.contains(value)) {
        throw new IllegalArgumentException("invalid expiration action: " + value);
      }
      action = value;
    }

    /**
     * Gets the value of the timeout property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the timeout property
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
     * @param value the value of the timeout property
     */
    public void setTimeout(String value) {
      timeout = value;
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
   * &lt;complexType&gt;
   *   &lt;complexContent&gt;
   *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *       &lt;choice&gt;
   *         &lt;element name="lru-entry-count"&gt;
   *           &lt;complexType&gt;
   *             &lt;complexContent&gt;
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                 &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" /&gt;
   *                 &lt;attribute name="maximum" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *               &lt;/restriction&gt;
   *             &lt;/complexContent&gt;
   *           &lt;/complexType&gt;
   *         &lt;/element&gt;
   *         &lt;element name="lru-heap-percentage"&gt;
   *           &lt;complexType&gt;
   *             &lt;complexContent&gt;
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                 &lt;sequence minOccurs="0"&gt;
   *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
   *                   &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
   *                 &lt;/sequence&gt;
   *                 &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" /&gt;
   *               &lt;/restriction&gt;
   *             &lt;/complexContent&gt;
   *           &lt;/complexType&gt;
   *         &lt;/element&gt;
   *         &lt;element name="lru-memory-size"&gt;
   *           &lt;complexType&gt;
   *             &lt;complexContent&gt;
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                 &lt;sequence minOccurs="0"&gt;
   *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
   *                   &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
   *                 &lt;/sequence&gt;
   *                 &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" /&gt;
   *                 &lt;attribute name="maximum" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *               &lt;/restriction&gt;
   *             &lt;/complexContent&gt;
   *           &lt;/complexType&gt;
   *         &lt;/element&gt;
   *       &lt;/choice&gt;
   *     &lt;/restriction&gt;
   *   &lt;/complexContent&gt;
   * &lt;/complexType&gt;
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

    public static EvictionAttributes generate(String evictionAction,
        Integer maxMemory, Integer maxEntryCount,
        ClassName objectSizer) {
      if (evictionAction == null && maxMemory == null && maxEntryCount == null
          && objectSizer == null) {
        return null;
      }

      RegionAttributesType.EvictionAttributes evictionAttributes =
          new RegionAttributesType.EvictionAttributes();
      EnumActionDestroyOverflow action = EnumActionDestroyOverflow.fromValue(evictionAction);

      if (maxMemory == null && maxEntryCount == null) {
        LruHeapPercentage heapPercentage =
            new LruHeapPercentage();
        heapPercentage.setAction(action);
        if (objectSizer != null) {
          heapPercentage.setClassName(objectSizer.getClassName());
          heapPercentage.setParameters(objectSizer.getInitProperties());
        }
        evictionAttributes.setLruHeapPercentage(heapPercentage);
      } else if (maxMemory != null) {
        LruMemorySize memorySize =
            new LruMemorySize();
        memorySize.setAction(action);
        if (objectSizer != null) {
          memorySize.setClassName(objectSizer.getClassName());
          memorySize.setParameters(objectSizer.getInitProperties());
        }
        memorySize.setMaximum(maxMemory.toString());
        evictionAttributes.setLruMemorySize(memorySize);
      } else {
        LruEntryCount entryCount =
            new LruEntryCount();
        entryCount.setAction(action);
        entryCount.setMaximum(maxEntryCount.toString());
        evictionAttributes.setLruEntryCount(entryCount);
      }
      return evictionAttributes;
    }

    public static EvictionAttributes generate(String evictionAction,
        Integer maxMemory, Integer maxEntryCount,
        String objectSizer) {
      if (objectSizer == null) {
        return generate(evictionAction, maxMemory, maxEntryCount, (ClassName) null);
      }
      return generate(evictionAction, maxMemory, maxEntryCount, new ClassName(objectSizer));
    }

    /**
     * Gets the value of the lruEntryCount property.
     *
     * possible object is
     * {@link RegionAttributesType.EvictionAttributes.LruEntryCount }
     *
     * @return the value of the lruEntryCount property
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
     * @param value the value of the lruEntryCount property
     */
    public void setLruEntryCount(RegionAttributesType.EvictionAttributes.LruEntryCount value) {
      lruEntryCount = value;
    }

    /**
     * Gets the value of the lruHeapPercentage property.
     *
     * possible object is
     * {@link RegionAttributesType.EvictionAttributes.LruHeapPercentage }
     *
     * @return the value of the lruHeapPercentage property
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
     * @param value the value of the lruHeapPercentage property
     */
    public void setLruHeapPercentage(
        RegionAttributesType.EvictionAttributes.LruHeapPercentage value) {
      lruHeapPercentage = value;
    }

    /**
     * Gets the value of the lruMemorySize property.
     *
     * possible object is
     * {@link RegionAttributesType.EvictionAttributes.LruMemorySize }
     *
     * @return the value of the lruMemorySize property
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
     * @param value the value of the lruMemorySize property
     */
    public void setLruMemorySize(RegionAttributesType.EvictionAttributes.LruMemorySize value) {
      lruMemorySize = value;
    }


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
     *       &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" /&gt;
     *       &lt;attribute name="maximum" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *     &lt;/restriction&gt;
     *   &lt;/complexContent&gt;
     * &lt;/complexType&gt;
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
       * @return the value of the action property
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
       * @param value the value of the action property
       */
      public void setAction(EnumActionDestroyOverflow value) {
        action = value;
      }

      /**
       * Gets the value of the maximum property.
       *
       * possible object is
       * {@link String }
       *
       * @return the value of the maximum property
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
       * @param value the value of the maximum property
       */
      public void setMaximum(String value) {
        maximum = value;
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
     * &lt;complexType&gt;
     *   &lt;complexContent&gt;
     *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
     *       &lt;sequence minOccurs="0"&gt;
     *         &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
     *         &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
     *       &lt;/sequence&gt;
     *       &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" /&gt;
     *     &lt;/restriction&gt;
     *   &lt;/complexContent&gt;
     * &lt;/complexType&gt;
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
       * @return the value of the action property
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
       * @param value the value of the action property
       */
      public void setAction(EnumActionDestroyOverflow value) {
        action = value;
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
     * &lt;complexType&gt;
     *   &lt;complexContent&gt;
     *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
     *       &lt;sequence minOccurs="0"&gt;
     *         &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
     *         &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
     *       &lt;/sequence&gt;
     *       &lt;attribute name="action" type="{http://geode.apache.org/schema/cache}enum-action-destroy-overflow" /&gt;
     *       &lt;attribute name="maximum" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *     &lt;/restriction&gt;
     *   &lt;/complexContent&gt;
     * &lt;/complexType&gt;
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
       * @return the value of the maximum property
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
       * @param value the value of the maximum property
       */
      public void setMaximum(String value) {
        maximum = value;
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
   * &lt;complexType&gt;
   *   &lt;complexContent&gt;
   *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *       &lt;sequence&gt;
   *         &lt;element name="required-role" maxOccurs="unbounded" minOccurs="0"&gt;
   *           &lt;complexType&gt;
   *             &lt;complexContent&gt;
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                 &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *               &lt;/restriction&gt;
   *             &lt;/complexContent&gt;
   *           &lt;/complexType&gt;
   *         &lt;/element&gt;
   *       &lt;/sequence&gt;
   *       &lt;attribute name="loss-action"&gt;
   *         &lt;simpleType&gt;
   *           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
   *             &lt;enumeration value="full-access"/&gt;
   *             &lt;enumeration value="limited-access"/&gt;
   *             &lt;enumeration value="no-access"/&gt;
   *             &lt;enumeration value="reconnect"/&gt;
   *           &lt;/restriction&gt;
   *         &lt;/simpleType&gt;
   *       &lt;/attribute&gt;
   *       &lt;attribute name="resumption-action"&gt;
   *         &lt;simpleType&gt;
   *           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
   *             &lt;enumeration value="none"/&gt;
   *             &lt;enumeration value="reinitialize"/&gt;
   *           &lt;/restriction&gt;
   *         &lt;/simpleType&gt;
   *       &lt;/attribute&gt;
   *     &lt;/restriction&gt;
   *   &lt;/complexContent&gt;
   * &lt;/complexType&gt;
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"requiredRoles"})
  public static class MembershipAttributes implements Serializable {

    @XmlElement(name = "required-role", namespace = "http://geode.apache.org/schema/cache")
    protected List<RequiredRole> requiredRoles;
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
     * @return the value of the requiredRole property
     */
    public List<RequiredRole> getRequiredRoles() {
      if (requiredRoles == null) {
        requiredRoles = new ArrayList<>();
      }
      return requiredRoles;
    }

    /**
     * Gets the value of the lossAction property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the lossAction property
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
     * @param value the value of the lossAction property
     */
    public void setLossAction(String value) {
      lossAction = value;
    }

    /**
     * Gets the value of the resumptionAction property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the resumptionAction property
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
     * @param value the value of the resumptionAction property
     */
    public void setResumptionAction(String value) {
      resumptionAction = value;
    }


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
     *       &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *     &lt;/restriction&gt;
     *   &lt;/complexContent&gt;
     * &lt;/complexType&gt;
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
       * @return the value of the name property
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
       * @param value the value of the name property
       */
      public void setName(String value) {
        name = value;
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
   * &lt;complexType&gt;
   *   &lt;complexContent&gt;
   *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *       &lt;sequence&gt;
   *         &lt;element name="partition-resolver" minOccurs="0"&gt;
   *           &lt;complexType&gt;
   *             &lt;complexContent&gt;
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                 &lt;sequence&gt;
   *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
   *                   &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
   *                 &lt;/sequence&gt;
   *                 &lt;attribute name="name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *               &lt;/restriction&gt;
   *             &lt;/complexContent&gt;
   *           &lt;/complexType&gt;
   *         &lt;/element&gt;
   *         &lt;element name="partition-listener" maxOccurs="unbounded" minOccurs="0"&gt;
   *           &lt;complexType&gt;
   *             &lt;complexContent&gt;
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                 &lt;sequence&gt;
   *                   &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
   *                   &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
   *                 &lt;/sequence&gt;
   *               &lt;/restriction&gt;
   *             &lt;/complexContent&gt;
   *           &lt;/complexType&gt;
   *         &lt;/element&gt;
   *         &lt;element name="fixed-partition-attributes" maxOccurs="unbounded" minOccurs="0"&gt;
   *           &lt;complexType&gt;
   *             &lt;complexContent&gt;
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                 &lt;attribute name="partition-name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                 &lt;attribute name="is-primary" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *                 &lt;attribute name="num-buckets" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *               &lt;/restriction&gt;
   *             &lt;/complexContent&gt;
   *           &lt;/complexType&gt;
   *         &lt;/element&gt;
   *       &lt;/sequence&gt;
   *       &lt;attribute name="local-max-memory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="recovery-delay" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="redundant-copies" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="startup-recovery-delay" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="total-max-memory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="total-num-buckets" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="colocated-with" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *     &lt;/restriction&gt;
   *   &lt;/complexContent&gt;
   * &lt;/complexType&gt;
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
    protected List<FixedPartitionAttributes> fixedPartitionAttributes;
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

    public static PartitionAttributes generate(String partitionResolver,
        List<String> partitionListeners, Integer localMaxMemory,
        Long recoveryDelay, Integer redundantCopies,
        Long startupRecoveryDelay, Long totalMaxMemory,
        Integer totalNumBuckets, String colocatedWith) {
      if (partitionResolver == null &&
          (partitionListeners == null || partitionListeners.isEmpty()) &&
          localMaxMemory == null &&
          recoveryDelay == null &&
          redundantCopies == null &&
          startupRecoveryDelay == null &&
          totalMaxMemory == null &&
          totalNumBuckets == null &&
          colocatedWith == null) {
        return null;
      }

      RegionAttributesType.PartitionAttributes partitionAttributes =
          new RegionAttributesType.PartitionAttributes();
      partitionAttributes.setColocatedWith(colocatedWith);
      partitionAttributes.setLocalMaxMemory(Objects.toString(localMaxMemory, null));
      partitionAttributes.setTotalMaxMemory(Objects.toString(totalMaxMemory, null));
      partitionAttributes.setRecoveryDelay(Objects.toString(recoveryDelay, null));
      partitionAttributes.setRedundantCopies(Objects.toString(redundantCopies, null));
      partitionAttributes.setStartupRecoveryDelay(Objects.toString(startupRecoveryDelay, null));
      partitionAttributes.setTotalNumBuckets(Objects.toString(totalNumBuckets, null));
      if (partitionResolver != null) {
        partitionAttributes.setPartitionResolver(new DeclarableType(partitionResolver));
      }

      if (partitionListeners != null) {
        partitionListeners.stream().map(DeclarableType::new)
            .forEach(partitionAttributes.getPartitionListeners()::add);
      }
      return partitionAttributes;
    }

    public static PartitionAttributes combine(PartitionAttributes existing,
        PartitionAttributes delta) {
      if (existing == null) {
        return delta;
      }
      if (delta == null) {
        return existing;
      }

      if (delta.getRedundantCopies() != null) {
        existing.setRedundantCopies(delta.getRedundantCopies());
      }

      if (delta.getPartitionListeners() != null) {
        existing.getPartitionListeners().clear();
        existing.getPartitionListeners().addAll(delta.getPartitionListeners());
      }

      if (delta.getColocatedWith() != null) {
        existing.setColocatedWith(delta.getColocatedWith());
      }

      if (delta.getLocalMaxMemory() != null) {
        existing.setLocalMaxMemory(delta.getLocalMaxMemory());
      }

      if (delta.getPartitionResolver() != null) {
        existing.setPartitionResolver(delta.getPartitionResolver());
      }

      if (delta.getRecoveryDelay() != null) {
        existing.setRecoveryDelay(delta.getRecoveryDelay());
      }

      if (delta.getStartupRecoveryDelay() != null) {
        existing.setStartupRecoveryDelay(delta.getStartupRecoveryDelay());
      }

      if (delta.getTotalMaxMemory() != null) {
        existing.setTotalMaxMemory(delta.getTotalMaxMemory());
      }

      if (delta.getTotalNumBuckets() != null) {
        existing.setTotalNumBuckets(delta.getTotalNumBuckets());
      }

      if (delta.getFixedPartitionAttributes() != null) {
        existing.getFixedPartitionAttributes().clear();
        existing.getFixedPartitionAttributes().addAll(delta.getFixedPartitionAttributes());
      }
      return existing;
    }

    /**
     * Gets the value of the partitionResolver property.
     *
     * possible object is
     * {@link DeclarableType }
     *
     * @return the value of the partitionResolver property
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
     * @param value the value of the partitionResolver property
     */
    public void setPartitionResolver(DeclarableType value) {
      partitionResolver = value;
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
     * @return the value of the partitionListener property
     */
    public List<DeclarableType> getPartitionListeners() {
      if (partitionListeners == null) {
        partitionListeners = new ArrayList<>();
      }
      return partitionListeners;
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
     * @return the value of the fixedPartitionAttributes property
     */
    public List<FixedPartitionAttributes> getFixedPartitionAttributes() {
      if (fixedPartitionAttributes == null) {
        fixedPartitionAttributes =
            new ArrayList<>();
      }
      return fixedPartitionAttributes;
    }

    /**
     * Gets the value of the localMaxMemory property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the localMaxMemory property
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
     * @param value the value of the localMaxMemory property
     */
    public void setLocalMaxMemory(String value) {
      localMaxMemory = value;
    }

    /**
     * Gets the value of the recoveryDelay property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the recoveryDelay property
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
     * @param value the value of the recoveryDelay property
     */
    public void setRecoveryDelay(String value) {
      recoveryDelay = value;
    }

    /**
     * Gets the value of the redundantCopies property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the redundantCopies property
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
     * @param value the value of the redundantCopies property
     */
    public void setRedundantCopies(String value) {
      redundantCopies = value;
    }

    /**
     * Gets the value of the startupRecoveryDelay property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the startupRecoveryDelay property
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
     * @param value the value of the startupRecoveryDelay property
     */
    public void setStartupRecoveryDelay(String value) {
      startupRecoveryDelay = value;
    }

    /**
     * Gets the value of the totalMaxMemory property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the totalMaxMemory property
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
     * @param value the value of the totalMaxMemory property
     */
    public void setTotalMaxMemory(String value) {
      totalMaxMemory = value;
    }

    /**
     * Gets the value of the totalNumBuckets property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the totalNumBuckets property
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
     * @param value the value of the totalNumBuckets property
     */
    public void setTotalNumBuckets(String value) {
      totalNumBuckets = value;
    }

    /**
     * Gets the value of the colocatedWith property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the colocatedWith property
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
     * @param value the value of the colocatedWith property
     */
    public void setColocatedWith(String value) {
      colocatedWith = value;
    }


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
     *       &lt;attribute name="partition-name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *       &lt;attribute name="is-primary" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
     *       &lt;attribute name="num-buckets" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *     &lt;/restriction&gt;
     *   &lt;/complexContent&gt;
     * &lt;/complexType&gt;
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
       * @return the value of the partitionName property
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
       * @param value the value of the partitionName property
       */
      public void setPartitionName(String value) {
        partitionName = value;
      }

      /**
       * Gets the value of the isPrimary property.
       *
       * possible object is
       * {@link Boolean }
       *
       * @return the value of the isPrimary property
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
       * @param value the value of the isPrimary property
       */
      public void setIsPrimary(Boolean value) {
        isPrimary = value;
      }

      /**
       * Gets the value of the numBuckets property.
       *
       * possible object is
       * {@link String }
       *
       * @return the value of the numBuckets property
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
       * @param value the value of the numBuckets property
       */
      public void setNumBuckets(String value) {
        numBuckets = value;
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
   * &lt;complexType&gt;
   *   &lt;complexContent&gt;
   *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *       &lt;attribute name="interest-policy"&gt;
   *         &lt;simpleType&gt;
   *           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
   *             &lt;enumeration value="all"/&gt;
   *             &lt;enumeration value="cache-content"/&gt;
   *           &lt;/restriction&gt;
   *         &lt;/simpleType&gt;
   *       &lt;/attribute&gt;
   *     &lt;/restriction&gt;
   *   &lt;/complexContent&gt;
   * &lt;/complexType&gt;
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
     * @return the value of the interestPolicy property
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
     * @param value the value of the interestPolicy property
     */
    public void setInterestPolicy(String value) {
      interestPolicy = value;
    }

  }

}
