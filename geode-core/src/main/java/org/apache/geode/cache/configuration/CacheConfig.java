
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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.CollapsedStringAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.w3c.dom.Element;

import org.apache.geode.annotations.Experimental;


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
 *         &lt;element name="cache-transaction-manager" type="{http://geode.apache.org/schema/cache}cache-transaction-manager-type" minOccurs="0"/>
 *         &lt;element name="dynamic-region-factory" type="{http://geode.apache.org/schema/cache}dynamic-region-factory-type" minOccurs="0"/>
 *         &lt;element name="gateway-hub" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="gateway" maxOccurs="unbounded" minOccurs="0">
 *                     &lt;complexType>
 *                       &lt;complexContent>
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                           &lt;sequence>
 *                             &lt;choice>
 *                               &lt;element name="gateway-endpoint" maxOccurs="unbounded">
 *                                 &lt;complexType>
 *                                   &lt;complexContent>
 *                                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                                       &lt;attribute name="host" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                                       &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                                       &lt;attribute name="port" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                                     &lt;/restriction>
 *                                   &lt;/complexContent>
 *                                 &lt;/complexType>
 *                               &lt;/element>
 *                               &lt;element name="gateway-listener" maxOccurs="unbounded">
 *                                 &lt;complexType>
 *                                   &lt;complexContent>
 *                                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                                       &lt;sequence>
 *                                         &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
 *                                         &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
 *                                       &lt;/sequence>
 *                                     &lt;/restriction>
 *                                   &lt;/complexContent>
 *                                 &lt;/complexType>
 *                               &lt;/element>
 *                             &lt;/choice>
 *                             &lt;element name="gateway-queue" minOccurs="0">
 *                               &lt;complexType>
 *                                 &lt;complexContent>
 *                                   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                                     &lt;attribute name="alert-threshold" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                                     &lt;attribute name="batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                                     &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                                     &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                                     &lt;attribute name="enable-persistence" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                                     &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                                     &lt;attribute name="roll-oplogs" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                                     &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                                     &lt;attribute name="overflow-directory" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                                   &lt;/restriction>
 *                                 &lt;/complexContent>
 *                               &lt;/complexType>
 *                             &lt;/element>
 *                           &lt;/sequence>
 *                           &lt;attribute name="early-ack" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                           &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                           &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                           &lt;attribute name="socket-read-timeout" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                           &lt;attribute name="concurrency-level" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                           &lt;attribute name="order-policy" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                         &lt;/restriction>
 *                       &lt;/complexContent>
 *                     &lt;/complexType>
 *                   &lt;/element>
 *                 &lt;/sequence>
 *                 &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="bind-address" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="maximum-time-between-pings" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="port" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="startup-policy">
 *                   &lt;simpleType>
 *                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *                       &lt;enumeration value="primary"/>
 *                       &lt;enumeration value="secondary"/>
 *                       &lt;enumeration value="none"/>
 *                     &lt;/restriction>
 *                   &lt;/simpleType>
 *                 &lt;/attribute>
 *                 &lt;attribute name="manual-start" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                 &lt;attribute name="max-connections" type="{http://www.w3.org/2001/XMLSchema}integer" />
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="gateway-sender" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="gateway-event-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" maxOccurs="unbounded" minOccurs="0"/>
 *                   &lt;element name="gateway-event-substitution-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" minOccurs="0"/>
 *                   &lt;element name="gateway-transport-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" maxOccurs="unbounded" minOccurs="0"/>
 *                 &lt;/sequence>
 *                 &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="remote-distributed-system-id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="parallel" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                 &lt;attribute name="manual-start" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                 &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="socket-read-timeout" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="enable-batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                 &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="enable-persistence" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                 &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="disk-synchronous" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                 &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="alert-threshold" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="dispatcher-threads" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="order-policy" type="{http://www.w3.org/2001/XMLSchema}string" />
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="gateway-receiver" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="gateway-transport-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" maxOccurs="unbounded" minOccurs="0"/>
 *                 &lt;/sequence>
 *                 &lt;attribute name="start-port" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="end-port" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="bind-address" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="maximum-time-between-pings" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="hostname-for-senders" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="manual-start" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="gateway-conflict-resolver" minOccurs="0">
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
 *         &lt;element name="async-event-queue" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="gateway-event-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" maxOccurs="unbounded" minOccurs="0"/>
 *                   &lt;element name="gateway-event-substitution-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" minOccurs="0"/>
 *                   &lt;element name="async-event-listener" type="{http://geode.apache.org/schema/cache}class-with-parameters-type"/>
 *                 &lt;/sequence>
 *                 &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="parallel" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                 &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="enable-batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                 &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="persistent" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                 &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="disk-synchronous" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *                 &lt;attribute name="dispatcher-threads" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="order-policy" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="forward-expiration-destroy" type="{http://www.w3.org/2001/XMLSchema}boolean" default="false" />
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="cache-server" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;extension base="{http://geode.apache.org/schema/cache}server-type">
 *                 &lt;attribute name="tcp-no-delay" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *               &lt;/extension>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="pool" type="{http://geode.apache.org/schema/cache}pool-type" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="disk-store" type="{http://geode.apache.org/schema/cache}disk-store-type" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="pdx" type="{http://geode.apache.org/schema/cache}pdx-type" minOccurs="0"/>
 *         &lt;element name="region-attributes" type="{http://geode.apache.org/schema/cache}region-attributes-type" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;choice maxOccurs="unbounded" minOccurs="0">
 *           &lt;element name="jndi-bindings" type="{http://geode.apache.org/schema/cache}jndi-bindings-type"/>
 *           &lt;element name="region" type="{http://geode.apache.org/schema/cache}region-type"/>
 *           &lt;element name="vm-root-region" type="{http://geode.apache.org/schema/cache}region-type"/>
 *         &lt;/choice>
 *         &lt;element name="function-service" type="{http://geode.apache.org/schema/cache}function-service-type" minOccurs="0"/>
 *         &lt;element name="resource-manager" type="{http://geode.apache.org/schema/cache}resource-manager-type" minOccurs="0"/>
 *         &lt;element name="serialization-registration" type="{http://geode.apache.org/schema/cache}serialization-registration-type" minOccurs="0"/>
 *         &lt;element name="backup" type="{http://www.w3.org/2001/XMLSchema}string" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="initializer" type="{http://geode.apache.org/schema/cache}initializer-type" minOccurs="0"/>
 *         &lt;any processContents='lax' namespace='##other' maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *       &lt;attribute name="copy-on-read" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="is-server" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="lock-timeout" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="lock-lease" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="message-sync-interval" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="search-timeout" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="version" use="required" type="{http://geode.apache.org/schema/cache}versionType" fixed="1.0" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "",
    propOrder = {"cacheTransactionManager", "dynamicRegionFactory", "gatewayHub", "gatewaySender",
        "gatewayReceiver", "gatewayConflictResolver", "asyncEventQueue", "cacheServer", "pool",
        "diskStore", "pdx", "regionAttributes", "jndiBindings", "region", "functionService",
        "resourceManager", "serializationRegistration", "backup", "initializer", "cacheElements"})
@XmlRootElement(name = "cache", namespace = "http://geode.apache.org/schema/cache")
@Experimental
public class CacheConfig {

  @XmlElement(name = "cache-transaction-manager",
      namespace = "http://geode.apache.org/schema/cache")
  protected CacheTransactionManagerType cacheTransactionManager;
  @XmlElement(name = "dynamic-region-factory", namespace = "http://geode.apache.org/schema/cache")
  protected DynamicRegionFactoryType dynamicRegionFactory;
  @XmlElement(name = "gateway-hub", namespace = "http://geode.apache.org/schema/cache")
  protected List<CacheConfig.GatewayHub> gatewayHub;
  @XmlElement(name = "gateway-sender", namespace = "http://geode.apache.org/schema/cache")
  protected List<CacheConfig.GatewaySender> gatewaySender;
  @XmlElement(name = "gateway-receiver", namespace = "http://geode.apache.org/schema/cache")
  protected CacheConfig.GatewayReceiver gatewayReceiver;
  @XmlElement(name = "gateway-conflict-resolver",
      namespace = "http://geode.apache.org/schema/cache")
  protected CacheConfig.GatewayConflictResolver gatewayConflictResolver;
  @XmlElement(name = "async-event-queue", namespace = "http://geode.apache.org/schema/cache")
  protected List<CacheConfig.AsyncEventQueue> asyncEventQueue;
  @XmlElement(name = "cache-server", namespace = "http://geode.apache.org/schema/cache")
  protected List<CacheConfig.CacheServer> cacheServer;
  @XmlElement(namespace = "http://geode.apache.org/schema/cache")
  protected List<PoolType> pool;
  @XmlElement(name = "disk-store", namespace = "http://geode.apache.org/schema/cache")
  protected List<DiskStoreType> diskStore;
  @XmlElement(namespace = "http://geode.apache.org/schema/cache")
  protected PdxType pdx;
  @XmlElement(name = "region-attributes", namespace = "http://geode.apache.org/schema/cache")
  protected List<RegionAttributesType> regionAttributes;
  @XmlElement(name = "jndi-bindings", namespace = "http://geode.apache.org/schema/cache")
  protected JndiBindingsType jndiBindings;
  @XmlElement(namespace = "http://geode.apache.org/schema/cache")
  protected List<RegionConfig> region;
  @XmlElement(name = "function-service", namespace = "http://geode.apache.org/schema/cache")
  protected FunctionServiceType functionService;
  @XmlElement(name = "resource-manager", namespace = "http://geode.apache.org/schema/cache")
  protected ResourceManagerType resourceManager;
  @XmlElement(name = "serialization-registration",
      namespace = "http://geode.apache.org/schema/cache")
  protected SerializationRegistrationType serializationRegistration;
  @XmlElement(namespace = "http://geode.apache.org/schema/cache")
  protected List<String> backup;
  @XmlElement(namespace = "http://geode.apache.org/schema/cache")
  protected InitializerType initializer;
  @XmlAnyElement(lax = true)
  protected List<CacheElement> cacheElements;
  @XmlAttribute(name = "copy-on-read")
  protected Boolean copyOnRead;
  @XmlAttribute(name = "is-server")
  protected Boolean isServer;
  @XmlAttribute(name = "lock-timeout")
  protected String lockTimeout;
  @XmlAttribute(name = "lock-lease")
  protected String lockLease;
  @XmlAttribute(name = "message-sync-interval")
  protected String messageSyncInterval;
  @XmlAttribute(name = "search-timeout")
  protected String searchTimeout;
  @XmlAttribute(name = "version", required = true)
  @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
  protected String version;

  public CacheConfig() {}

  public CacheConfig(String version) {
    this.version = version;
  }

  /**
   * Gets the value of the cacheTransactionManager property.
   *
   * @return
   *         possible object is
   *         {@link CacheTransactionManagerType }
   *
   */
  public CacheTransactionManagerType getCacheTransactionManager() {
    return cacheTransactionManager;
  }

  /**
   * Sets the value of the cacheTransactionManager property.
   *
   * @param value
   *        allowed object is
   *        {@link CacheTransactionManagerType }
   *
   */
  public void setCacheTransactionManager(CacheTransactionManagerType value) {
    this.cacheTransactionManager = value;
  }

  /**
   * Gets the value of the dynamicRegionFactory property.
   *
   * @return
   *         possible object is
   *         {@link DynamicRegionFactoryType }
   *
   */
  public DynamicRegionFactoryType getDynamicRegionFactory() {
    return dynamicRegionFactory;
  }

  /**
   * Sets the value of the dynamicRegionFactory property.
   *
   * @param value
   *        allowed object is
   *        {@link DynamicRegionFactoryType }
   *
   */
  public void setDynamicRegionFactory(DynamicRegionFactoryType value) {
    this.dynamicRegionFactory = value;
  }

  /**
   * Gets the value of the gatewayHub property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the gatewayHub property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getGatewayHub().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link CacheConfig.GatewayHub }
   *
   *
   */
  public List<CacheConfig.GatewayHub> getGatewayHub() {
    if (gatewayHub == null) {
      gatewayHub = new ArrayList<CacheConfig.GatewayHub>();
    }
    return this.gatewayHub;
  }

  /**
   * Gets the value of the gatewaySender property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the gatewaySender property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getGatewaySender().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link CacheConfig.GatewaySender }
   *
   *
   */
  public List<CacheConfig.GatewaySender> getGatewaySender() {
    if (gatewaySender == null) {
      gatewaySender = new ArrayList<CacheConfig.GatewaySender>();
    }
    return this.gatewaySender;
  }

  /**
   * Gets the value of the gatewayReceiver property.
   *
   * @return
   *         possible object is
   *         {@link CacheConfig.GatewayReceiver }
   *
   */
  public CacheConfig.GatewayReceiver getGatewayReceiver() {
    return gatewayReceiver;
  }

  /**
   * Sets the value of the gatewayReceiver property.
   *
   * @param value
   *        allowed object is
   *        {@link CacheConfig.GatewayReceiver }
   *
   */
  public void setGatewayReceiver(CacheConfig.GatewayReceiver value) {
    this.gatewayReceiver = value;
  }

  /**
   * Gets the value of the gatewayConflictResolver property.
   *
   * @return
   *         possible object is
   *         {@link CacheConfig.GatewayConflictResolver }
   *
   */
  public CacheConfig.GatewayConflictResolver getGatewayConflictResolver() {
    return gatewayConflictResolver;
  }

  /**
   * Sets the value of the gatewayConflictResolver property.
   *
   * @param value
   *        allowed object is
   *        {@link CacheConfig.GatewayConflictResolver }
   *
   */
  public void setGatewayConflictResolver(CacheConfig.GatewayConflictResolver value) {
    this.gatewayConflictResolver = value;
  }

  /**
   * Gets the value of the asyncEventQueue property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the asyncEventQueue property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getAsyncEventQueue().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link CacheConfig.AsyncEventQueue }
   *
   *
   */
  public List<CacheConfig.AsyncEventQueue> getAsyncEventQueue() {
    if (asyncEventQueue == null) {
      asyncEventQueue = new ArrayList<CacheConfig.AsyncEventQueue>();
    }
    return this.asyncEventQueue;
  }

  /**
   * Gets the value of the cacheServer property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the cacheServer property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getCacheServer().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link CacheConfig.CacheServer }
   *
   *
   */
  public List<CacheConfig.CacheServer> getCacheServer() {
    if (cacheServer == null) {
      cacheServer = new ArrayList<CacheConfig.CacheServer>();
    }
    return this.cacheServer;
  }

  /**
   * Gets the value of the pool property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the pool property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getPool().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link PoolType }
   *
   *
   */
  public List<PoolType> getPool() {
    if (pool == null) {
      pool = new ArrayList<PoolType>();
    }
    return this.pool;
  }

  /**
   * Gets the value of the diskStore property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the diskStore property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getDiskStore().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link DiskStoreType }
   *
   *
   */
  public List<DiskStoreType> getDiskStore() {
    if (diskStore == null) {
      diskStore = new ArrayList<DiskStoreType>();
    }
    return this.diskStore;
  }

  /**
   * Gets the value of the pdx property.
   *
   * @return
   *         possible object is
   *         {@link PdxType }
   *
   */
  public PdxType getPdx() {
    return pdx;
  }

  /**
   * Sets the value of the pdx property.
   *
   * @param value
   *        allowed object is
   *        {@link PdxType }
   *
   */
  public void setPdx(PdxType value) {
    this.pdx = value;
  }

  /**
   * Gets the value of the regionAttributes property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the regionAttributes property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getRegionAttributes().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link RegionAttributesType }
   *
   *
   */
  public List<RegionAttributesType> getRegionAttributes() {
    if (regionAttributes == null) {
      regionAttributes = new ArrayList<RegionAttributesType>();
    }
    return this.regionAttributes;
  }

  /**
   * Gets the value of the jndiBindings property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the jndiBindings property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getJndiBindings().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link JndiBindingsType }
   *
   *
   */
  public List<JndiBindingsType.JndiBinding> getJndiBindings() {
    if (jndiBindings == null) {
      jndiBindings = new JndiBindingsType();
    }
    return jndiBindings.getJndiBinding();
  }


  /**
   * Gets the value of the region property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the region property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getRegion().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link RegionConfig }
   *
   *
   */
  public List<RegionConfig> getRegion() {
    if (region == null) {
      region = new ArrayList<RegionConfig>();
    }
    return this.region;
  }

  /**
   * Gets the value of the functionService property.
   *
   * @return
   *         possible object is
   *         {@link FunctionServiceType }
   *
   */
  public FunctionServiceType getFunctionService() {
    return functionService;
  }

  /**
   * Sets the value of the functionService property.
   *
   * @param value
   *        allowed object is
   *        {@link FunctionServiceType }
   *
   */
  public void setFunctionService(FunctionServiceType value) {
    this.functionService = value;
  }

  /**
   * Gets the value of the resourceManager property.
   *
   * @return
   *         possible object is
   *         {@link ResourceManagerType }
   *
   */
  public ResourceManagerType getResourceManager() {
    return resourceManager;
  }

  /**
   * Sets the value of the resourceManager property.
   *
   * @param value
   *        allowed object is
   *        {@link ResourceManagerType }
   *
   */
  public void setResourceManager(ResourceManagerType value) {
    this.resourceManager = value;
  }

  /**
   * Gets the value of the serializationRegistration property.
   *
   * @return
   *         possible object is
   *         {@link SerializationRegistrationType }
   *
   */
  public SerializationRegistrationType getSerializationRegistration() {
    return serializationRegistration;
  }

  /**
   * Sets the value of the serializationRegistration property.
   *
   * @param value
   *        allowed object is
   *        {@link SerializationRegistrationType }
   *
   */
  public void setSerializationRegistration(SerializationRegistrationType value) {
    this.serializationRegistration = value;
  }

  /**
   * Gets the value of the backup property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the backup property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getBackup().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link String }
   *
   *
   */
  public List<String> getBackup() {
    if (backup == null) {
      backup = new ArrayList<String>();
    }
    return this.backup;
  }

  /**
   * Gets the value of the initializer property.
   *
   * @return
   *         possible object is
   *         {@link InitializerType }
   *
   */
  public InitializerType getInitializer() {
    return initializer;
  }

  /**
   * Sets the value of the initializer property.
   *
   * @param value
   *        allowed object is
   *        {@link InitializerType }
   *
   */
  public void setInitializer(InitializerType value) {
    this.initializer = value;
  }

  /**
   * Gets the value of the any property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the any property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getCustomCacheElements().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link Element }
   * {@link CacheElement }
   *
   *
   */
  public List<CacheElement> getCustomCacheElements() {
    if (cacheElements == null) {
      cacheElements = new ArrayList<CacheElement>();
    }
    return this.cacheElements;
  }

  /**
   * Gets the value of the copyOnRead property.
   *
   * @return
   *         possible object is
   *         {@link Boolean }
   *
   */
  public Boolean isCopyOnRead() {
    return copyOnRead;
  }

  /**
   * Sets the value of the copyOnRead property.
   *
   * @param value
   *        allowed object is
   *        {@link Boolean }
   *
   */
  public void setCopyOnRead(Boolean value) {
    this.copyOnRead = value;
  }

  /**
   * Gets the value of the isServer property.
   *
   * @return
   *         possible object is
   *         {@link Boolean }
   *
   */
  public Boolean isIsServer() {
    return isServer;
  }

  /**
   * Sets the value of the isServer property.
   *
   * @param value
   *        allowed object is
   *        {@link Boolean }
   *
   */
  public void setIsServer(Boolean value) {
    this.isServer = value;
  }

  /**
   * Gets the value of the lockTimeout property.
   *
   * @return
   *         possible object is
   *         {@link String }
   *
   */
  public String getLockTimeout() {
    return lockTimeout;
  }

  /**
   * Sets the value of the lockTimeout property.
   *
   * @param value
   *        allowed object is
   *        {@link String }
   *
   */
  public void setLockTimeout(String value) {
    this.lockTimeout = value;
  }

  /**
   * Gets the value of the lockLease property.
   *
   * @return
   *         possible object is
   *         {@link String }
   *
   */
  public String getLockLease() {
    return lockLease;
  }

  /**
   * Sets the value of the lockLease property.
   *
   * @param value
   *        allowed object is
   *        {@link String }
   *
   */
  public void setLockLease(String value) {
    this.lockLease = value;
  }

  /**
   * Gets the value of the messageSyncInterval property.
   *
   * @return
   *         possible object is
   *         {@link String }
   *
   */
  public String getMessageSyncInterval() {
    return messageSyncInterval;
  }

  /**
   * Sets the value of the messageSyncInterval property.
   *
   * @param value
   *        allowed object is
   *        {@link String }
   *
   */
  public void setMessageSyncInterval(String value) {
    this.messageSyncInterval = value;
  }

  /**
   * Gets the value of the searchTimeout property.
   *
   * @return
   *         possible object is
   *         {@link String }
   *
   */
  public String getSearchTimeout() {
    return searchTimeout;
  }

  /**
   * Sets the value of the searchTimeout property.
   *
   * @param value
   *        allowed object is
   *        {@link String }
   *
   */
  public void setSearchTimeout(String value) {
    this.searchTimeout = value;
  }

  /**
   * Gets the value of the version property.
   *
   * @return
   *         possible object is
   *         {@link String }
   *
   */
  public String getVersion() {
    if (version == null) {
      return "1.0";
    } else {
      return version;
    }
  }

  /**
   * Sets the value of the version property.
   *
   * @param value
   *        allowed object is
   *        {@link String }
   *
   */
  public void setVersion(String value) {
    this.version = value;
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
   *         &lt;element name="gateway-event-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" maxOccurs="unbounded" minOccurs="0"/>
   *         &lt;element name="gateway-event-substitution-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" minOccurs="0"/>
   *         &lt;element name="async-event-listener" type="{http://geode.apache.org/schema/cache}class-with-parameters-type"/>
   *       &lt;/sequence>
   *       &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="parallel" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *       &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="enable-batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *       &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="persistent" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *       &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="disk-synchronous" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *       &lt;attribute name="dispatcher-threads" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="order-policy" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="forward-expiration-destroy" type="{http://www.w3.org/2001/XMLSchema}boolean" default="false" />
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "",
      propOrder = {"gatewayEventFilter", "gatewayEventSubstitutionFilter", "asyncEventListener"})
  public static class AsyncEventQueue {

    @XmlElement(name = "gateway-event-filter", namespace = "http://geode.apache.org/schema/cache")
    protected List<ClassWithParametersType> gatewayEventFilter;
    @XmlElement(name = "gateway-event-substitution-filter",
        namespace = "http://geode.apache.org/schema/cache")
    protected ClassWithParametersType gatewayEventSubstitutionFilter;
    @XmlElement(name = "async-event-listener", namespace = "http://geode.apache.org/schema/cache",
        required = true)
    protected ClassWithParametersType asyncEventListener;
    @XmlAttribute(name = "id", required = true)
    protected String id;
    @XmlAttribute(name = "parallel")
    protected Boolean parallel;
    @XmlAttribute(name = "batch-size")
    protected String batchSize;
    @XmlAttribute(name = "batch-time-interval")
    protected String batchTimeInterval;
    @XmlAttribute(name = "enable-batch-conflation")
    protected Boolean enableBatchConflation;
    @XmlAttribute(name = "maximum-queue-memory")
    protected String maximumQueueMemory;
    @XmlAttribute(name = "persistent")
    protected Boolean persistent;
    @XmlAttribute(name = "disk-store-name")
    protected String diskStoreName;
    @XmlAttribute(name = "disk-synchronous")
    protected Boolean diskSynchronous;
    @XmlAttribute(name = "dispatcher-threads")
    protected String dispatcherThreads;
    @XmlAttribute(name = "order-policy")
    protected String orderPolicy;
    @XmlAttribute(name = "forward-expiration-destroy")
    protected Boolean forwardExpirationDestroy;

    /**
     * Gets the value of the gatewayEventFilter property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the gatewayEventFilter property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getGatewayEventFilter().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ClassWithParametersType }
     *
     *
     */
    public List<ClassWithParametersType> getGatewayEventFilter() {
      if (gatewayEventFilter == null) {
        gatewayEventFilter = new ArrayList<ClassWithParametersType>();
      }
      return this.gatewayEventFilter;
    }

    /**
     * Gets the value of the gatewayEventSubstitutionFilter property.
     *
     * @return
     *         possible object is
     *         {@link ClassWithParametersType }
     *
     */
    public ClassWithParametersType getGatewayEventSubstitutionFilter() {
      return gatewayEventSubstitutionFilter;
    }

    /**
     * Sets the value of the gatewayEventSubstitutionFilter property.
     *
     * @param value
     *        allowed object is
     *        {@link ClassWithParametersType }
     *
     */
    public void setGatewayEventSubstitutionFilter(ClassWithParametersType value) {
      this.gatewayEventSubstitutionFilter = value;
    }

    /**
     * Gets the value of the asyncEventListener property.
     *
     * @return
     *         possible object is
     *         {@link ClassWithParametersType }
     *
     */
    public ClassWithParametersType getAsyncEventListener() {
      return asyncEventListener;
    }

    /**
     * Sets the value of the asyncEventListener property.
     *
     * @param value
     *        allowed object is
     *        {@link ClassWithParametersType }
     *
     */
    public void setAsyncEventListener(ClassWithParametersType value) {
      this.asyncEventListener = value;
    }

    /**
     * Gets the value of the id property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getId() {
      return id;
    }

    /**
     * Sets the value of the id property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setId(String value) {
      this.id = value;
    }

    /**
     * Gets the value of the parallel property.
     *
     * @return
     *         possible object is
     *         {@link Boolean }
     *
     */
    public Boolean isParallel() {
      return parallel;
    }

    /**
     * Sets the value of the parallel property.
     *
     * @param value
     *        allowed object is
     *        {@link Boolean }
     *
     */
    public void setParallel(Boolean value) {
      this.parallel = value;
    }

    /**
     * Gets the value of the batchSize property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getBatchSize() {
      return batchSize;
    }

    /**
     * Sets the value of the batchSize property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setBatchSize(String value) {
      this.batchSize = value;
    }

    /**
     * Gets the value of the batchTimeInterval property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getBatchTimeInterval() {
      return batchTimeInterval;
    }

    /**
     * Sets the value of the batchTimeInterval property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setBatchTimeInterval(String value) {
      this.batchTimeInterval = value;
    }

    /**
     * Gets the value of the enableBatchConflation property.
     *
     * @return
     *         possible object is
     *         {@link Boolean }
     *
     */
    public Boolean isEnableBatchConflation() {
      return enableBatchConflation;
    }

    /**
     * Sets the value of the enableBatchConflation property.
     *
     * @param value
     *        allowed object is
     *        {@link Boolean }
     *
     */
    public void setEnableBatchConflation(Boolean value) {
      this.enableBatchConflation = value;
    }

    /**
     * Gets the value of the maximumQueueMemory property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getMaximumQueueMemory() {
      return maximumQueueMemory;
    }

    /**
     * Sets the value of the maximumQueueMemory property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setMaximumQueueMemory(String value) {
      this.maximumQueueMemory = value;
    }

    /**
     * Gets the value of the persistent property.
     *
     * @return
     *         possible object is
     *         {@link Boolean }
     *
     */
    public Boolean isPersistent() {
      return persistent;
    }

    /**
     * Sets the value of the persistent property.
     *
     * @param value
     *        allowed object is
     *        {@link Boolean }
     *
     */
    public void setPersistent(Boolean value) {
      this.persistent = value;
    }

    /**
     * Gets the value of the diskStoreName property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getDiskStoreName() {
      return diskStoreName;
    }

    /**
     * Sets the value of the diskStoreName property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setDiskStoreName(String value) {
      this.diskStoreName = value;
    }

    /**
     * Gets the value of the diskSynchronous property.
     *
     * @return
     *         possible object is
     *         {@link Boolean }
     *
     */
    public Boolean isDiskSynchronous() {
      return diskSynchronous;
    }

    /**
     * Sets the value of the diskSynchronous property.
     *
     * @param value
     *        allowed object is
     *        {@link Boolean }
     *
     */
    public void setDiskSynchronous(Boolean value) {
      this.diskSynchronous = value;
    }

    /**
     * Gets the value of the dispatcherThreads property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getDispatcherThreads() {
      return dispatcherThreads;
    }

    /**
     * Sets the value of the dispatcherThreads property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setDispatcherThreads(String value) {
      this.dispatcherThreads = value;
    }

    /**
     * Gets the value of the orderPolicy property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getOrderPolicy() {
      return orderPolicy;
    }

    /**
     * Sets the value of the orderPolicy property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setOrderPolicy(String value) {
      this.orderPolicy = value;
    }

    /**
     * Gets the value of the forwardExpirationDestroy property.
     *
     * @return
     *         possible object is
     *         {@link Boolean }
     *
     */
    public boolean isForwardExpirationDestroy() {
      if (forwardExpirationDestroy == null) {
        return false;
      } else {
        return forwardExpirationDestroy;
      }
    }

    /**
     * Sets the value of the forwardExpirationDestroy property.
     *
     * @param value
     *        allowed object is
     *        {@link Boolean }
     *
     */
    public void setForwardExpirationDestroy(Boolean value) {
      this.forwardExpirationDestroy = value;
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
   *     &lt;extension base="{http://geode.apache.org/schema/cache}server-type">
   *       &lt;attribute name="tcp-no-delay" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *     &lt;/extension>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "")
  public static class CacheServer extends ServerType {

    @XmlAttribute(name = "tcp-no-delay")
    protected Boolean tcpNoDelay;

    /**
     * Gets the value of the tcpNoDelay property.
     *
     * @return
     *         possible object is
     *         {@link Boolean }
     *
     */
    public Boolean isTcpNoDelay() {
      return tcpNoDelay;
    }

    /**
     * Sets the value of the tcpNoDelay property.
     *
     * @param value
     *        allowed object is
     *        {@link Boolean }
     *
     */
    public void setTcpNoDelay(Boolean value) {
      this.tcpNoDelay = value;
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
   *         &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
   *         &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
   *       &lt;/sequence>
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"className", "parameter"})
  public static class GatewayConflictResolver {

    @XmlElement(name = "class-name", namespace = "http://geode.apache.org/schema/cache",
        required = true)
    protected String className;
    @XmlElement(namespace = "http://geode.apache.org/schema/cache")
    protected List<ParameterType> parameter;

    /**
     * Gets the value of the className property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getClassName() {
      return className;
    }

    /**
     * Sets the value of the className property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setClassName(String value) {
      this.className = value;
    }

    /**
     * Gets the value of the parameter property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the parameter property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getParameter().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ParameterType }
     *
     *
     */
    public List<ParameterType> getParameter() {
      if (parameter == null) {
        parameter = new ArrayList<ParameterType>();
      }
      return this.parameter;
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
   *         &lt;element name="gateway" maxOccurs="unbounded" minOccurs="0">
   *           &lt;complexType>
   *             &lt;complexContent>
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                 &lt;sequence>
   *                   &lt;choice>
   *                     &lt;element name="gateway-endpoint" maxOccurs="unbounded">
   *                       &lt;complexType>
   *                         &lt;complexContent>
   *                           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                             &lt;attribute name="host" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                             &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                             &lt;attribute name="port" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                           &lt;/restriction>
   *                         &lt;/complexContent>
   *                       &lt;/complexType>
   *                     &lt;/element>
   *                     &lt;element name="gateway-listener" maxOccurs="unbounded">
   *                       &lt;complexType>
   *                         &lt;complexContent>
   *                           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                             &lt;sequence>
   *                               &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
   *                               &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
   *                             &lt;/sequence>
   *                           &lt;/restriction>
   *                         &lt;/complexContent>
   *                       &lt;/complexType>
   *                     &lt;/element>
   *                   &lt;/choice>
   *                   &lt;element name="gateway-queue" minOccurs="0">
   *                     &lt;complexType>
   *                       &lt;complexContent>
   *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *                           &lt;attribute name="alert-threshold" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                           &lt;attribute name="batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *                           &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                           &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                           &lt;attribute name="enable-persistence" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *                           &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                           &lt;attribute name="roll-oplogs" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *                           &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                           &lt;attribute name="overflow-directory" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                         &lt;/restriction>
   *                       &lt;/complexContent>
   *                     &lt;/complexType>
   *                   &lt;/element>
   *                 &lt;/sequence>
   *                 &lt;attribute name="early-ack" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *                 &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                 &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                 &lt;attribute name="socket-read-timeout" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                 &lt;attribute name="concurrency-level" type="{http://www.w3.org/2001/XMLSchema}string" />
   *                 &lt;attribute name="order-policy" type="{http://www.w3.org/2001/XMLSchema}string" />
   *               &lt;/restriction>
   *             &lt;/complexContent>
   *           &lt;/complexType>
   *         &lt;/element>
   *       &lt;/sequence>
   *       &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="bind-address" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="maximum-time-between-pings" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="port" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="startup-policy">
   *         &lt;simpleType>
   *           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
   *             &lt;enumeration value="primary"/>
   *             &lt;enumeration value="secondary"/>
   *             &lt;enumeration value="none"/>
   *           &lt;/restriction>
   *         &lt;/simpleType>
   *       &lt;/attribute>
   *       &lt;attribute name="manual-start" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *       &lt;attribute name="max-connections" type="{http://www.w3.org/2001/XMLSchema}integer" />
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"gateway"})
  public static class GatewayHub {

    @XmlElement(namespace = "http://geode.apache.org/schema/cache")
    protected List<CacheConfig.GatewayHub.Gateway> gateway;
    @XmlAttribute(name = "id", required = true)
    protected String id;
    @XmlAttribute(name = "bind-address")
    protected String bindAddress;
    @XmlAttribute(name = "maximum-time-between-pings")
    protected String maximumTimeBetweenPings;
    @XmlAttribute(name = "port")
    protected String port;
    @XmlAttribute(name = "socket-buffer-size")
    protected String socketBufferSize;
    @XmlAttribute(name = "startup-policy")
    protected String startupPolicy;
    @XmlAttribute(name = "manual-start")
    protected Boolean manualStart;
    @XmlAttribute(name = "max-connections")
    protected BigInteger maxConnections;

    /**
     * Gets the value of the gateway property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the gateway property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getGateway().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link CacheConfig.GatewayHub.Gateway }
     *
     *
     */
    public List<CacheConfig.GatewayHub.Gateway> getGateway() {
      if (gateway == null) {
        gateway = new ArrayList<CacheConfig.GatewayHub.Gateway>();
      }
      return this.gateway;
    }

    /**
     * Gets the value of the id property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getId() {
      return id;
    }

    /**
     * Sets the value of the id property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setId(String value) {
      this.id = value;
    }

    /**
     * Gets the value of the bindAddress property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getBindAddress() {
      return bindAddress;
    }

    /**
     * Sets the value of the bindAddress property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setBindAddress(String value) {
      this.bindAddress = value;
    }

    /**
     * Gets the value of the maximumTimeBetweenPings property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getMaximumTimeBetweenPings() {
      return maximumTimeBetweenPings;
    }

    /**
     * Sets the value of the maximumTimeBetweenPings property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setMaximumTimeBetweenPings(String value) {
      this.maximumTimeBetweenPings = value;
    }

    /**
     * Gets the value of the port property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getPort() {
      return port;
    }

    /**
     * Sets the value of the port property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setPort(String value) {
      this.port = value;
    }

    /**
     * Gets the value of the socketBufferSize property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getSocketBufferSize() {
      return socketBufferSize;
    }

    /**
     * Sets the value of the socketBufferSize property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setSocketBufferSize(String value) {
      this.socketBufferSize = value;
    }

    /**
     * Gets the value of the startupPolicy property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getStartupPolicy() {
      return startupPolicy;
    }

    /**
     * Sets the value of the startupPolicy property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setStartupPolicy(String value) {
      this.startupPolicy = value;
    }

    /**
     * Gets the value of the manualStart property.
     *
     * @return
     *         possible object is
     *         {@link Boolean }
     *
     */
    public Boolean isManualStart() {
      return manualStart;
    }

    /**
     * Sets the value of the manualStart property.
     *
     * @param value
     *        allowed object is
     *        {@link Boolean }
     *
     */
    public void setManualStart(Boolean value) {
      this.manualStart = value;
    }

    /**
     * Gets the value of the maxConnections property.
     *
     * @return
     *         possible object is
     *         {@link BigInteger }
     *
     */
    public BigInteger getMaxConnections() {
      return maxConnections;
    }

    /**
     * Sets the value of the maxConnections property.
     *
     * @param value
     *        allowed object is
     *        {@link BigInteger }
     *
     */
    public void setMaxConnections(BigInteger value) {
      this.maxConnections = value;
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
     *         &lt;choice>
     *           &lt;element name="gateway-endpoint" maxOccurs="unbounded">
     *             &lt;complexType>
     *               &lt;complexContent>
     *                 &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
     *                   &lt;attribute name="host" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
     *                   &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
     *                   &lt;attribute name="port" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
     *                 &lt;/restriction>
     *               &lt;/complexContent>
     *             &lt;/complexType>
     *           &lt;/element>
     *           &lt;element name="gateway-listener" maxOccurs="unbounded">
     *             &lt;complexType>
     *               &lt;complexContent>
     *                 &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
     *                   &lt;sequence>
     *                     &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
     *                     &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
     *                   &lt;/sequence>
     *                 &lt;/restriction>
     *               &lt;/complexContent>
     *             &lt;/complexType>
     *           &lt;/element>
     *         &lt;/choice>
     *         &lt;element name="gateway-queue" minOccurs="0">
     *           &lt;complexType>
     *             &lt;complexContent>
     *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
     *                 &lt;attribute name="alert-threshold" type="{http://www.w3.org/2001/XMLSchema}string" />
     *                 &lt;attribute name="batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" />
     *                 &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" />
     *                 &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" />
     *                 &lt;attribute name="enable-persistence" type="{http://www.w3.org/2001/XMLSchema}boolean" />
     *                 &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" />
     *                 &lt;attribute name="roll-oplogs" type="{http://www.w3.org/2001/XMLSchema}boolean" />
     *                 &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" />
     *                 &lt;attribute name="overflow-directory" type="{http://www.w3.org/2001/XMLSchema}string" />
     *               &lt;/restriction>
     *             &lt;/complexContent>
     *           &lt;/complexType>
     *         &lt;/element>
     *       &lt;/sequence>
     *       &lt;attribute name="early-ack" type="{http://www.w3.org/2001/XMLSchema}boolean" />
     *       &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
     *       &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" />
     *       &lt;attribute name="socket-read-timeout" type="{http://www.w3.org/2001/XMLSchema}string" />
     *       &lt;attribute name="concurrency-level" type="{http://www.w3.org/2001/XMLSchema}string" />
     *       &lt;attribute name="order-policy" type="{http://www.w3.org/2001/XMLSchema}string" />
     *     &lt;/restriction>
     *   &lt;/complexContent>
     * &lt;/complexType>
     * </pre>
     *
     *
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "", propOrder = {"gatewayEndpoint", "gatewayListener", "gatewayQueue"})
    public static class Gateway {

      @XmlElement(name = "gateway-endpoint", namespace = "http://geode.apache.org/schema/cache")
      protected List<CacheConfig.GatewayHub.Gateway.GatewayEndpoint> gatewayEndpoint;
      @XmlElement(name = "gateway-listener", namespace = "http://geode.apache.org/schema/cache")
      protected List<CacheConfig.GatewayHub.Gateway.GatewayListener> gatewayListener;
      @XmlElement(name = "gateway-queue", namespace = "http://geode.apache.org/schema/cache")
      protected CacheConfig.GatewayHub.Gateway.GatewayQueue gatewayQueue;
      @XmlAttribute(name = "early-ack")
      protected Boolean earlyAck;
      @XmlAttribute(name = "id", required = true)
      protected String id;
      @XmlAttribute(name = "socket-buffer-size")
      protected String socketBufferSize;
      @XmlAttribute(name = "socket-read-timeout")
      protected String socketReadTimeout;
      @XmlAttribute(name = "concurrency-level")
      protected String concurrencyLevel;
      @XmlAttribute(name = "order-policy")
      protected String orderPolicy;

      /**
       * Gets the value of the gatewayEndpoint property.
       *
       * <p>
       * This accessor method returns a reference to the live list,
       * not a snapshot. Therefore any modification you make to the
       * returned list will be present inside the JAXB object.
       * This is why there is not a <CODE>set</CODE> method for the gatewayEndpoint property.
       *
       * <p>
       * For example, to add a new item, do as follows:
       *
       * <pre>
       * getGatewayEndpoint().add(newItem);
       * </pre>
       *
       *
       * <p>
       * Objects of the following type(s) are allowed in the list
       * {@link CacheConfig.GatewayHub.Gateway.GatewayEndpoint }
       *
       *
       */
      public List<CacheConfig.GatewayHub.Gateway.GatewayEndpoint> getGatewayEndpoint() {
        if (gatewayEndpoint == null) {
          gatewayEndpoint = new ArrayList<CacheConfig.GatewayHub.Gateway.GatewayEndpoint>();
        }
        return this.gatewayEndpoint;
      }

      /**
       * Gets the value of the gatewayListener property.
       *
       * <p>
       * This accessor method returns a reference to the live list,
       * not a snapshot. Therefore any modification you make to the
       * returned list will be present inside the JAXB object.
       * This is why there is not a <CODE>set</CODE> method for the gatewayListener property.
       *
       * <p>
       * For example, to add a new item, do as follows:
       *
       * <pre>
       * getGatewayListener().add(newItem);
       * </pre>
       *
       *
       * <p>
       * Objects of the following type(s) are allowed in the list
       * {@link CacheConfig.GatewayHub.Gateway.GatewayListener }
       *
       *
       */
      public List<CacheConfig.GatewayHub.Gateway.GatewayListener> getGatewayListener() {
        if (gatewayListener == null) {
          gatewayListener = new ArrayList<CacheConfig.GatewayHub.Gateway.GatewayListener>();
        }
        return this.gatewayListener;
      }

      /**
       * Gets the value of the gatewayQueue property.
       *
       * @return
       *         possible object is
       *         {@link CacheConfig.GatewayHub.Gateway.GatewayQueue }
       *
       */
      public CacheConfig.GatewayHub.Gateway.GatewayQueue getGatewayQueue() {
        return gatewayQueue;
      }

      /**
       * Sets the value of the gatewayQueue property.
       *
       * @param value
       *        allowed object is
       *        {@link CacheConfig.GatewayHub.Gateway.GatewayQueue }
       *
       */
      public void setGatewayQueue(CacheConfig.GatewayHub.Gateway.GatewayQueue value) {
        this.gatewayQueue = value;
      }

      /**
       * Gets the value of the earlyAck property.
       *
       * @return
       *         possible object is
       *         {@link Boolean }
       *
       */
      public Boolean isEarlyAck() {
        return earlyAck;
      }

      /**
       * Sets the value of the earlyAck property.
       *
       * @param value
       *        allowed object is
       *        {@link Boolean }
       *
       */
      public void setEarlyAck(Boolean value) {
        this.earlyAck = value;
      }

      /**
       * Gets the value of the id property.
       *
       * @return
       *         possible object is
       *         {@link String }
       *
       */
      public String getId() {
        return id;
      }

      /**
       * Sets the value of the id property.
       *
       * @param value
       *        allowed object is
       *        {@link String }
       *
       */
      public void setId(String value) {
        this.id = value;
      }

      /**
       * Gets the value of the socketBufferSize property.
       *
       * @return
       *         possible object is
       *         {@link String }
       *
       */
      public String getSocketBufferSize() {
        return socketBufferSize;
      }

      /**
       * Sets the value of the socketBufferSize property.
       *
       * @param value
       *        allowed object is
       *        {@link String }
       *
       */
      public void setSocketBufferSize(String value) {
        this.socketBufferSize = value;
      }

      /**
       * Gets the value of the socketReadTimeout property.
       *
       * @return
       *         possible object is
       *         {@link String }
       *
       */
      public String getSocketReadTimeout() {
        return socketReadTimeout;
      }

      /**
       * Sets the value of the socketReadTimeout property.
       *
       * @param value
       *        allowed object is
       *        {@link String }
       *
       */
      public void setSocketReadTimeout(String value) {
        this.socketReadTimeout = value;
      }

      /**
       * Gets the value of the concurrencyLevel property.
       *
       * @return
       *         possible object is
       *         {@link String }
       *
       */
      public String getConcurrencyLevel() {
        return concurrencyLevel;
      }

      /**
       * Sets the value of the concurrencyLevel property.
       *
       * @param value
       *        allowed object is
       *        {@link String }
       *
       */
      public void setConcurrencyLevel(String value) {
        this.concurrencyLevel = value;
      }

      /**
       * Gets the value of the orderPolicy property.
       *
       * @return
       *         possible object is
       *         {@link String }
       *
       */
      public String getOrderPolicy() {
        return orderPolicy;
      }

      /**
       * Sets the value of the orderPolicy property.
       *
       * @param value
       *        allowed object is
       *        {@link String }
       *
       */
      public void setOrderPolicy(String value) {
        this.orderPolicy = value;
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
       *       &lt;attribute name="host" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
       *       &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
       *       &lt;attribute name="port" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
       *     &lt;/restriction>
       *   &lt;/complexContent>
       * &lt;/complexType>
       * </pre>
       *
       *
       */
      @XmlAccessorType(XmlAccessType.FIELD)
      @XmlType(name = "")
      public static class GatewayEndpoint {

        @XmlAttribute(name = "host", required = true)
        protected String host;
        @XmlAttribute(name = "id", required = true)
        protected String id;
        @XmlAttribute(name = "port", required = true)
        protected String port;

        /**
         * Gets the value of the host property.
         *
         * @return
         *         possible object is
         *         {@link String }
         *
         */
        public String getHost() {
          return host;
        }

        /**
         * Sets the value of the host property.
         *
         * @param value
         *        allowed object is
         *        {@link String }
         *
         */
        public void setHost(String value) {
          this.host = value;
        }

        /**
         * Gets the value of the id property.
         *
         * @return
         *         possible object is
         *         {@link String }
         *
         */
        public String getId() {
          return id;
        }

        /**
         * Sets the value of the id property.
         *
         * @param value
         *        allowed object is
         *        {@link String }
         *
         */
        public void setId(String value) {
          this.id = value;
        }

        /**
         * Gets the value of the port property.
         *
         * @return
         *         possible object is
         *         {@link String }
         *
         */
        public String getPort() {
          return port;
        }

        /**
         * Sets the value of the port property.
         *
         * @param value
         *        allowed object is
         *        {@link String }
         *
         */
        public void setPort(String value) {
          this.port = value;
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
       *         &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/>
       *         &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/>
       *       &lt;/sequence>
       *     &lt;/restriction>
       *   &lt;/complexContent>
       * &lt;/complexType>
       * </pre>
       *
       *
       */
      @XmlAccessorType(XmlAccessType.FIELD)
      @XmlType(name = "", propOrder = {"className", "parameter"})
      public static class GatewayListener {

        @XmlElement(name = "class-name", namespace = "http://geode.apache.org/schema/cache",
            required = true)
        protected String className;
        @XmlElement(namespace = "http://geode.apache.org/schema/cache")
        protected List<ParameterType> parameter;

        /**
         * Gets the value of the className property.
         *
         * @return
         *         possible object is
         *         {@link String }
         *
         */
        public String getClassName() {
          return className;
        }

        /**
         * Sets the value of the className property.
         *
         * @param value
         *        allowed object is
         *        {@link String }
         *
         */
        public void setClassName(String value) {
          this.className = value;
        }

        /**
         * Gets the value of the parameter property.
         *
         * <p>
         * This accessor method returns a reference to the live list,
         * not a snapshot. Therefore any modification you make to the
         * returned list will be present inside the JAXB object.
         * This is why there is not a <CODE>set</CODE> method for the parameter property.
         *
         * <p>
         * For example, to add a new item, do as follows:
         *
         * <pre>
         * getParameter().add(newItem);
         * </pre>
         *
         *
         * <p>
         * Objects of the following type(s) are allowed in the list
         * {@link ParameterType }
         *
         *
         */
        public List<ParameterType> getParameter() {
          if (parameter == null) {
            parameter = new ArrayList<ParameterType>();
          }
          return this.parameter;
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
       *       &lt;attribute name="alert-threshold" type="{http://www.w3.org/2001/XMLSchema}string" />
       *       &lt;attribute name="batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" />
       *       &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" />
       *       &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" />
       *       &lt;attribute name="enable-persistence" type="{http://www.w3.org/2001/XMLSchema}boolean" />
       *       &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" />
       *       &lt;attribute name="roll-oplogs" type="{http://www.w3.org/2001/XMLSchema}boolean" />
       *       &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" />
       *       &lt;attribute name="overflow-directory" type="{http://www.w3.org/2001/XMLSchema}string" />
       *     &lt;/restriction>
       *   &lt;/complexContent>
       * &lt;/complexType>
       * </pre>
       *
       *
       */
      @XmlAccessorType(XmlAccessType.FIELD)
      @XmlType(name = "")
      public static class GatewayQueue {

        @XmlAttribute(name = "alert-threshold")
        protected String alertThreshold;
        @XmlAttribute(name = "batch-conflation")
        protected Boolean batchConflation;
        @XmlAttribute(name = "batch-size")
        protected String batchSize;
        @XmlAttribute(name = "batch-time-interval")
        protected String batchTimeInterval;
        @XmlAttribute(name = "enable-persistence")
        protected Boolean enablePersistence;
        @XmlAttribute(name = "disk-store-name")
        protected String diskStoreName;
        @XmlAttribute(name = "roll-oplogs")
        protected Boolean rollOplogs;
        @XmlAttribute(name = "maximum-queue-memory")
        protected String maximumQueueMemory;
        @XmlAttribute(name = "overflow-directory")
        protected String overflowDirectory;

        /**
         * Gets the value of the alertThreshold property.
         *
         * @return
         *         possible object is
         *         {@link String }
         *
         */
        public String getAlertThreshold() {
          return alertThreshold;
        }

        /**
         * Sets the value of the alertThreshold property.
         *
         * @param value
         *        allowed object is
         *        {@link String }
         *
         */
        public void setAlertThreshold(String value) {
          this.alertThreshold = value;
        }

        /**
         * Gets the value of the batchConflation property.
         *
         * @return
         *         possible object is
         *         {@link Boolean }
         *
         */
        public Boolean isBatchConflation() {
          return batchConflation;
        }

        /**
         * Sets the value of the batchConflation property.
         *
         * @param value
         *        allowed object is
         *        {@link Boolean }
         *
         */
        public void setBatchConflation(Boolean value) {
          this.batchConflation = value;
        }

        /**
         * Gets the value of the batchSize property.
         *
         * @return
         *         possible object is
         *         {@link String }
         *
         */
        public String getBatchSize() {
          return batchSize;
        }

        /**
         * Sets the value of the batchSize property.
         *
         * @param value
         *        allowed object is
         *        {@link String }
         *
         */
        public void setBatchSize(String value) {
          this.batchSize = value;
        }

        /**
         * Gets the value of the batchTimeInterval property.
         *
         * @return
         *         possible object is
         *         {@link String }
         *
         */
        public String getBatchTimeInterval() {
          return batchTimeInterval;
        }

        /**
         * Sets the value of the batchTimeInterval property.
         *
         * @param value
         *        allowed object is
         *        {@link String }
         *
         */
        public void setBatchTimeInterval(String value) {
          this.batchTimeInterval = value;
        }

        /**
         * Gets the value of the enablePersistence property.
         *
         * @return
         *         possible object is
         *         {@link Boolean }
         *
         */
        public Boolean isEnablePersistence() {
          return enablePersistence;
        }

        /**
         * Sets the value of the enablePersistence property.
         *
         * @param value
         *        allowed object is
         *        {@link Boolean }
         *
         */
        public void setEnablePersistence(Boolean value) {
          this.enablePersistence = value;
        }

        /**
         * Gets the value of the diskStoreName property.
         *
         * @return
         *         possible object is
         *         {@link String }
         *
         */
        public String getDiskStoreName() {
          return diskStoreName;
        }

        /**
         * Sets the value of the diskStoreName property.
         *
         * @param value
         *        allowed object is
         *        {@link String }
         *
         */
        public void setDiskStoreName(String value) {
          this.diskStoreName = value;
        }

        /**
         * Gets the value of the rollOplogs property.
         *
         * @return
         *         possible object is
         *         {@link Boolean }
         *
         */
        public Boolean isRollOplogs() {
          return rollOplogs;
        }

        /**
         * Sets the value of the rollOplogs property.
         *
         * @param value
         *        allowed object is
         *        {@link Boolean }
         *
         */
        public void setRollOplogs(Boolean value) {
          this.rollOplogs = value;
        }

        /**
         * Gets the value of the maximumQueueMemory property.
         *
         * @return
         *         possible object is
         *         {@link String }
         *
         */
        public String getMaximumQueueMemory() {
          return maximumQueueMemory;
        }

        /**
         * Sets the value of the maximumQueueMemory property.
         *
         * @param value
         *        allowed object is
         *        {@link String }
         *
         */
        public void setMaximumQueueMemory(String value) {
          this.maximumQueueMemory = value;
        }

        /**
         * Gets the value of the overflowDirectory property.
         *
         * @return
         *         possible object is
         *         {@link String }
         *
         */
        public String getOverflowDirectory() {
          return overflowDirectory;
        }

        /**
         * Sets the value of the overflowDirectory property.
         *
         * @param value
         *        allowed object is
         *        {@link String }
         *
         */
        public void setOverflowDirectory(String value) {
          this.overflowDirectory = value;
        }

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
   *         &lt;element name="gateway-transport-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" maxOccurs="unbounded" minOccurs="0"/>
   *       &lt;/sequence>
   *       &lt;attribute name="start-port" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="end-port" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="bind-address" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="maximum-time-between-pings" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="hostname-for-senders" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="manual-start" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"gatewayTransportFilter"})
  public static class GatewayReceiver {

    @XmlElement(name = "gateway-transport-filter",
        namespace = "http://geode.apache.org/schema/cache")
    protected List<ClassWithParametersType> gatewayTransportFilter;
    @XmlAttribute(name = "start-port")
    protected String startPort;
    @XmlAttribute(name = "end-port")
    protected String endPort;
    @XmlAttribute(name = "bind-address")
    protected String bindAddress;
    @XmlAttribute(name = "maximum-time-between-pings")
    protected String maximumTimeBetweenPings;
    @XmlAttribute(name = "socket-buffer-size")
    protected String socketBufferSize;
    @XmlAttribute(name = "hostname-for-senders")
    protected String hostnameForSenders;
    @XmlAttribute(name = "manual-start")
    protected Boolean manualStart;

    /**
     * Gets the value of the gatewayTransportFilter property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the gatewayTransportFilter property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getGatewayTransportFilter().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ClassWithParametersType }
     *
     *
     */
    public List<ClassWithParametersType> getGatewayTransportFilter() {
      if (gatewayTransportFilter == null) {
        gatewayTransportFilter = new ArrayList<ClassWithParametersType>();
      }
      return this.gatewayTransportFilter;
    }

    /**
     * Gets the value of the startPort property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getStartPort() {
      return startPort;
    }

    /**
     * Sets the value of the startPort property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setStartPort(String value) {
      this.startPort = value;
    }

    /**
     * Gets the value of the endPort property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getEndPort() {
      return endPort;
    }

    /**
     * Sets the value of the endPort property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setEndPort(String value) {
      this.endPort = value;
    }

    /**
     * Gets the value of the bindAddress property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getBindAddress() {
      return bindAddress;
    }

    /**
     * Sets the value of the bindAddress property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setBindAddress(String value) {
      this.bindAddress = value;
    }

    /**
     * Gets the value of the maximumTimeBetweenPings property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getMaximumTimeBetweenPings() {
      return maximumTimeBetweenPings;
    }

    /**
     * Sets the value of the maximumTimeBetweenPings property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setMaximumTimeBetweenPings(String value) {
      this.maximumTimeBetweenPings = value;
    }

    /**
     * Gets the value of the socketBufferSize property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getSocketBufferSize() {
      return socketBufferSize;
    }

    /**
     * Sets the value of the socketBufferSize property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setSocketBufferSize(String value) {
      this.socketBufferSize = value;
    }

    /**
     * Gets the value of the hostnameForSenders property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getHostnameForSenders() {
      return hostnameForSenders;
    }

    /**
     * Sets the value of the hostnameForSenders property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setHostnameForSenders(String value) {
      this.hostnameForSenders = value;
    }

    /**
     * Gets the value of the manualStart property.
     *
     * @return
     *         possible object is
     *         {@link Boolean }
     *
     */
    public Boolean isManualStart() {
      return manualStart;
    }

    /**
     * Sets the value of the manualStart property.
     *
     * @param value
     *        allowed object is
     *        {@link Boolean }
     *
     */
    public void setManualStart(Boolean value) {
      this.manualStart = value;
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
   *         &lt;element name="gateway-event-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" maxOccurs="unbounded" minOccurs="0"/>
   *         &lt;element name="gateway-event-substitution-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" minOccurs="0"/>
   *         &lt;element name="gateway-transport-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" maxOccurs="unbounded" minOccurs="0"/>
   *       &lt;/sequence>
   *       &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="remote-distributed-system-id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="parallel" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *       &lt;attribute name="manual-start" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *       &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="socket-read-timeout" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="enable-batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *       &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="enable-persistence" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *       &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="disk-synchronous" type="{http://www.w3.org/2001/XMLSchema}boolean" />
   *       &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="alert-threshold" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="dispatcher-threads" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="order-policy" type="{http://www.w3.org/2001/XMLSchema}string" />
   *     &lt;/restriction>
   *   &lt;/complexContent>
   * &lt;/complexType>
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"gatewayEventFilter", "gatewayEventSubstitutionFilter",
      "gatewayTransportFilter"})
  public static class GatewaySender {

    @XmlElement(name = "gateway-event-filter", namespace = "http://geode.apache.org/schema/cache")
    protected List<ClassWithParametersType> gatewayEventFilter;
    @XmlElement(name = "gateway-event-substitution-filter",
        namespace = "http://geode.apache.org/schema/cache")
    protected ClassWithParametersType gatewayEventSubstitutionFilter;
    @XmlElement(name = "gateway-transport-filter",
        namespace = "http://geode.apache.org/schema/cache")
    protected List<ClassWithParametersType> gatewayTransportFilter;
    @XmlAttribute(name = "id", required = true)
    protected String id;
    @XmlAttribute(name = "remote-distributed-system-id", required = true)
    protected String remoteDistributedSystemId;
    @XmlAttribute(name = "parallel")
    protected Boolean parallel;
    @XmlAttribute(name = "manual-start")
    protected Boolean manualStart;
    @XmlAttribute(name = "socket-buffer-size")
    protected String socketBufferSize;
    @XmlAttribute(name = "socket-read-timeout")
    protected String socketReadTimeout;
    @XmlAttribute(name = "enable-batch-conflation")
    protected Boolean enableBatchConflation;
    @XmlAttribute(name = "batch-size")
    protected String batchSize;
    @XmlAttribute(name = "batch-time-interval")
    protected String batchTimeInterval;
    @XmlAttribute(name = "enable-persistence")
    protected Boolean enablePersistence;
    @XmlAttribute(name = "disk-store-name")
    protected String diskStoreName;
    @XmlAttribute(name = "disk-synchronous")
    protected Boolean diskSynchronous;
    @XmlAttribute(name = "maximum-queue-memory")
    protected String maximumQueueMemory;
    @XmlAttribute(name = "alert-threshold")
    protected String alertThreshold;
    @XmlAttribute(name = "dispatcher-threads")
    protected String dispatcherThreads;
    @XmlAttribute(name = "order-policy")
    protected String orderPolicy;

    /**
     * Gets the value of the gatewayEventFilter property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the gatewayEventFilter property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getGatewayEventFilter().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ClassWithParametersType }
     *
     *
     */
    public List<ClassWithParametersType> getGatewayEventFilter() {
      if (gatewayEventFilter == null) {
        gatewayEventFilter = new ArrayList<ClassWithParametersType>();
      }
      return this.gatewayEventFilter;
    }

    /**
     * Gets the value of the gatewayEventSubstitutionFilter property.
     *
     * @return
     *         possible object is
     *         {@link ClassWithParametersType }
     *
     */
    public ClassWithParametersType getGatewayEventSubstitutionFilter() {
      return gatewayEventSubstitutionFilter;
    }

    /**
     * Sets the value of the gatewayEventSubstitutionFilter property.
     *
     * @param value
     *        allowed object is
     *        {@link ClassWithParametersType }
     *
     */
    public void setGatewayEventSubstitutionFilter(ClassWithParametersType value) {
      this.gatewayEventSubstitutionFilter = value;
    }

    /**
     * Gets the value of the gatewayTransportFilter property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the gatewayTransportFilter property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getGatewayTransportFilter().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ClassWithParametersType }
     *
     *
     */
    public List<ClassWithParametersType> getGatewayTransportFilter() {
      if (gatewayTransportFilter == null) {
        gatewayTransportFilter = new ArrayList<ClassWithParametersType>();
      }
      return this.gatewayTransportFilter;
    }

    /**
     * Gets the value of the id property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getId() {
      return id;
    }

    /**
     * Sets the value of the id property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setId(String value) {
      this.id = value;
    }

    /**
     * Gets the value of the remoteDistributedSystemId property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getRemoteDistributedSystemId() {
      return remoteDistributedSystemId;
    }

    /**
     * Sets the value of the remoteDistributedSystemId property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setRemoteDistributedSystemId(String value) {
      this.remoteDistributedSystemId = value;
    }

    /**
     * Gets the value of the parallel property.
     *
     * @return
     *         possible object is
     *         {@link Boolean }
     *
     */
    public Boolean isParallel() {
      return parallel;
    }

    /**
     * Sets the value of the parallel property.
     *
     * @param value
     *        allowed object is
     *        {@link Boolean }
     *
     */
    public void setParallel(Boolean value) {
      this.parallel = value;
    }

    /**
     * Gets the value of the manualStart property.
     *
     * @return
     *         possible object is
     *         {@link Boolean }
     *
     */
    public Boolean isManualStart() {
      return manualStart;
    }

    /**
     * Sets the value of the manualStart property.
     *
     * @param value
     *        allowed object is
     *        {@link Boolean }
     *
     */
    public void setManualStart(Boolean value) {
      this.manualStart = value;
    }

    /**
     * Gets the value of the socketBufferSize property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getSocketBufferSize() {
      return socketBufferSize;
    }

    /**
     * Sets the value of the socketBufferSize property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setSocketBufferSize(String value) {
      this.socketBufferSize = value;
    }

    /**
     * Gets the value of the socketReadTimeout property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getSocketReadTimeout() {
      return socketReadTimeout;
    }

    /**
     * Sets the value of the socketReadTimeout property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setSocketReadTimeout(String value) {
      this.socketReadTimeout = value;
    }

    /**
     * Gets the value of the enableBatchConflation property.
     *
     * @return
     *         possible object is
     *         {@link Boolean }
     *
     */
    public Boolean isEnableBatchConflation() {
      return enableBatchConflation;
    }

    /**
     * Sets the value of the enableBatchConflation property.
     *
     * @param value
     *        allowed object is
     *        {@link Boolean }
     *
     */
    public void setEnableBatchConflation(Boolean value) {
      this.enableBatchConflation = value;
    }

    /**
     * Gets the value of the batchSize property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getBatchSize() {
      return batchSize;
    }

    /**
     * Sets the value of the batchSize property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setBatchSize(String value) {
      this.batchSize = value;
    }

    /**
     * Gets the value of the batchTimeInterval property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getBatchTimeInterval() {
      return batchTimeInterval;
    }

    /**
     * Sets the value of the batchTimeInterval property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setBatchTimeInterval(String value) {
      this.batchTimeInterval = value;
    }

    /**
     * Gets the value of the enablePersistence property.
     *
     * @return
     *         possible object is
     *         {@link Boolean }
     *
     */
    public Boolean isEnablePersistence() {
      return enablePersistence;
    }

    /**
     * Sets the value of the enablePersistence property.
     *
     * @param value
     *        allowed object is
     *        {@link Boolean }
     *
     */
    public void setEnablePersistence(Boolean value) {
      this.enablePersistence = value;
    }

    /**
     * Gets the value of the diskStoreName property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getDiskStoreName() {
      return diskStoreName;
    }

    /**
     * Sets the value of the diskStoreName property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setDiskStoreName(String value) {
      this.diskStoreName = value;
    }

    /**
     * Gets the value of the diskSynchronous property.
     *
     * @return
     *         possible object is
     *         {@link Boolean }
     *
     */
    public Boolean isDiskSynchronous() {
      return diskSynchronous;
    }

    /**
     * Sets the value of the diskSynchronous property.
     *
     * @param value
     *        allowed object is
     *        {@link Boolean }
     *
     */
    public void setDiskSynchronous(Boolean value) {
      this.diskSynchronous = value;
    }

    /**
     * Gets the value of the maximumQueueMemory property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getMaximumQueueMemory() {
      return maximumQueueMemory;
    }

    /**
     * Sets the value of the maximumQueueMemory property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setMaximumQueueMemory(String value) {
      this.maximumQueueMemory = value;
    }

    /**
     * Gets the value of the alertThreshold property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getAlertThreshold() {
      return alertThreshold;
    }

    /**
     * Sets the value of the alertThreshold property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setAlertThreshold(String value) {
      this.alertThreshold = value;
    }

    /**
     * Gets the value of the dispatcherThreads property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getDispatcherThreads() {
      return dispatcherThreads;
    }

    /**
     * Sets the value of the dispatcherThreads property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setDispatcherThreads(String value) {
      this.dispatcherThreads = value;
    }

    /**
     * Gets the value of the orderPolicy property.
     *
     * @return
     *         possible object is
     *         {@link String }
     *
     */
    public String getOrderPolicy() {
      return orderPolicy;
    }

    /**
     * Sets the value of the orderPolicy property.
     *
     * @param value
     *        allowed object is
     *        {@link String }
     *
     */
    public void setOrderPolicy(String value) {
      this.orderPolicy = value;
    }

  }

}
