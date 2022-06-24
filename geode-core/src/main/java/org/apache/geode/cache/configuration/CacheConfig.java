
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

import static org.apache.geode.lang.Identifiable.find;

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
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.w3c.dom.Element;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.internal.config.VersionAdapter;
import org.apache.geode.lang.Identifiable;

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
 *         &lt;element name="cache-transaction-manager" type="{http://geode.apache.org/schema/cache}cache-transaction-manager-type" minOccurs="0"/&gt;
 *         &lt;element name="dynamic-region-factory" type="{http://geode.apache.org/schema/cache}dynamic-region-factory-type" minOccurs="0"/&gt;
 *         &lt;element name="gateway-hub" maxOccurs="unbounded" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="gateway" maxOccurs="unbounded" minOccurs="0"&gt;
 *                     &lt;complexType&gt;
 *                       &lt;complexContent&gt;
 *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                           &lt;sequence&gt;
 *                             &lt;choice&gt;
 *                               &lt;element name="gateway-endpoint" maxOccurs="unbounded"&gt;
 *                                 &lt;complexType&gt;
 *                                   &lt;complexContent&gt;
 *                                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                                       &lt;attribute name="host" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                                       &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                                       &lt;attribute name="port" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                                     &lt;/restriction&gt;
 *                                   &lt;/complexContent&gt;
 *                                 &lt;/complexType&gt;
 *                               &lt;/element&gt;
 *                               &lt;element name="gateway-listener" maxOccurs="unbounded"&gt;
 *                                 &lt;complexType&gt;
 *                                   &lt;complexContent&gt;
 *                                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                                       &lt;sequence&gt;
 *                                         &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
 *                                         &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *                                       &lt;/sequence&gt;
 *                                     &lt;/restriction&gt;
 *                                   &lt;/complexContent&gt;
 *                                 &lt;/complexType&gt;
 *                               &lt;/element&gt;
 *                             &lt;/choice&gt;
 *                             &lt;element name="gateway-queue" minOccurs="0"&gt;
 *                               &lt;complexType&gt;
 *                                 &lt;complexContent&gt;
 *                                   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                                     &lt;attribute name="alert-threshold" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                                     &lt;attribute name="batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                                     &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                                     &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                                     &lt;attribute name="enable-persistence" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                                     &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                                     &lt;attribute name="roll-oplogs" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                                     &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                                     &lt;attribute name="overflow-directory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                                   &lt;/restriction&gt;
 *                                 &lt;/complexContent&gt;
 *                               &lt;/complexType&gt;
 *                             &lt;/element&gt;
 *                           &lt;/sequence&gt;
 *                           &lt;attribute name="early-ack" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                           &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                           &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                           &lt;attribute name="socket-read-timeout" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                           &lt;attribute name="concurrency-level" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                           &lt;attribute name="order-policy" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                         &lt;/restriction&gt;
 *                       &lt;/complexContent&gt;
 *                     &lt;/complexType&gt;
 *                   &lt;/element&gt;
 *                 &lt;/sequence&gt;
 *                 &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="bind-address" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="maximum-time-between-pings" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="port" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="startup-policy"&gt;
 *                   &lt;simpleType&gt;
 *                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
 *                       &lt;enumeration value="primary"/&gt;
 *                       &lt;enumeration value="secondary"/&gt;
 *                       &lt;enumeration value="none"/&gt;
 *                     &lt;/restriction&gt;
 *                   &lt;/simpleType&gt;
 *                 &lt;/attribute&gt;
 *                 &lt;attribute name="manual-start" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                 &lt;attribute name="max-connections" type="{http://www.w3.org/2001/XMLSchema}integer" /&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="gateway-sender" maxOccurs="unbounded" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="gateway-event-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *                   &lt;element name="gateway-event-substitution-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" minOccurs="0"/&gt;
 *                   &lt;element name="gateway-transport-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *                 &lt;/sequence&gt;
 *                 &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="remote-distributed-system-id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="parallel" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                 &lt;attribute name="manual-start" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                 &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="socket-read-timeout" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="enable-batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                 &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="enable-persistence" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                 &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="disk-synchronous" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                 &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="alert-threshold" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="dispatcher-threads" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="order-policy" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="group-transaction-events" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="gateway-receiver" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="gateway-transport-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *                 &lt;/sequence&gt;
 *                 &lt;attribute name="start-port" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="end-port" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="bind-address" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="maximum-time-between-pings" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="hostname-for-senders" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="manual-start" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="gateway-conflict-resolver" minOccurs="0"&gt;
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
 *         &lt;element name="async-event-queue" maxOccurs="unbounded" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;sequence&gt;
 *                   &lt;element name="gateway-event-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *                   &lt;element name="gateway-event-substitution-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" minOccurs="0"/&gt;
 *                   &lt;element name="async-event-listener" type="{http://geode.apache.org/schema/cache}class-with-parameters-type"/&gt;
 *                 &lt;/sequence&gt;
 *                 &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="parallel" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                 &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="enable-batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                 &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="persistent" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                 &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="disk-synchronous" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *                 &lt;attribute name="dispatcher-threads" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="order-policy" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="forward-expiration-destroy" type="{http://www.w3.org/2001/XMLSchema}boolean" default="false" /&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="cache-server" maxOccurs="unbounded" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;extension base="{http://geode.apache.org/schema/cache}server-type"&gt;
 *                 &lt;attribute name="tcp-no-delay" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *               &lt;/extension&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="pool" type="{http://geode.apache.org/schema/cache}pool-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *         &lt;element name="disk-store" type="{http://geode.apache.org/schema/cache}disk-store-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *         &lt;element name="pdx" type="{http://geode.apache.org/schema/cache}pdx-type" minOccurs="0"/&gt;
 *         &lt;element name="region-attributes" type="{http://geode.apache.org/schema/cache}region-attributes-type" maxOccurs="unbounded" minOccurs="0"/&gt;
 *         &lt;choice maxOccurs="unbounded" minOccurs="0"&gt;
 *           &lt;element name="jndi-bindings" type="{http://geode.apache.org/schema/cache}jndi-bindings-type"/&gt;
 *           &lt;element name="region" type="{http://geode.apache.org/schema/cache}region-type"/&gt;
 *           &lt;element name="vm-root-region" type="{http://geode.apache.org/schema/cache}region-type"/&gt;
 *         &lt;/choice&gt;
 *         &lt;element name="function-service" type="{http://geode.apache.org/schema/cache}function-service-type" minOccurs="0"/&gt;
 *         &lt;element name="resource-manager" type="{http://geode.apache.org/schema/cache}resource-manager-type" minOccurs="0"/&gt;
 *         &lt;element name="serialization-registration" type="{http://geode.apache.org/schema/cache}serialization-registration-type" minOccurs="0"/&gt;
 *         &lt;element name="backup" type="{http://www.w3.org/2001/XMLSchema}string" maxOccurs="unbounded" minOccurs="0"/&gt;
 *         &lt;element name="initializer" type="{http://geode.apache.org/schema/cache}initializer-type" minOccurs="0"/&gt;
 *         &lt;any processContents='lax' namespace='##other' maxOccurs="unbounded" minOccurs="0"/&gt;
 *       &lt;/sequence&gt;
 *       &lt;attribute name="copy-on-read" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="is-server" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="lock-timeout" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="lock-lease" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="message-sync-interval" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="search-timeout" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="version" use="required" type="{http://geode.apache.org/schema/cache}versionType" fixed="1.0" /&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "",
    propOrder = {"cacheTransactionManager", "dynamicRegionFactory", "gatewayHubs", "gatewaySenders",
        "gatewayReceiver", "gatewayConflictResolver", "asyncEventQueues", "cacheServers", "pools",
        "diskStores", "pdx", "regionAttributes", "jndiBindings", "regions", "functionService",
        "resourceManager", "serializationRegistration", "backups", "initializer", "cacheElements"})
@XmlRootElement(name = "cache", namespace = "http://geode.apache.org/schema/cache")
@XSDRootElement(namespace = "http://geode.apache.org/schema/cache",
    schemaLocation = "http://geode.apache.org/schema/cache/cache-1.0.xsd")
@Experimental
public class CacheConfig {
  @XmlElement(name = "cache-transaction-manager",
      namespace = "http://geode.apache.org/schema/cache")
  protected CacheTransactionManagerType cacheTransactionManager;
  @XmlElement(name = "dynamic-region-factory", namespace = "http://geode.apache.org/schema/cache")
  protected DynamicRegionFactoryType dynamicRegionFactory;
  @XmlElement(name = "gateway-hub", namespace = "http://geode.apache.org/schema/cache")
  protected List<GatewayHub> gatewayHubs;
  @XmlElement(name = "gateway-sender", namespace = "http://geode.apache.org/schema/cache")
  protected List<GatewaySender> gatewaySenders;
  @XmlElement(name = "gateway-receiver", namespace = "http://geode.apache.org/schema/cache")
  protected GatewayReceiverConfig gatewayReceiver;
  @XmlElement(name = "gateway-conflict-resolver",
      namespace = "http://geode.apache.org/schema/cache")
  protected DeclarableType gatewayConflictResolver;
  @XmlElement(name = "async-event-queue", namespace = "http://geode.apache.org/schema/cache")
  protected List<AsyncEventQueue> asyncEventQueues;
  @XmlElement(name = "cache-server", namespace = "http://geode.apache.org/schema/cache")
  protected List<CacheServer> cacheServers;
  @XmlElement(name = "pool", namespace = "http://geode.apache.org/schema/cache")
  protected List<PoolType> pools;
  @XmlElement(name = "disk-store", namespace = "http://geode.apache.org/schema/cache")
  protected List<DiskStoreType> diskStores;
  @XmlElement(namespace = "http://geode.apache.org/schema/cache")
  protected PdxType pdx;
  @XmlElement(name = "region-attributes", namespace = "http://geode.apache.org/schema/cache")
  protected List<RegionAttributesType> regionAttributes;
  @XmlElement(name = "jndi-bindings", namespace = "http://geode.apache.org/schema/cache")
  protected JndiBindingsType jndiBindings;
  @XmlElement(name = "region", namespace = "http://geode.apache.org/schema/cache")
  protected List<RegionConfig> regions;
  @XmlElement(name = "function-service", namespace = "http://geode.apache.org/schema/cache")
  protected FunctionServiceType functionService;
  @XmlElement(name = "resource-manager", namespace = "http://geode.apache.org/schema/cache")
  protected ResourceManagerType resourceManager;
  @XmlElement(name = "serialization-registration",
      namespace = "http://geode.apache.org/schema/cache")
  protected SerializationRegistrationType serializationRegistration;
  @XmlElement(name = "backup", namespace = "http://geode.apache.org/schema/cache")
  protected List<String> backups;
  @XmlElement(namespace = "http://geode.apache.org/schema/cache")
  protected DeclarableType initializer;
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
  @XmlJavaTypeAdapter(VersionAdapter.class)
  protected String version;

  /**
   * @deprecated Please use {@link Region#SEPARATOR}
   */
  @Deprecated
  public static final String SEPARATOR = Region.SEPARATOR;

  public CacheConfig() {}

  public CacheConfig(String version) {

    this.version = version;
  }

  /**
   * Gets the value of the cacheTransactionManager property.
   *
   * possible object is
   * {@link CacheTransactionManagerType }
   *
   * @return the value of the cacheTransactionManager property
   */
  public CacheTransactionManagerType getCacheTransactionManager() {
    return cacheTransactionManager;
  }

  /**
   * Sets the value of the cacheTransactionManager property.
   *
   * allowed object is
   * {@link CacheTransactionManagerType }
   *
   * @param value the value of the cacheTransactionManager property
   */
  public void setCacheTransactionManager(CacheTransactionManagerType value) {
    cacheTransactionManager = value;
  }

  /**
   * Gets the value of the dynamicRegionFactory property.
   *
   * possible object is
   * {@link DynamicRegionFactoryType }
   *
   * @return the value of the dynamicRegionFactory property
   */
  public DynamicRegionFactoryType getDynamicRegionFactory() {
    return dynamicRegionFactory;
  }

  /**
   * Sets the value of the dynamicRegionFactory property.
   *
   * allowed object is
   * {@link DynamicRegionFactoryType }
   *
   * @param value the value of the dynamicRegionFactory property
   */
  public void setDynamicRegionFactory(DynamicRegionFactoryType value) {
    dynamicRegionFactory = value;
  }

  /**
   * Gets the value of the gatewayHubs property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the gatewayHubs property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getGatewayHubs().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link CacheConfig.GatewayHub }
   *
   * @return the value of the gatewayHubs property
   */
  public List<GatewayHub> getGatewayHubs() {
    if (gatewayHubs == null) {
      gatewayHubs = new ArrayList<>();
    }
    return gatewayHubs;
  }

  /**
   * Gets the value of the gatewaySenders property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the gatewaySenders property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getGatewaySenders().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link CacheConfig.GatewaySender }
   *
   * @return the value of the gatewaySenders property
   */
  public List<GatewaySender> getGatewaySenders() {
    if (gatewaySenders == null) {
      gatewaySenders = new ArrayList<>();
    }
    return gatewaySenders;
  }

  /**
   * Gets the value of the gatewayReceiver property.
   *
   * possible object is
   * {@link GatewayReceiverConfig }
   *
   * @return the value of the gatewayReceiver property
   */
  public GatewayReceiverConfig getGatewayReceiver() {
    return gatewayReceiver;
  }

  /**
   * Sets the value of the gatewayReceiver property.
   *
   * allowed object is
   * {@link GatewayReceiverConfig }
   *
   * @param value the value of the gatewayReceiver property
   */
  public void setGatewayReceiver(GatewayReceiverConfig value) {
    gatewayReceiver = value;
  }

  /**
   * Gets the value of the gatewayConflictResolver property.
   *
   * possible object is
   * {@link DeclarableType }
   *
   * @return the value of the gatewayConflictResolver property
   */
  public DeclarableType getGatewayConflictResolver() {
    return gatewayConflictResolver;
  }

  /**
   * Sets the value of the gatewayConflictResolver property.
   *
   * allowed object is
   * {@link DeclarableType }
   *
   * @param value the value of the gatewayConflictResolver property
   */
  public void setGatewayConflictResolver(DeclarableType value) {
    gatewayConflictResolver = value;
  }

  /**
   * Gets the value of the asyncEventQueues property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the asyncEventQueues property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getAsyncEventQueues().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link CacheConfig.AsyncEventQueue }
   *
   * @return the value of the asyncEventQueues property
   */
  public List<AsyncEventQueue> getAsyncEventQueues() {
    if (asyncEventQueues == null) {
      asyncEventQueues = new ArrayList<>();
    }
    return asyncEventQueues;
  }

  /**
   * Gets the value of the cacheServers property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the cacheServers property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getCacheServers().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link CacheConfig.CacheServer }
   *
   * @return the value of the cacheServers property
   */
  public List<CacheServer> getCacheServers() {
    if (cacheServers == null) {
      cacheServers = new ArrayList<>();
    }
    return cacheServers;
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
   * getPools().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link PoolType }
   *
   * @return the value of the pools property
   */
  public List<PoolType> getPools() {
    if (pools == null) {
      pools = new ArrayList<>();
    }
    return pools;
  }

  /**
   * Gets the value of the diskStores property.
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
   * getDiskStores().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link DiskStoreType }
   *
   * @return the value of the diskStores property
   */
  public List<DiskStoreType> getDiskStores() {
    if (diskStores == null) {
      diskStores = new ArrayList<>();
    }
    return diskStores;
  }

  /**
   * Gets the value of the pdx property.
   *
   * possible object is
   * {@link PdxType }
   *
   * @return the value of the pdx property
   */
  public PdxType getPdx() {
    return pdx;
  }

  /**
   * Sets the value of the pdx property.
   *
   * allowed object is
   * {@link PdxType }
   *
   * @param value the value of the pdx property
   */
  public void setPdx(PdxType value) {
    pdx = value;
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
   * @return the value of the regionAttributes property
   */
  public List<RegionAttributesType> getRegionAttributes() {
    if (regionAttributes == null) {
      regionAttributes = new ArrayList<>();
    }
    return regionAttributes;
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
   * @return the value of the jndiBindings property
   */
  public List<JndiBindingsType.JndiBinding> getJndiBindings() {
    if (jndiBindings == null) {
      jndiBindings = new JndiBindingsType();
    }
    return jndiBindings.getJndiBindings();
  }


  /**
   * Gets the value of the regions property.
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
   * getRegions().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link RegionConfig }
   *
   * @return the value of the regions property
   */
  public List<RegionConfig> getRegions() {
    if (regions == null) {
      regions = new ArrayList<>();
    }
    return regions;
  }

  /**
   * Gets the value of the functionService property.
   *
   * possible object is
   * {@link FunctionServiceType }
   *
   * @return the value of the functionService property
   */
  public FunctionServiceType getFunctionService() {
    return functionService;
  }

  /**
   * Sets the value of the functionService property.
   *
   * allowed object is
   * {@link FunctionServiceType }
   *
   * @param value the value of the functionService property
   */
  public void setFunctionService(FunctionServiceType value) {
    functionService = value;
  }

  /**
   * Gets the value of the resourceManager property.
   *
   * possible object is
   * {@link ResourceManagerType }
   *
   * @return the value of the resourceManager property
   */
  public ResourceManagerType getResourceManager() {
    return resourceManager;
  }

  /**
   * Sets the value of the resourceManager property.
   *
   * allowed object is
   * {@link ResourceManagerType }
   *
   * @param value the value of the resourceManager property
   */
  public void setResourceManager(ResourceManagerType value) {
    resourceManager = value;
  }

  /**
   * Gets the value of the serializationRegistration property.
   *
   * possible object is
   * {@link SerializationRegistrationType }
   *
   * @return the value of the serializationRegistration property
   */
  public SerializationRegistrationType getSerializationRegistration() {
    return serializationRegistration;
  }

  /**
   * Sets the value of the serializationRegistration property.
   *
   * allowed object is
   * {@link SerializationRegistrationType }
   *
   * @param value the value of the serializationRegistration property
   */
  public void setSerializationRegistration(SerializationRegistrationType value) {
    serializationRegistration = value;
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
   * getBackups().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link String }
   *
   * @return the value of the backup property
   */
  public List<String> getBackups() {
    if (backups == null) {
      backups = new ArrayList<>();
    }
    return backups;
  }

  /**
   * Gets the value of the initializer property.
   *
   * possible object is
   * {@link DeclarableType }
   *
   * @return the value of the initializer property
   */
  public DeclarableType getInitializer() {
    return initializer;
  }

  /**
   * Sets the value of the initializer property.
   *
   * allowed object is
   * {@link DeclarableType }
   *
   * @param value the value of the initializer property
   */
  public void setInitializer(DeclarableType value) {
    initializer = value;
  }

  /**
   * Gets the value of the customCacheElements property.
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
   * @return the value of the customCacheElements property
   */
  public List<CacheElement> getCustomCacheElements() {
    if (cacheElements == null) {
      cacheElements = new ArrayList<>();
    }
    return cacheElements;
  }

  /**
   * Gets the value of the copyOnRead property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the copyOnRead property
   */
  public Boolean isCopyOnRead() {
    return copyOnRead;
  }

  /**
   * Sets the value of the copyOnRead property.
   *
   * allowed object is
   * {@link Boolean }
   *
   * @param value the value of the copyOnRead property
   */
  public void setCopyOnRead(Boolean value) {
    copyOnRead = value;
  }

  /**
   * Gets the value of the isServer property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return the value of the isServer property
   */
  public Boolean isIsServer() {
    return isServer;
  }

  /**
   * Sets the value of the isServer property.
   *
   * allowed object is
   * {@link Boolean }
   *
   * @param value the value of the isServer property
   */
  public void setIsServer(Boolean value) {
    isServer = value;
  }

  /**
   * Gets the value of the lockTimeout property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the lockTimeout property
   */
  public String getLockTimeout() {
    return lockTimeout;
  }

  /**
   * Sets the value of the lockTimeout property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the value of the lockTimeout property
   */
  public void setLockTimeout(String value) {
    lockTimeout = value;
  }

  /**
   * Gets the value of the lockLease property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the lockLease property
   */
  public String getLockLease() {
    return lockLease;
  }

  /**
   * Sets the value of the lockLease property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the value of the lockLease property
   */
  public void setLockLease(String value) {
    lockLease = value;
  }

  /**
   * Gets the value of the messageSyncInterval property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the messageSyncInterval property
   */
  public String getMessageSyncInterval() {
    return messageSyncInterval;
  }

  /**
   * Sets the value of the messageSyncInterval property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the value of the messageSyncInterval property
   */
  public void setMessageSyncInterval(String value) {
    messageSyncInterval = value;
  }

  /**
   * Gets the value of the searchTimeout property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the searchTimeout property
   */
  public String getSearchTimeout() {
    return searchTimeout;
  }

  /**
   * Sets the value of the searchTimeout property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the value of the searchTimeout property
   */
  public void setSearchTimeout(String value) {
    searchTimeout = value;
  }

  /**
   * Gets the value of the version property.
   *
   * possible object is
   * {@link String }
   *
   * @return the value of the version property
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
   * allowed object is
   * {@link String }
   *
   * @param value the value of the version property
   */
  public void setVersion(String value) {
    version = value;
  }

  // this supports looking for sub regions
  public RegionConfig findRegionConfiguration(String regionPath) {
    if (regionPath.startsWith(Region.SEPARATOR)) {
      regionPath = regionPath.substring(1);
    }
    List<RegionConfig> regions = getRegions();
    RegionConfig found = null;
    for (String regionToken : regionPath.split(Region.SEPARATOR)) {
      found = Identifiable.find(regions, regionToken);
      // couldn't find one of the sub regions, break out of the loop
      if (found == null) {
        return null;
      }
      regions = found.getRegions();
    }
    return found;
  }

  public <T extends CacheElement> List<T> findCustomCacheElements(Class<T> classT) {
    List<T> newList = new ArrayList<>();
    // streaming won't work here, because it's trying to cast element into CacheElement
    for (Object element : getCustomCacheElements()) {
      if (classT.isInstance(element)) {
        newList.add(classT.cast(element));
      }
    }
    return newList;
  }

  public <T extends CacheElement> T findCustomCacheElement(String elementId, Class<T> classT) {
    return find(findCustomCacheElements(classT), elementId);
  }

  public <T extends CacheElement> List<T> findCustomRegionElements(String regionPath,
      Class<T> classT) {
    List<T> newList = new ArrayList<>();
    RegionConfig regionConfig = findRegionConfiguration(regionPath);
    if (regionConfig == null) {
      return newList;
    }

    // streaming won't work here, because it's trying to cast element into CacheElement
    for (Object element : regionConfig.getCustomRegionElements()) {
      if (classT.isInstance(element)) {
        newList.add(classT.cast(element));
      }
    }
    return newList;
  }

  public <T extends CacheElement> T findCustomRegionElement(String regionPath, String elementId,
      Class<T> classT) {
    return find(findCustomRegionElements(regionPath, classT), elementId);
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
   *         &lt;element name="gateway-event-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" maxOccurs="unbounded" minOccurs="0"/&gt;
   *         &lt;element name="gateway-event-substitution-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" minOccurs="0"/&gt;
   *         &lt;element name="async-event-listener" type="{http://geode.apache.org/schema/cache}class-with-parameters-type"/&gt;
   *       &lt;/sequence&gt;
   *       &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="parallel" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *       &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="enable-batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *       &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="persistent" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *       &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="disk-synchronous" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *       &lt;attribute name="dispatcher-threads" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="order-policy" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="forward-expiration-destroy" type="{http://www.w3.org/2001/XMLSchema}boolean" default="false" /&gt;
   *     &lt;/restriction&gt;
   *   &lt;/complexContent&gt;
   * &lt;/complexType&gt;
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "",
      propOrder = {"gatewayEventFilters", "gatewayEventSubstitutionFilter", "asyncEventListener"})
  public static class AsyncEventQueue extends CacheElement {

    @XmlElement(name = "gateway-event-filter", namespace = "http://geode.apache.org/schema/cache")
    protected List<DeclarableType> gatewayEventFilters;
    @XmlElement(name = "gateway-event-substitution-filter",
        namespace = "http://geode.apache.org/schema/cache")
    protected DeclarableType gatewayEventSubstitutionFilter;
    @XmlElement(name = "async-event-listener", namespace = "http://geode.apache.org/schema/cache",
        required = true)
    protected DeclarableType asyncEventListener;
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
    @XmlAttribute(name = "pause-event-processing")
    protected Boolean pauseEventProcessing;

    /**
     * Gets the value of whether the queue was created with paused processing of the events queued
     *
     *
     * @return {@link Boolean} - true if queue will be created with paused processing of the events
     *         queued
     *         - false if queue will be created without pausing the processing of the events queued
     *
     */
    public Boolean isPauseEventProcessing() {
      return pauseEventProcessing;
    }

    /**
     * Sets the value of whether the queue will be created with paused processing of the events
     * queued
     *
     * @param pauseEventProcessing {@link Boolean} - true if queue will be created with paused
     *        processing of the events queued
     *        - false if queue will be created without pausing the processing of the events
     *        queued
     */

    public void setPauseEventProcessing(Boolean pauseEventProcessing) {
      this.pauseEventProcessing = pauseEventProcessing;
    }

    /**
     * Gets the value of the gatewayEventFilters property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the gatewayEventFilters property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getGatewayEventFilters().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link DeclarableType }
     *
     * @return the value of the gatewayEventFilters property
     */
    public List<DeclarableType> getGatewayEventFilters() {
      if (gatewayEventFilters == null) {
        gatewayEventFilters = new ArrayList<>();
      }
      return gatewayEventFilters;
    }

    /**
     * Gets the value of the gatewayEventSubstitutionFilter property.
     *
     * possible object is
     * {@link DeclarableType }
     *
     * @return the value of the gatewayEventSubstitutionFilter property
     */
    public DeclarableType getGatewayEventSubstitutionFilter() {
      return gatewayEventSubstitutionFilter;
    }

    /**
     * Sets the value of the gatewayEventSubstitutionFilter property.
     *
     * allowed object is
     * {@link DeclarableType }
     *
     * @param value the value of the gatewayEventSubstitutionFilter property
     */
    public void setGatewayEventSubstitutionFilter(DeclarableType value) {
      gatewayEventSubstitutionFilter = value;
    }

    /**
     * Gets the value of the asyncEventListener property.
     *
     * possible object is
     * {@link DeclarableType }
     *
     * @return the value of the asyncEventListener property
     */
    public DeclarableType getAsyncEventListener() {
      return asyncEventListener;
    }

    /**
     * Sets the value of the asyncEventListener property.
     *
     * allowed object is
     * {@link DeclarableType }
     *
     * @param value the value of the asyncEventListener property
     */
    public void setAsyncEventListener(DeclarableType value) {
      asyncEventListener = value;
    }

    /**
     * Gets the value of the id property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the id property
     */
    @Override
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
     * Gets the value of the parallel property.
     *
     * possible object is
     * {@link Boolean }
     *
     * @return the value of the parallel property
     */
    public Boolean isParallel() {
      return parallel;
    }

    /**
     * Sets the value of the parallel property.
     *
     * allowed object is
     * {@link Boolean }
     *
     * @param value the value of the parallel property
     */
    public void setParallel(Boolean value) {
      parallel = value;
    }

    /**
     * Gets the value of the batchSize property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the batchSize property
     */
    public String getBatchSize() {
      return batchSize;
    }

    /**
     * Sets the value of the batchSize property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the value of the batchSize property
     */
    public void setBatchSize(String value) {
      batchSize = value;
    }

    /**
     * Gets the value of the batchTimeInterval property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the batchTimeInterval property
     */
    public String getBatchTimeInterval() {
      return batchTimeInterval;
    }

    /**
     * Sets the value of the batchTimeInterval property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the value of the batchTimeInterval property
     */
    public void setBatchTimeInterval(String value) {
      batchTimeInterval = value;
    }

    /**
     * Gets the value of the enableBatchConflation property.
     *
     * possible object is
     * {@link Boolean }
     *
     * @return the value of the enableBatchConflation property
     */
    public Boolean isEnableBatchConflation() {
      return enableBatchConflation;
    }

    /**
     * Sets the value of the enableBatchConflation property.
     *
     * allowed object is
     * {@link Boolean }
     *
     * @param value the value of the enableBatchConflation property
     */
    public void setEnableBatchConflation(Boolean value) {
      enableBatchConflation = value;
    }

    /**
     * Gets the value of the maximumQueueMemory property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the maximumQueueMemory property
     */
    public String getMaximumQueueMemory() {
      return maximumQueueMemory;
    }

    /**
     * Sets the value of the maximumQueueMemory property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the value of the maximumQueueMemory property
     */
    public void setMaximumQueueMemory(String value) {
      maximumQueueMemory = value;
    }

    /**
     * Gets the value of the persistent property.
     *
     * possible object is
     * {@link Boolean }
     *
     * @return the value of the persistent property
     */
    public Boolean isPersistent() {
      return persistent;
    }

    /**
     * Sets the value of the persistent property.
     *
     * allowed object is
     * {@link Boolean }
     *
     * @param value the value of the persistent property
     */
    public void setPersistent(Boolean value) {
      persistent = value;
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
     * Gets the value of the dispatcherThreads property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the dispatcherThreads property
     */
    public String getDispatcherThreads() {
      return dispatcherThreads;
    }

    /**
     * Sets the value of the dispatcherThreads property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the value of the dispatcherThreads property
     */
    public void setDispatcherThreads(String value) {
      dispatcherThreads = value;
    }

    /**
     * Gets the value of the orderPolicy property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the orderPolicy property
     */
    public String getOrderPolicy() {
      return orderPolicy;
    }

    /**
     * Sets the value of the orderPolicy property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the value of the orderPolicy property
     */
    public void setOrderPolicy(String value) {
      orderPolicy = value;
    }

    /**
     * Gets the value of the forwardExpirationDestroy property.
     *
     * possible object is
     * {@link Boolean }
     *
     * @return the value of the forwardExpirationDestroy property
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
     * allowed object is
     * {@link Boolean }
     *
     * @param value the value of the forwardExpirationDestroy property
     */
    public void setForwardExpirationDestroy(Boolean value) {
      forwardExpirationDestroy = value;
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
   *     &lt;extension base="{http://geode.apache.org/schema/cache}server-type"&gt;
   *       &lt;attribute name="tcp-no-delay" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *     &lt;/extension&gt;
   *   &lt;/complexContent&gt;
   * &lt;/complexType&gt;
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
     * possible object is
     * {@link Boolean }
     *
     * @return the value of the tcpNoDelay property
     */
    public Boolean isTcpNoDelay() {
      return tcpNoDelay;
    }

    /**
     * Sets the value of the tcpNoDelay property.
     *
     * allowed object is
     * {@link Boolean }
     *
     * @param value the value of the tcpNoDelay property
     */
    public void setTcpNoDelay(Boolean value) {
      tcpNoDelay = value;
    }

  }

  /**
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
   *         &lt;element name="gateway" maxOccurs="unbounded" minOccurs="0"&gt;
   *           &lt;complexType&gt;
   *             &lt;complexContent&gt;
   *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                 &lt;sequence&gt;
   *                   &lt;choice&gt;
   *                     &lt;element name="gateway-endpoint" maxOccurs="unbounded"&gt;
   *                       &lt;complexType&gt;
   *                         &lt;complexContent&gt;
   *                           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                             &lt;attribute name="host" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                             &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                             &lt;attribute name="port" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                           &lt;/restriction&gt;
   *                         &lt;/complexContent&gt;
   *                       &lt;/complexType&gt;
   *                     &lt;/element&gt;
   *                     &lt;element name="gateway-listener" maxOccurs="unbounded"&gt;
   *                       &lt;complexType&gt;
   *                         &lt;complexContent&gt;
   *                           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                             &lt;sequence&gt;
   *                               &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
   *                               &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
   *                             &lt;/sequence&gt;
   *                           &lt;/restriction&gt;
   *                         &lt;/complexContent&gt;
   *                       &lt;/complexType&gt;
   *                     &lt;/element&gt;
   *                   &lt;/choice&gt;
   *                   &lt;element name="gateway-queue" minOccurs="0"&gt;
   *                     &lt;complexType&gt;
   *                       &lt;complexContent&gt;
   *                         &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *                           &lt;attribute name="alert-threshold" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                           &lt;attribute name="batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *                           &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                           &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                           &lt;attribute name="enable-persistence" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *                           &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                           &lt;attribute name="roll-oplogs" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *                           &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                           &lt;attribute name="overflow-directory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                         &lt;/restriction&gt;
   *                       &lt;/complexContent&gt;
   *                     &lt;/complexType&gt;
   *                   &lt;/element&gt;
   *                 &lt;/sequence&gt;
   *                 &lt;attribute name="early-ack" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *                 &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                 &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                 &lt;attribute name="socket-read-timeout" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                 &lt;attribute name="concurrency-level" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *                 &lt;attribute name="order-policy" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *               &lt;/restriction&gt;
   *             &lt;/complexContent&gt;
   *           &lt;/complexType&gt;
   *         &lt;/element&gt;
   *       &lt;/sequence&gt;
   *       &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="bind-address" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="maximum-time-between-pings" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="port" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="startup-policy"&gt;
   *         &lt;simpleType&gt;
   *           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
   *             &lt;enumeration value="primary"/&gt;
   *             &lt;enumeration value="secondary"/&gt;
   *             &lt;enumeration value="none"/&gt;
   *           &lt;/restriction&gt;
   *         &lt;/simpleType&gt;
   *       &lt;/attribute&gt;
   *       &lt;attribute name="manual-start" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *       &lt;attribute name="max-connections" type="{http://www.w3.org/2001/XMLSchema}integer" /&gt;
   *     &lt;/restriction&gt;
   *   &lt;/complexContent&gt;
   * &lt;/complexType&gt;
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"gateways"})
  public static class GatewayHub {

    @XmlElement(name = "gateway", namespace = "http://geode.apache.org/schema/cache")
    protected List<Gateway> gateways;
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
     * @return the {@link List} of {@link Gateway}s.
     */
    public List<Gateway> getGateway() {
      if (gateways == null) {
        gateways = new ArrayList<>();
      }
      return gateways;
    }

    /**
     * Gets the value of the id property.
     *
     * possible object is
     * {@link String }
     *
     * @return the ID.
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
     * @param value the ID.
     */
    public void setId(String value) {
      id = value;
    }

    /**
     * Gets the value of the bindAddress property.
     *
     * possible object is
     * {@link String }
     *
     * @return the bind address.
     */
    public String getBindAddress() {
      return bindAddress;
    }

    /**
     * Sets the value of the bindAddress property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the bind address.
     */
    public void setBindAddress(String value) {
      bindAddress = value;
    }

    /**
     * Gets the value of the maximumTimeBetweenPings property.
     *
     * possible object is
     * {@link String }
     *
     * @return the maximum time between pings.
     */
    public String getMaximumTimeBetweenPings() {
      return maximumTimeBetweenPings;
    }

    /**
     * Sets the value of the maximumTimeBetweenPings property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the maximum time between pings.
     */
    public void setMaximumTimeBetweenPings(String value) {
      maximumTimeBetweenPings = value;
    }

    /**
     * Gets the value of the port property.
     *
     * possible object is
     * {@link String }
     *
     * @return the port value.
     */
    public String getPort() {
      return port;
    }

    /**
     * Sets the value of the port property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the port number.
     */
    public void setPort(String value) {
      port = value;
    }

    /**
     * Gets the value of the socketBufferSize property.
     *
     * possible object is
     * {@link String }
     *
     * @return the socket buffer size.
     */
    public String getSocketBufferSize() {
      return socketBufferSize;
    }

    /**
     * Sets the value of the socketBufferSize property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the socket buffer size.
     */
    public void setSocketBufferSize(String value) {
      socketBufferSize = value;
    }

    /**
     * Gets the value of the startupPolicy property.
     *
     * possible object is
     * {@link String }
     *
     * @return the startup policy.
     */
    public String getStartupPolicy() {
      return startupPolicy;
    }

    /**
     * Sets the value of the startupPolicy property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the startup policy.
     */
    public void setStartupPolicy(String value) {
      startupPolicy = value;
    }

    /**
     * Gets the value of the manualStart property.
     *
     * possible object is
     * {@link Boolean }
     *
     * @return true if manual start is enabled, false otherwise.
     */
    public Boolean isManualStart() {
      return manualStart;
    }

    /**
     * Sets the value of the manualStart property.
     *
     * allowed object is
     * {@link Boolean }
     *
     * @param value enables or disabled manual start.
     */
    public void setManualStart(Boolean value) {
      manualStart = value;
    }

    /**
     * Gets the value of the maxConnections property.
     *
     * possible object is
     * {@link BigInteger }
     *
     * @return the maximum number of connections.
     */
    public BigInteger getMaxConnections() {
      return maxConnections;
    }

    /**
     * Sets the value of the maxConnections property.
     *
     * allowed object is
     * {@link BigInteger }
     *
     * @param value the maximum number of connections.
     */
    public void setMaxConnections(BigInteger value) {
      maxConnections = value;
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
     *         &lt;choice&gt;
     *           &lt;element name="gateway-endpoint" maxOccurs="unbounded"&gt;
     *             &lt;complexType&gt;
     *               &lt;complexContent&gt;
     *                 &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
     *                   &lt;attribute name="host" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *                   &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *                   &lt;attribute name="port" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *                 &lt;/restriction&gt;
     *               &lt;/complexContent&gt;
     *             &lt;/complexType&gt;
     *           &lt;/element&gt;
     *           &lt;element name="gateway-listener" maxOccurs="unbounded"&gt;
     *             &lt;complexType&gt;
     *               &lt;complexContent&gt;
     *                 &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
     *                   &lt;sequence&gt;
     *                     &lt;element name="class-name" type="{http://geode.apache.org/schema/cache}class-name-type"/&gt;
     *                     &lt;element name="parameter" type="{http://geode.apache.org/schema/cache}parameter-type" maxOccurs="unbounded" minOccurs="0"/&gt;
     *                   &lt;/sequence&gt;
     *                 &lt;/restriction&gt;
     *               &lt;/complexContent&gt;
     *             &lt;/complexType&gt;
     *           &lt;/element&gt;
     *         &lt;/choice&gt;
     *         &lt;element name="gateway-queue" minOccurs="0"&gt;
     *           &lt;complexType&gt;
     *             &lt;complexContent&gt;
     *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
     *                 &lt;attribute name="alert-threshold" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *                 &lt;attribute name="batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
     *                 &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *                 &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *                 &lt;attribute name="enable-persistence" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
     *                 &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *                 &lt;attribute name="roll-oplogs" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
     *                 &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *                 &lt;attribute name="overflow-directory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *               &lt;/restriction&gt;
     *             &lt;/complexContent&gt;
     *           &lt;/complexType&gt;
     *         &lt;/element&gt;
     *       &lt;/sequence&gt;
     *       &lt;attribute name="early-ack" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
     *       &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *       &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *       &lt;attribute name="socket-read-timeout" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *       &lt;attribute name="concurrency-level" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *       &lt;attribute name="order-policy" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
     *     &lt;/restriction&gt;
     *   &lt;/complexContent&gt;
     * &lt;/complexType&gt;
     * </pre>
     *
     *
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "", propOrder = {"gatewayEndpoints", "gatewayListeners", "gatewayQueue"})
    public static class Gateway {

      @XmlElement(name = "gateway-endpoint", namespace = "http://geode.apache.org/schema/cache")
      protected List<GatewayEndpoint> gatewayEndpoints;
      @XmlElement(name = "gateway-listener", namespace = "http://geode.apache.org/schema/cache")
      protected List<DeclarableType> gatewayListeners;
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
       * Gets the value of the gatewayEndpoints property.
       *
       * <p>
       * This accessor method returns a reference to the live list,
       * not a snapshot. Therefore any modification you make to the
       * returned list will be present inside the JAXB object.
       * This is why there is not a <CODE>set</CODE> method for the gatewayEndpoints property.
       *
       * <p>
       * For example, to add a new item, do as follows:
       *
       * <pre>
       * getGatewayEndpoints().add(newItem);
       * </pre>
       *
       *
       * <p>
       * Objects of the following type(s) are allowed in the list
       * {@link CacheConfig.GatewayHub.Gateway.GatewayEndpoint }
       *
       * @return the {@link List} of {@link GatewayEndpoint}s.
       */
      public List<GatewayEndpoint> getGatewayEndpoints() {
        if (gatewayEndpoints == null) {
          gatewayEndpoints = new ArrayList<>();
        }
        return gatewayEndpoints;
      }

      /**
       * Gets the value of the gatewayListeners property.
       *
       * <p>
       * This accessor method returns a reference to the live list,
       * not a snapshot. Therefore any modification you make to the
       * returned list will be present inside the JAXB object.
       * This is why there is not a <CODE>set</CODE> method for the gatewayListeners property.
       *
       * <p>
       * For example, to add a new item, do as follows:
       *
       * <pre>
       * getGatewayListeners().add(newItem);
       * </pre>
       *
       *
       * <p>
       * Objects of the following type(s) are allowed in the list
       * {@link DeclarableType }
       *
       * @return the {@link List} of gateway listeners.
       */
      public List<DeclarableType> getGatewayListeners() {
        if (gatewayListeners == null) {
          gatewayListeners = new ArrayList<>();
        }
        return gatewayListeners;
      }

      /**
       * Gets the value of the gatewayQueue property.
       *
       * possible object is
       * {@link CacheConfig.GatewayHub.Gateway.GatewayQueue }
       *
       * @return the {@link GatewayQueue}.
       */
      public CacheConfig.GatewayHub.Gateway.GatewayQueue getGatewayQueue() {
        return gatewayQueue;
      }

      /**
       * Sets the value of the gatewayQueue property.
       *
       * allowed object is
       * {@link CacheConfig.GatewayHub.Gateway.GatewayQueue }
       *
       * @param value the value of the gatewayQueue property
       */
      public void setGatewayQueue(CacheConfig.GatewayHub.Gateway.GatewayQueue value) {
        gatewayQueue = value;
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
       * Gets the value of the socketBufferSize property.
       *
       * possible object is
       * {@link String }
       *
       * @return the value of the socketBufferSize property
       */
      public String getSocketBufferSize() {
        return socketBufferSize;
      }

      /**
       * Sets the value of the socketBufferSize property.
       *
       * allowed object is
       * {@link String }
       *
       * @param value the value of the socketBufferSize property
       */
      public void setSocketBufferSize(String value) {
        socketBufferSize = value;
      }

      /**
       * Gets the value of the socketReadTimeout property.
       *
       * possible object is
       * {@link String }
       *
       * @return the value of the socketReadTimeout property
       */
      public String getSocketReadTimeout() {
        return socketReadTimeout;
      }

      /**
       * Sets the value of the socketReadTimeout property.
       *
       * allowed object is
       * {@link String }
       *
       * @param value the value of the socketReadTimeout property
       */
      public void setSocketReadTimeout(String value) {
        socketReadTimeout = value;
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
       * Gets the value of the orderPolicy property.
       *
       * possible object is
       * {@link String }
       *
       * @return the value of the orderPolicy property
       */
      public String getOrderPolicy() {
        return orderPolicy;
      }

      /**
       * Sets the value of the orderPolicy property.
       *
       * allowed object is
       * {@link String }
       *
       * @param value the value of the orderPolicy property
       */
      public void setOrderPolicy(String value) {
        orderPolicy = value;
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
       *       &lt;attribute name="host" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
       *       &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
       *       &lt;attribute name="port" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
       *     &lt;/restriction&gt;
       *   &lt;/complexContent&gt;
       * &lt;/complexType&gt;
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
         * possible object is
         * {@link String }
         *
         * @return the value of the host property
         */
        public String getHost() {
          return host;
        }

        /**
         * Sets the value of the host property.
         *
         * allowed object is
         * {@link String }
         *
         * @param value the value of the host property
         */
        public void setHost(String value) {
          host = value;
        }

        /**
         * Gets the value of the id property.
         *
         * possible object is
         * {@link String }
         *
         * @return the ID.
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
         * @param value the ID.
         */
        public void setId(String value) {
          id = value;
        }

        /**
         * Gets the value of the port property.
         *
         * possible object is
         * {@link String }
         *
         * @return the port number.
         */
        public String getPort() {
          return port;
        }

        /**
         * Sets the value of the port property.
         *
         * allowed object is
         * {@link String }
         *
         * @param value the port number.
         */
        public void setPort(String value) {
          port = value;
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
       *       &lt;attribute name="alert-threshold" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
       *       &lt;attribute name="batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
       *       &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
       *       &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
       *       &lt;attribute name="enable-persistence" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
       *       &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
       *       &lt;attribute name="roll-oplogs" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
       *       &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
       *       &lt;attribute name="overflow-directory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
       *     &lt;/restriction&gt;
       *   &lt;/complexContent&gt;
       * &lt;/complexType&gt;
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
         * possible object is
         * {@link String }
         *
         * @return the alert threshold.
         */
        public String getAlertThreshold() {
          return alertThreshold;
        }

        /**
         * Sets the value of the alertThreshold property.
         *
         * allowed object is
         * {@link String }
         *
         * @param value the alert threshold.
         */
        public void setAlertThreshold(String value) {
          alertThreshold = value;
        }

        /**
         * Gets the value of the batchConflation property.
         *
         * possible object is
         * {@link Boolean }
         *
         * @return true if batch conflation is enabled.
         */
        public Boolean isBatchConflation() {
          return batchConflation;
        }

        /**
         * Sets the value of the batchConflation property.
         *
         * allowed object is
         * {@link Boolean }
         *
         * @param value enables of disables batch conflation.
         */
        public void setBatchConflation(Boolean value) {
          batchConflation = value;
        }

        /**
         * Gets the value of the batchSize property.
         *
         * possible object is
         * {@link String }
         *
         * @return the batch size.
         */
        public String getBatchSize() {
          return batchSize;
        }

        /**
         * Sets the value of the batchSize property.
         *
         * allowed object is
         * {@link String }
         *
         * @param value the batch size.
         */
        public void setBatchSize(String value) {
          batchSize = value;
        }

        /**
         * Gets the value of the batchTimeInterval property.
         *
         * possible object is
         * {@link String }
         *
         * @return the batch time interval.
         */
        public String getBatchTimeInterval() {
          return batchTimeInterval;
        }

        /**
         * Sets the value of the batchTimeInterval property.
         *
         * allowed object is
         * {@link String }
         *
         * @param value the batch time interval.
         */
        public void setBatchTimeInterval(String value) {
          batchTimeInterval = value;
        }

        /**
         * Gets the value of the enablePersistence property.
         *
         * possible object is
         * {@link Boolean }
         *
         * @return true if persistence is enabled, false otherwise.
         */
        public Boolean isEnablePersistence() {
          return enablePersistence;
        }

        /**
         * Sets the value of the enablePersistence property.
         *
         * allowed object is
         * {@link Boolean }
         *
         * @param value enables or disables persistence.
         */
        public void setEnablePersistence(Boolean value) {
          enablePersistence = value;
        }

        /**
         * Gets the value of the diskStoreName property.
         *
         * possible object is
         * {@link String }
         *
         * @return the disk store name.
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
         * @param value the disk store name.
         */
        public void setDiskStoreName(String value) {
          diskStoreName = value;
        }

        /**
         * Gets the value of the rollOplogs property.
         *
         * possible object is
         * {@link Boolean }
         *
         * @return true if opslog rolling is enabled, false otherwise.
         */
        public Boolean isRollOplogs() {
          return rollOplogs;
        }

        /**
         * Sets the value of the rollOplogs property.
         *
         * allowed object is
         * {@link Boolean }
         *
         * @param value enables or disabled oplog rolling.
         */
        public void setRollOplogs(Boolean value) {
          rollOplogs = value;
        }

        /**
         * Gets the value of the maximumQueueMemory property.
         *
         * possible object is
         * {@link String }
         *
         * @return the maximum queue memory.
         */
        public String getMaximumQueueMemory() {
          return maximumQueueMemory;
        }

        /**
         * Sets the value of the maximumQueueMemory property.
         *
         * allowed object is
         * {@link String }
         *
         * @param value the maximum queue memory.
         */
        public void setMaximumQueueMemory(String value) {
          maximumQueueMemory = value;
        }

        /**
         * Gets the value of the overflowDirectory property.
         *
         * possible object is
         * {@link String }
         *
         * @return the overflow directory.
         */
        public String getOverflowDirectory() {
          return overflowDirectory;
        }

        /**
         * Sets the value of the overflowDirectory property.
         *
         * allowed object is
         * {@link String }
         *
         * @param value the overflow directory.
         */
        public void setOverflowDirectory(String value) {
          overflowDirectory = value;
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
   * &lt;complexType&gt;
   *   &lt;complexContent&gt;
   *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
   *       &lt;sequence&gt;
   *         &lt;element name="gateway-event-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" maxOccurs="unbounded" minOccurs="0"/&gt;
   *         &lt;element name="gateway-event-substitution-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" minOccurs="0"/&gt;
   *         &lt;element name="gateway-transport-filter" type="{http://geode.apache.org/schema/cache}class-with-parameters-type" maxOccurs="unbounded" minOccurs="0"/&gt;
   *       &lt;/sequence&gt;
   *       &lt;attribute name="id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="remote-distributed-system-id" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="parallel" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *       &lt;attribute name="type" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="manual-start" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *       &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="socket-read-timeout" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="enable-batch-conflation" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *       &lt;attribute name="batch-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="batch-time-interval" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="enable-persistence" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *       &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="disk-synchronous" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *       &lt;attribute name="maximum-queue-memory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="alert-threshold" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="dispatcher-threads" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="order-policy" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="group-transaction-events" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
   *     &lt;/restriction&gt;
   *   &lt;/complexContent&gt;
   * &lt;/complexType&gt;
   * </pre>
   *
   *
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "", propOrder = {"gatewayEventFilters", "gatewayEventSubstitutionFilter",
      "gatewayTransportFilters"})
  public static class GatewaySender {

    @XmlElement(name = "gateway-event-filter", namespace = "http://geode.apache.org/schema/cache")
    protected List<DeclarableType> gatewayEventFilters;
    @XmlElement(name = "gateway-event-substitution-filter",
        namespace = "http://geode.apache.org/schema/cache")
    protected DeclarableType gatewayEventSubstitutionFilter;
    @XmlElement(name = "gateway-transport-filter",
        namespace = "http://geode.apache.org/schema/cache")
    protected List<DeclarableType> gatewayTransportFilters;
    @XmlAttribute(name = "id", required = true)
    protected String id;
    @XmlAttribute(name = "remote-distributed-system-id", required = true)
    protected String remoteDistributedSystemId;
    @XmlAttribute(name = "parallel")
    protected Boolean parallel;
    @XmlAttribute(name = "type")
    protected String type;
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
    @XmlAttribute(name = "group-transaction-events")
    protected Boolean groupTransactionEvents;
    @XmlAttribute(name = "enforce-threads-connect-same-receiver")
    protected Boolean enforceThreadsConnectSameReceiver;

    /**
     * Gets the value of the gatewayEventFilters property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the gatewayEventFilters property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getGatewayEventFilters().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link DeclarableType }
     *
     * @return the {@link List} of {@link GatewayEventFilter} types.
     */
    public List<DeclarableType> getGatewayEventFilters() {
      if (gatewayEventFilters == null) {
        gatewayEventFilters = new ArrayList<>();
      }
      return gatewayEventFilters;
    }

    public boolean areGatewayEventFiltersUpdated() {
      return gatewayEventFilters != null;
    }

    /**
     * Gets the value of the gatewayEventSubstitutionFilter property.
     *
     * possible object is
     * {@link DeclarableType }
     *
     * @return the {@link GatewayEventSubstitutionFilter} type.
     */
    public DeclarableType getGatewayEventSubstitutionFilter() {
      return gatewayEventSubstitutionFilter;
    }

    /**
     * Sets the value of the gatewayEventSubstitutionFilter property.
     *
     * allowed object is
     * {@link DeclarableType }
     *
     * @param value the {@link GatewayEventSubstitutionFilter} type.
     */
    public void setGatewayEventSubstitutionFilter(DeclarableType value) {
      gatewayEventSubstitutionFilter = value;
    }

    /**
     * Gets the value of the gatewayTransportFilters property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the gatewayTransportFilters property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     *
     * <pre>
     * getGatewayTransportFilters().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link DeclarableType }
     *
     * @return the {@link List} of {@link GatewayTransportFilter} types.
     */
    public List<DeclarableType> getGatewayTransportFilters() {
      if (gatewayTransportFilters == null) {
        gatewayTransportFilters = new ArrayList<>();
      }
      return gatewayTransportFilters;
    }

    /**
     * Gets the value of the id property.
     *
     * possible object is
     * {@link String }
     *
     * @return the ID.
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
     * @param value the ID.
     */
    public void setId(String value) {
      id = value;
    }

    /**
     * Gets the value of the remoteDistributedSystemId property.
     *
     * possible object is
     * {@link String }
     *
     * @return the remote distributed system ID.
     */
    public String getRemoteDistributedSystemId() {
      return remoteDistributedSystemId;
    }

    /**
     * Sets the value of the remoteDistributedSystemId property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the remote distributed system ID.
     */
    public void setRemoteDistributedSystemId(String value) {
      remoteDistributedSystemId = value;
    }

    /**
     * Gets the value of the mustGroupTransactionEvents property.
     *
     * possible object is
     * {@link Boolean }
     *
     * @return the value of the property.
     */
    public Boolean mustGroupTransactionEvents() {
      return groupTransactionEvents;
    }

    /**
     * Sets the value of the groupTransactionsEvent property.
     *
     * allowed object is
     * {@link Boolean }
     *
     * @param value the value for the property.
     */
    public void setGroupTransactionEvents(Boolean value) {
      groupTransactionEvents = value;
    }

    /**
     * Gets the value of the parallel property.
     *
     * possible object is
     * {@link Boolean }
     *
     * @return true if parallel is enabled, false otherwise.
     */
    public Boolean isParallel() {
      return parallel;
    }

    /**
     * Sets the value of the parallel property.
     *
     * allowed object is
     * {@link Boolean }
     *
     * @param value enable or disable parallel.
     */
    public void setParallel(Boolean value) {
      parallel = value;
    }

    /**
     * Gets the value of the type property.
     *
     * possible object is
     * {@link String }
     *
     * @return the type.
     */
    public String getType() {
      return type;
    }

    /**
     * Sets the value of the type property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the type.
     */
    public void setType(String value) {
      this.type = value;
    }

    /**
     * Gets the value of the manualStart property.
     *
     * possible object is
     * {@link Boolean }
     *
     * @return true is manual start is enabled, false otherwise.
     */
    public Boolean isManualStart() {
      return manualStart;
    }

    /**
     * Sets the value of the manualStart property.
     *
     * allowed object is
     * {@link Boolean }
     *
     * @param value enables or disables manual start.
     */
    public void setManualStart(Boolean value) {
      manualStart = value;
    }

    /**
     * Gets the value of the socketBufferSize property.
     *
     * possible object is
     * {@link String }
     *
     * @return the socket buffer size.
     */
    public String getSocketBufferSize() {
      return socketBufferSize;
    }

    /**
     * Sets the value of the socketBufferSize property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the socket buffer size.
     */
    public void setSocketBufferSize(String value) {
      socketBufferSize = value;
    }

    /**
     * Gets the value of the socketReadTimeout property.
     *
     * possible object is
     * {@link String }
     *
     * @return the socket read timeout.
     */
    public String getSocketReadTimeout() {
      return socketReadTimeout;
    }

    /**
     * Sets the value of the socketReadTimeout property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the socket read timeout.
     */
    public void setSocketReadTimeout(String value) {
      socketReadTimeout = value;
    }

    /**
     * Gets the value of the enableBatchConflation property.
     *
     * possible object is
     * {@link Boolean }
     *
     * @return true if batch conflation is enabled, false otherwise.
     */
    public Boolean isEnableBatchConflation() {
      return enableBatchConflation;
    }

    /**
     * Sets the value of the enableBatchConflation property.
     *
     * allowed object is
     * {@link Boolean }
     *
     * @param value enable or disable batch conflation.
     */
    public void setEnableBatchConflation(Boolean value) {
      enableBatchConflation = value;
    }

    /**
     * Gets the value of the batchSize property.
     *
     * possible object is
     * {@link String }
     *
     * @return the batch size.
     */
    public String getBatchSize() {
      return batchSize;
    }

    /**
     * Sets the value of the batchSize property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the batch size.
     */
    public void setBatchSize(String value) {
      batchSize = value;
    }

    /**
     * Gets the value of the batchTimeInterval property.
     *
     * possible object is
     * {@link String }
     *
     * @return the batch time interval.
     */
    public String getBatchTimeInterval() {
      return batchTimeInterval;
    }

    /**
     * Sets the value of the batchTimeInterval property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the batch time interval.
     */
    public void setBatchTimeInterval(String value) {
      batchTimeInterval = value;
    }

    /**
     * Gets the value of the enablePersistence property.
     *
     * possible object is
     * {@link Boolean }
     *
     * @return true if persistence is enabled, false otherwise.
     */
    public Boolean isEnablePersistence() {
      return enablePersistence;
    }

    /**
     * Sets the value of the enablePersistence property.
     *
     * allowed object is
     * {@link Boolean }
     *
     * @param value enables or disables persistence.
     */
    public void setEnablePersistence(Boolean value) {
      enablePersistence = value;
    }

    /**
     * Gets the value of the diskStoreName property.
     *
     * possible object is
     * {@link String }
     *
     * @return disk store name.
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
     * @param value disk store name.
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
     * @return true if the disk is synchronous, false if asynchronous.
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
     * @param value true for synchronous, false for asynchronous.
     */
    public void setDiskSynchronous(Boolean value) {
      diskSynchronous = value;
    }

    /**
     * Gets the value of the maximumQueueMemory property.
     *
     * possible object is
     * {@link String }
     *
     * @return the maximum queue memory.
     */
    public String getMaximumQueueMemory() {
      return maximumQueueMemory;
    }

    /**
     * Sets the value of the maximumQueueMemory property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the maximum queue memory.
     */
    public void setMaximumQueueMemory(String value) {
      maximumQueueMemory = value;
    }

    /**
     * Gets the value of the alertThreshold property.
     *
     * possible object is
     * {@link String }
     *
     * @return the alert threshold.
     */
    public String getAlertThreshold() {
      return alertThreshold;
    }

    /**
     * Sets the value of the alertThreshold property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the alert threshold.
     */
    public void setAlertThreshold(String value) {
      alertThreshold = value;
    }

    /**
     * Gets the value of the dispatcherThreads property.
     *
     * possible object is
     * {@link String }
     *
     * @return the number of dispatched threads.
     */
    public String getDispatcherThreads() {
      return dispatcherThreads;
    }

    /**
     * Sets the value of the dispatcherThreads property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the number of dispatched threads.
     */
    public void setDispatcherThreads(String value) {
      dispatcherThreads = value;
    }

    /**
     * Gets the value of the orderPolicy property.
     *
     * possible object is
     * {@link String }
     *
     * @return the order policy.
     */
    public String getOrderPolicy() {
      return orderPolicy;
    }

    /**
     * Sets the value of the orderPolicy property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the order policy.
     */
    public void setOrderPolicy(String value) {
      orderPolicy = value;
    }

    /**
     * Sets the value of the enforceThreadsConnectSameReceiver property.
     *
     * allowed object is
     * {@link Boolean }
     *
     * @param value if true, threads ensure they connect to the same receiver, false if they do not.
     */
    public void setEnforceThreadsConnectSameReceiver(Boolean value) {
      enforceThreadsConnectSameReceiver = value;
    }

    /**
     * Gets the value of the enforceThreadsConnectSameReceiver property.
     *
     * possible object is
     * {@link Boolean }
     *
     * @return true if all threads ensure they connect to the same receiver, false otherwise.
     */
    public Boolean getEnforceThreadsConnectSameReceiver() {
      return enforceThreadsConnectSameReceiver;
    }
  }

}
