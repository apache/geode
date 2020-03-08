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

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


/**
 *
 * A "pool" element specifies a client to server connection pool.
 *
 *
 * <p>
 * Java class for pool-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="pool-type">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;choice>
 *         &lt;element name="locator" maxOccurs="unbounded">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;attribute name="host" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="port" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="server" maxOccurs="unbounded">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;attribute name="host" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="port" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *       &lt;/choice>
 *       &lt;attribute name="subscription-timeout-multiplier" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="socket-connect-timeout" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="free-connection-timeout" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="server-connection-timeout" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="load-conditioning-interval" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="min-connections" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="max-connections" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="retry-attempts" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="idle-timeout" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="ping-interval" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="read-timeout" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="server-group" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="subscription-enabled" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="subscription-message-tracking-timeout" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="subscription-ack-interval" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="subscription-redundancy" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="statistic-interval" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="thread-local-connections" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="pr-single-hop-enabled" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="multiuser-authentication" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "pool-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"locators", "servers"})
@Experimental
public class PoolType {

  @XmlElement(name = "locator", namespace = "http://geode.apache.org/schema/cache")
  protected List<Locator> locators;
  @XmlElement(name = "server", namespace = "http://geode.apache.org/schema/cache")
  protected List<Server> servers;
  @XmlAttribute(name = "subscription-timeout-multiplier")
  private String subscriptionTimeoutMultiplier;
  @XmlAttribute(name = "socket-connect-timeout")
  private String socketConnectTimeout;
  @XmlAttribute(name = "free-connection-timeout")
  protected String freeConnectionTimeout;
  @XmlAttribute(name = "server-connection-timeout")
  protected String serverConnectionTimeout;
  @XmlAttribute(name = "load-conditioning-interval")
  protected String loadConditioningInterval;
  @XmlAttribute(name = "min-connections")
  protected String minConnections;
  @XmlAttribute(name = "max-connections")
  protected String maxConnections;
  @XmlAttribute(name = "retry-attempts")
  protected String retryAttempts;
  @XmlAttribute(name = "idle-timeout")
  protected String idleTimeout;
  @XmlAttribute(name = "ping-interval")
  protected String pingInterval;
  @XmlAttribute(name = "name", required = true)
  protected String name;
  @XmlAttribute(name = "read-timeout")
  protected String readTimeout;
  @XmlAttribute(name = "server-group")
  protected String serverGroup;
  @XmlAttribute(name = "socket-buffer-size")
  protected String socketBufferSize;
  @XmlAttribute(name = "subscription-enabled")
  protected Boolean subscriptionEnabled;
  @XmlAttribute(name = "subscription-message-tracking-timeout")
  private String subscriptionMessageTrackingTimeout;
  @XmlAttribute(name = "subscription-ack-interval")
  protected String subscriptionAckInterval;
  @XmlAttribute(name = "subscription-redundancy")
  protected String subscriptionRedundancy;
  @XmlAttribute(name = "statistic-interval")
  protected String statisticInterval;
  @Deprecated
  @XmlAttribute(name = "thread-local-connections")
  protected Boolean threadLocalConnections;
  @XmlAttribute(name = "pr-single-hop-enabled")
  protected Boolean prSingleHopEnabled;
  @XmlAttribute(name = "multiuser-authentication")
  private Boolean multiuserAuthentication;

  /**
   * Gets the value of the locator property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the locator property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getLocators().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link PoolType.Locator }
   *
   *
   */
  public List<Locator> getLocators() {
    if (locators == null) {
      locators = new ArrayList<>();
    }
    return locators;
  }

  /**
   * Gets the value of the server property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the server property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getServers().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link PoolType.Server }
   *
   *
   */
  public List<Server> getServers() {
    if (servers == null) {
      servers = new ArrayList<>();
    }
    return servers;
  }

  /**
   * Gets the value of the subscriptionTimeoutMultiplier property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getSubscriptionTimeoutMultiplier() {
    return subscriptionTimeoutMultiplier;
  }

  /**
   * Sets the value of the subscriptionTimeoutMultiplier property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setSubscriptionTimeoutMultiplier(String value) {
    subscriptionTimeoutMultiplier = value;
  }

  /**
   * Gets the value of the socketConnectTimeout property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getSocketConnectTimeout() {
    return socketConnectTimeout;
  }

  /**
   * Sets the value of the socketConnectTimeout property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setSocketConnectTimeout(String value) {
    socketConnectTimeout = value;
  }

  /**
   * Gets the value of the freeConnectionTimeout property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getFreeConnectionTimeout() {
    return freeConnectionTimeout;
  }

  /**
   * Sets the value of the freeConnectionTimeout property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setFreeConnectionTimeout(String value) {
    freeConnectionTimeout = value;
  }

  /**
   * Gets the value of the serverConnectionTimeout property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getServerConnectionTimeout() {
    return serverConnectionTimeout;
  }

  /**
   * Sets the value of the serverConnectionTimeout property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setServerConnectionTimeout(String value) {
    serverConnectionTimeout = value;
  }

  /**
   * Gets the value of the loadConditioningInterval property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getLoadConditioningInterval() {
    return loadConditioningInterval;
  }

  /**
   * Sets the value of the loadConditioningInterval property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setLoadConditioningInterval(String value) {
    loadConditioningInterval = value;
  }

  /**
   * Gets the value of the minConnections property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getMinConnections() {
    return minConnections;
  }

  /**
   * Sets the value of the minConnections property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setMinConnections(String value) {
    minConnections = value;
  }

  /**
   * Gets the value of the maxConnections property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getMaxConnections() {
    return maxConnections;
  }

  /**
   * Sets the value of the maxConnections property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setMaxConnections(String value) {
    maxConnections = value;
  }

  /**
   * Gets the value of the retryAttempts property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getRetryAttempts() {
    return retryAttempts;
  }

  /**
   * Sets the value of the retryAttempts property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setRetryAttempts(String value) {
    retryAttempts = value;
  }

  /**
   * Gets the value of the idleTimeout property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getIdleTimeout() {
    return idleTimeout;
  }

  /**
   * Sets the value of the idleTimeout property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setIdleTimeout(String value) {
    idleTimeout = value;
  }

  /**
   * Gets the value of the pingInterval property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getPingInterval() {
    return pingInterval;
  }

  /**
   * Sets the value of the pingInterval property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setPingInterval(String value) {
    pingInterval = value;
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
    name = value;
  }

  /**
   * Gets the value of the readTimeout property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getReadTimeout() {
    return readTimeout;
  }

  /**
   * Sets the value of the readTimeout property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setReadTimeout(String value) {
    readTimeout = value;
  }

  /**
   * Gets the value of the serverGroup property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getServerGroup() {
    return serverGroup;
  }

  /**
   * Sets the value of the serverGroup property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setServerGroup(String value) {
    serverGroup = value;
  }

  /**
   * Gets the value of the socketBufferSize property.
   *
   * possible object is
   * {@link String }
   *
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
   */
  public void setSocketBufferSize(String value) {
    socketBufferSize = value;
  }

  /**
   * Gets the value of the subscriptionEnabled property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isSubscriptionEnabled() {
    return subscriptionEnabled;
  }

  /**
   * Sets the value of the subscriptionEnabled property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setSubscriptionEnabled(Boolean value) {
    subscriptionEnabled = value;
  }

  /**
   * Gets the value of the subscriptionMessageTrackingTimeout property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getSubscriptionMessageTrackingTimeout() {
    return subscriptionMessageTrackingTimeout;
  }

  /**
   * Sets the value of the subscriptionMessageTrackingTimeout property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setSubscriptionMessageTrackingTimeout(String value) {
    subscriptionMessageTrackingTimeout = value;
  }

  /**
   * Gets the value of the subscriptionAckInterval property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getSubscriptionAckInterval() {
    return subscriptionAckInterval;
  }

  /**
   * Sets the value of the subscriptionAckInterval property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setSubscriptionAckInterval(String value) {
    subscriptionAckInterval = value;
  }

  /**
   * Gets the value of the subscriptionRedundancy property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getSubscriptionRedundancy() {
    return subscriptionRedundancy;
  }

  /**
   * Sets the value of the subscriptionRedundancy property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setSubscriptionRedundancy(String value) {
    subscriptionRedundancy = value;
  }

  /**
   * Gets the value of the statisticInterval property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getStatisticInterval() {
    return statisticInterval;
  }

  /**
   * Sets the value of the statisticInterval property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setStatisticInterval(String value) {
    statisticInterval = value;
  }

  /**
   * Gets the value of the threadLocalConnections property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @deprecated Since Geode 1.10.0. Thread local connections are ignored. Will be removed in future
   *             major release.
   */
  @Deprecated
  public Boolean isThreadLocalConnections() {
    return threadLocalConnections;
  }

  /**
   * Sets the value of the threadLocalConnections property.
   *
   * allowed object is
   * {@link Boolean }
   *
   * @deprecated Since Geode 1.10.0. Thread local connections are ignored. Will be removed in future
   *             major release.
   */
  @Deprecated
  public void setThreadLocalConnections(Boolean value) {
    threadLocalConnections = value;
  }

  /**
   * Gets the value of the prSingleHopEnabled property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isPrSingleHopEnabled() {
    return prSingleHopEnabled;
  }

  /**
   * Sets the value of the prSingleHopEnabled property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setPrSingleHopEnabled(Boolean value) {
    prSingleHopEnabled = value;
  }

  /**
   * Gets the value of the multiuserAuthentication property.
   *
   * possible object is
   * {@link Boolean }
   *
   */
  public Boolean isMultiuserAuthentication() {
    return multiuserAuthentication;
  }

  /**
   * Sets the value of the multiuserAuthentication property.
   *
   * allowed object is
   * {@link Boolean }
   *
   */
  public void setMultiuserAuthentication(Boolean value) {
    multiuserAuthentication = value;
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
  public static class Locator {

    @XmlAttribute(name = "host", required = true)
    protected String host;
    @XmlAttribute(name = "port", required = true)
    protected String port;

    /**
     * Gets the value of the host property.
     *
     * possible object is
     * {@link String }
     *
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
     */
    public void setHost(String value) {
      host = value;
    }

    /**
     * Gets the value of the port property.
     *
     * possible object is
     * {@link String }
     *
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
   * &lt;complexType>
   *   &lt;complexContent>
   *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
   *       &lt;attribute name="host" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
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
  public static class Server {

    @XmlAttribute(name = "host", required = true)
    protected String host;
    @XmlAttribute(name = "port", required = true)
    protected String port;

    /**
     * Gets the value of the host property.
     *
     * possible object is
     * {@link String }
     *
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
     */
    public void setHost(String value) {
      host = value;
    }

    /**
     * Gets the value of the port property.
     *
     * possible object is
     * {@link String }
     *
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
     */
    public void setPort(String value) {
      port = value;
    }

  }

}
