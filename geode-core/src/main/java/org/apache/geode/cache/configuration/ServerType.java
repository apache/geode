
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

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlSeeAlso;
import jakarta.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.cache.partitioned.LoadProbe;


/**
 * <p>
 * Java class for server-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="server-type"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="group" type="{http://www.w3.org/2001/XMLSchema}string" maxOccurs="unbounded" minOccurs="0"/&gt;
 *         &lt;element name="client-subscription" minOccurs="0"&gt;
 *           &lt;complexType&gt;
 *             &lt;complexContent&gt;
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *                 &lt;attribute name="eviction-policy" use="required"&gt;
 *                   &lt;simpleType&gt;
 *                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
 *                       &lt;enumeration value="entry"/&gt;
 *                       &lt;enumeration value="mem"/&gt;
 *                     &lt;/restriction&gt;
 *                   &lt;/simpleType&gt;
 *                 &lt;/attribute&gt;
 *                 &lt;attribute name="capacity" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *                 &lt;attribute name="overflow-directory" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *               &lt;/restriction&gt;
 *             &lt;/complexContent&gt;
 *           &lt;/complexType&gt;
 *         &lt;/element&gt;
 *         &lt;element name="custom-load-probe" minOccurs="0"&gt;
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
 *       &lt;/sequence&gt;
 *       &lt;attribute name="bind-address" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="hostname-for-clients" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="max-connections" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="maximum-message-count" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="maximum-time-between-pings" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="max-threads" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="message-time-to-live" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="notify-by-subscription" type="{http://www.w3.org/2001/XMLSchema}boolean" /&gt;
 *       &lt;attribute name="port" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *       &lt;attribute name="load-poll-interval" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "server-type", namespace = "http://geode.apache.org/schema/cache",
    propOrder = {"groups", "clientSubscription", "customLoadProbe"})
@XmlSeeAlso({CacheConfig.CacheServer.class})
@Experimental
public class ServerType {

  @XmlElement(name = "group", namespace = "http://geode.apache.org/schema/cache")
  protected List<String> groups;
  @XmlElement(name = "client-subscription", namespace = "http://geode.apache.org/schema/cache")
  protected ServerType.ClientSubscription clientSubscription;
  @XmlElement(name = "custom-load-probe", namespace = "http://geode.apache.org/schema/cache")
  protected DeclarableType customLoadProbe;
  @XmlAttribute(name = "bind-address")
  protected String bindAddress;
  @XmlAttribute(name = "hostname-for-clients")
  protected String hostnameForClients;
  @XmlAttribute(name = "max-connections")
  protected String maxConnections;
  @XmlAttribute(name = "maximum-message-count")
  protected String maximumMessageCount;
  @XmlAttribute(name = "maximum-time-between-pings")
  protected String maximumTimeBetweenPings;
  @XmlAttribute(name = "max-threads")
  protected String maxThreads;
  @XmlAttribute(name = "message-time-to-live")
  protected String messageTimeToLive;
  @XmlAttribute(name = "notify-by-subscription")
  protected Boolean notifyBySubscription;
  @XmlAttribute(name = "port")
  protected String port;
  @XmlAttribute(name = "socket-buffer-size")
  protected String socketBufferSize;
  @XmlAttribute(name = "load-poll-interval")
  protected String loadPollInterval;

  /**
   * Gets the value of the group property.
   *
   * <p>
   * This accessor method returns a reference to the live list,
   * not a snapshot. Therefore any modification you make to the
   * returned list will be present inside the JAXB object.
   * This is why there is not a <CODE>set</CODE> method for the group property.
   *
   * <p>
   * For example, to add a new item, do as follows:
   *
   * <pre>
   * getGroups().add(newItem);
   * </pre>
   *
   *
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link String }
   *
   * @return the {@link List} of groups.
   */
  public List<String> getGroups() {
    if (groups == null) {
      groups = new ArrayList<>();
    }
    return groups;
  }

  /**
   * Gets the value of the clientSubscription property.
   *
   * possible object is
   * {@link ServerType.ClientSubscription }
   *
   * @return the client subscription.
   */
  public ServerType.ClientSubscription getClientSubscription() {
    return clientSubscription;
  }

  /**
   * Sets the value of the clientSubscription property.
   *
   * allowed object is
   * {@link ServerType.ClientSubscription }
   *
   * @param value the client subscription.
   */
  public void setClientSubscription(ServerType.ClientSubscription value) {
    clientSubscription = value;
  }

  /**
   * Gets the value of the customLoadProbe property.
   *
   * @return the custom {@link LoadProbe} type.
   */
  public DeclarableType getCustomLoadProbe() {
    return customLoadProbe;
  }

  /**
   * Sets the value of the customLoadProbe property.
   *
   * @param value the custom {@link LoadProbe} type.
   */
  public void setCustomLoadProbe(DeclarableType value) {
    customLoadProbe = value;
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
   * Gets the value of the hostnameForClients property.
   *
   * possible object is
   * {@link String }
   *
   * @return the hostname for clients.
   */
  public String getHostnameForClients() {
    return hostnameForClients;
  }

  /**
   * Sets the value of the hostnameForClients property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the host name for clients.
   */
  public void setHostnameForClients(String value) {
    hostnameForClients = value;
  }

  /**
   * Gets the value of the maxConnections property.
   *
   * possible object is
   * {@link String }
   *
   * @return the maximum number of connections.
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
   * @param value the maximum number of connections.
   */
  public void setMaxConnections(String value) {
    maxConnections = value;
  }

  /**
   * Gets the value of the maximumMessageCount property.
   *
   * possible object is
   * {@link String }
   *
   * @return the maximum message count.
   */
  public String getMaximumMessageCount() {
    return maximumMessageCount;
  }

  /**
   * Sets the value of the maximumMessageCount property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the maximum message count.
   */
  public void setMaximumMessageCount(String value) {
    maximumMessageCount = value;
  }

  /**
   * Gets the value of the maximumTimeBetweenPings property.
   *
   * possible object is
   * {@link String }
   *
   * @return the maximum tim between pings.
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
   * Gets the value of the maxThreads property.
   *
   * possible object is
   * {@link String }
   *
   * @return the maximum number of threads.
   */
  public String getMaxThreads() {
    return maxThreads;
  }

  /**
   * Sets the value of the maxThreads property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the maximum number of threads.
   */
  public void setMaxThreads(String value) {
    maxThreads = value;
  }

  /**
   * Gets the value of the messageTimeToLive property.
   *
   * possible object is
   * {@link String }
   *
   * @return the message time to live.
   */
  public String getMessageTimeToLive() {
    return messageTimeToLive;
  }

  /**
   * Sets the value of the messageTimeToLive property.
   *
   * allowed object is
   * {@link String }
   *
   *
   * @param value the message time to live.
   */
  public void setMessageTimeToLive(String value) {
    messageTimeToLive = value;
  }

  /**
   * Gets the value of the notifyBySubscription property.
   *
   * possible object is
   * {@link Boolean }
   *
   * @return true is notify by subscription is enabled, false otherwise.
   */
  public Boolean isNotifyBySubscription() {
    return notifyBySubscription;
  }

  /**
   * Sets the value of the notifyBySubscription property.
   *
   * allowed object is
   * {@link Boolean }
   *
   * @param value enables or disables notify by subscription.
   */
  public void setNotifyBySubscription(Boolean value) {
    notifyBySubscription = value;
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
   * Gets the value of the loadPollInterval property.
   *
   * possible object is
   * {@link String }
   *
   * @return the load polling interval.
   */
  public String getLoadPollInterval() {
    return loadPollInterval;
  }

  /**
   * Sets the value of the loadPollInterval property.
   *
   * allowed object is
   * {@link String }
   *
   * @param value the load polling interval.
   */
  public void setLoadPollInterval(String value) {
    loadPollInterval = value;
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
   *       &lt;attribute name="eviction-policy" use="required"&gt;
   *         &lt;simpleType&gt;
   *           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
   *             &lt;enumeration value="entry"/&gt;
   *             &lt;enumeration value="mem"/&gt;
   *           &lt;/restriction&gt;
   *         &lt;/simpleType&gt;
   *       &lt;/attribute&gt;
   *       &lt;attribute name="capacity" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
   *       &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
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
  public static class ClientSubscription {

    @XmlAttribute(name = "eviction-policy", required = true)
    protected String evictionPolicy;
    @XmlAttribute(name = "capacity", required = true)
    protected String capacity;
    @XmlAttribute(name = "disk-store-name")
    protected String diskStoreName;
    @XmlAttribute(name = "overflow-directory")
    protected String overflowDirectory;

    /**
     * Gets the value of the evictionPolicy property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the evictionPolicy property
     */
    public String getEvictionPolicy() {
      return evictionPolicy;
    }

    /**
     * Sets the value of the evictionPolicy property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the value of the evictionPolicy property
     */
    public void setEvictionPolicy(String value) {
      evictionPolicy = value;
    }

    /**
     * Gets the value of the capacity property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the capacity property
     */
    public String getCapacity() {
      return capacity;
    }

    /**
     * Sets the value of the capacity property.
     *
     * allowed object is
     * {@link String }
     *
     * @param value the value of the capacity property
     */
    public void setCapacity(String value) {
      capacity = value;
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
     * Gets the value of the overflowDirectory property.
     *
     * possible object is
     * {@link String }
     *
     * @return the value of the overflowDirectory property
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
     * @param value the value of the overflowDirectory property
     *
     */
    public void setOverflowDirectory(String value) {
      overflowDirectory = value;
    }

  }
}
