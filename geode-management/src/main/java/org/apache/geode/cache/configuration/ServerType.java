
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

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlType;

import org.apache.geode.annotations.Experimental;


/**
 * <p>
 * Java class for server-type complex type.
 *
 * <p>
 * The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="server-type">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="group" type="{http://www.w3.org/2001/XMLSchema}string" maxOccurs="unbounded" minOccurs="0"/>
 *         &lt;element name="client-subscription" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;attribute name="eviction-policy" use="required">
 *                   &lt;simpleType>
 *                     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *                       &lt;enumeration value="entry"/>
 *                       &lt;enumeration value="mem"/>
 *                     &lt;/restriction>
 *                   &lt;/simpleType>
 *                 &lt;/attribute>
 *                 &lt;attribute name="capacity" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" />
 *                 &lt;attribute name="overflow-directory" type="{http://www.w3.org/2001/XMLSchema}string" />
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="custom-load-probe" minOccurs="0">
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
 *       &lt;/sequence>
 *       &lt;attribute name="bind-address" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="hostname-for-clients" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="max-connections" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="maximum-message-count" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="maximum-time-between-pings" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="max-threads" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="message-time-to-live" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="notify-by-subscription" type="{http://www.w3.org/2001/XMLSchema}boolean" />
 *       &lt;attribute name="port" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="socket-buffer-size" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="load-poll-interval" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
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
   *
   */
  public List<String> getGroups() {
    if (groups == null) {
      groups = new ArrayList<String>();
    }
    return this.groups;
  }

  /**
   * Gets the value of the clientSubscription property.
   *
   * possible object is
   * {@link ServerType.ClientSubscription }
   *
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
   */
  public void setClientSubscription(ServerType.ClientSubscription value) {
    this.clientSubscription = value;
  }

  /**
   * Gets the value of the customLoadProbe property.
   *
   */
  public DeclarableType getCustomLoadProbe() {
    return customLoadProbe;
  }

  /**
   * Sets the value of the customLoadProbe property.
   *
   */
  public void setCustomLoadProbe(DeclarableType value) {
    this.customLoadProbe = value;
  }

  /**
   * Gets the value of the bindAddress property.
   *
   * possible object is
   * {@link String }
   *
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
   */
  public void setBindAddress(String value) {
    this.bindAddress = value;
  }

  /**
   * Gets the value of the hostnameForClients property.
   *
   * possible object is
   * {@link String }
   *
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
   */
  public void setHostnameForClients(String value) {
    this.hostnameForClients = value;
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
    this.maxConnections = value;
  }

  /**
   * Gets the value of the maximumMessageCount property.
   *
   * possible object is
   * {@link String }
   *
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
   */
  public void setMaximumMessageCount(String value) {
    this.maximumMessageCount = value;
  }

  /**
   * Gets the value of the maximumTimeBetweenPings property.
   *
   * possible object is
   * {@link String }
   *
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
   */
  public void setMaximumTimeBetweenPings(String value) {
    this.maximumTimeBetweenPings = value;
  }

  /**
   * Gets the value of the maxThreads property.
   *
   * possible object is
   * {@link String }
   *
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
   */
  public void setMaxThreads(String value) {
    this.maxThreads = value;
  }

  /**
   * Gets the value of the messageTimeToLive property.
   *
   * possible object is
   * {@link String }
   *
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
   */
  public void setMessageTimeToLive(String value) {
    this.messageTimeToLive = value;
  }

  /**
   * Gets the value of the notifyBySubscription property.
   *
   * possible object is
   * {@link Boolean }
   *
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
   */
  public void setNotifyBySubscription(Boolean value) {
    this.notifyBySubscription = value;
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
    this.port = value;
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
    this.socketBufferSize = value;
  }

  /**
   * Gets the value of the loadPollInterval property.
   *
   * possible object is
   * {@link String }
   *
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
   */
  public void setLoadPollInterval(String value) {
    this.loadPollInterval = value;
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
   *       &lt;attribute name="eviction-policy" use="required">
   *         &lt;simpleType>
   *           &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
   *             &lt;enumeration value="entry"/>
   *             &lt;enumeration value="mem"/>
   *           &lt;/restriction>
   *         &lt;/simpleType>
   *       &lt;/attribute>
   *       &lt;attribute name="capacity" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
   *       &lt;attribute name="disk-store-name" type="{http://www.w3.org/2001/XMLSchema}string" />
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
     */
    public void setEvictionPolicy(String value) {
      this.evictionPolicy = value;
    }

    /**
     * Gets the value of the capacity property.
     *
     * possible object is
     * {@link String }
     *
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
     */
    public void setCapacity(String value) {
      this.capacity = value;
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
     * Gets the value of the overflowDirectory property.
     *
     * possible object is
     * {@link String }
     *
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
     */
    public void setOverflowDirectory(String value) {
      this.overflowDirectory = value;
    }

  }
}
