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
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

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
@XmlType(name = "", propOrder = {"gatewayTransportFilters"})
@Experimental
public class GatewayReceiverConfig implements Serializable {
  @XmlElement(name = "gateway-transport-filter",
      namespace = "http://geode.apache.org/schema/cache")
  protected List<DeclarableType> gatewayTransportFilters;
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
   *
   */
  public List<DeclarableType> getGatewayTransportFilters() {
    if (gatewayTransportFilters == null) {
      gatewayTransportFilters = new ArrayList<>();
    }
    return gatewayTransportFilters;
  }

  /**
   * Gets the value of the startPort property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getStartPort() {
    return startPort;
  }

  /**
   * Sets the value of the startPort property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setStartPort(String value) {
    startPort = value;
  }

  /**
   * Gets the value of the endPort property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getEndPort() {
    return endPort;
  }

  /**
   * Sets the value of the endPort property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setEndPort(String value) {
    endPort = value;
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
    bindAddress = value;
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
    maximumTimeBetweenPings = value;
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
   * Gets the value of the hostnameForSenders property.
   *
   * possible object is
   * {@link String }
   *
   */
  public String getHostnameForSenders() {
    return hostnameForSenders;
  }

  /**
   * Sets the value of the hostnameForSenders property.
   *
   * allowed object is
   * {@link String }
   *
   */
  public void setHostnameForSenders(String value) {
    hostnameForSenders = value;
  }

  /**
   * Gets the value of the manualStart property.
   *
   * possible object is
   * {@link Boolean }
   *
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
   */
  public void setManualStart(Boolean value) {
    manualStart = value;
  }
}
