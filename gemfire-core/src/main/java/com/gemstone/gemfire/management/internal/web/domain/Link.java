/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.web.domain;

import java.io.Serializable;
import java.net.URI;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

import com.gemstone.gemfire.internal.lang.ObjectUtils;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.internal.web.http.HttpMethod;
import com.gemstone.gemfire.management.internal.web.util.UriUtils;

/**
 * The Link class models hypermedia controls/link relations.
 * <p/>
 * @author John Blum
 * @see java.lang.Comparable
 * @see java.io.Serializable
 * @see java.net.URI
 * @see javax.xml.bind.annotation.XmlAttribute
 * @see javax.xml.bind.annotation.XmlType
 * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex
 * @see com.gemstone.gemfire.management.internal.web.util.UriUtils
 * @see com.gemstone.gemfire.management.internal.web.http.HttpMethod
 * @since 8.0
 */
@SuppressWarnings("unused")
@XmlType(name = "link", propOrder = { "method", "href", "relation" })
public class Link implements Comparable<Link>, Serializable {

  protected static final HttpMethod DEFAULT_HTTP_METHOD = HttpMethod.GET;

  protected static final String HREF_ATTRIBUTE_NAME = "href";
  protected static final String LINK_ELEMENT_NAME = "link";
  protected static final String METHOD_ATTRIBUTE_NAME = "method";
  protected static final String RELATION_ATTRIBUTE_NAME = "rel";

  // This enum type is used in place of Spring's org.springframework.http.HttpMethod enum due to classpath issues
  // between the GemFire Locator/Manager and the embedded HTTP service using Tomcat with Java/Tomcat's class resolution
  // delegation model.
  private HttpMethod method;

  private String relation;

  private URI href;

  public Link() {
  }

  public Link(final String relation, final URI href) {
    this(relation, href, DEFAULT_HTTP_METHOD);
  }

  public Link(final String relation, final URI href, final HttpMethod method) {
    setRelation(relation);
    setHref(href);
    setMethod(method);
  }

  @XmlAttribute(name = HREF_ATTRIBUTE_NAME)
  public URI getHref() {
    return href;
  }

  public final void setHref(final URI href) {
    assert href != null : "The Link href URI cannot be null!";
    this.href = href;
  }

  @XmlAttribute(name = METHOD_ATTRIBUTE_NAME, required = false)
  public HttpMethod getMethod() {
    return method;
  }

  public final void setMethod(final HttpMethod method) {
    this.method = ObjectUtils.defaultIfNull(method, DEFAULT_HTTP_METHOD);
  }

  @XmlAttribute(name = RELATION_ATTRIBUTE_NAME)
  public String getRelation() {
    return relation;
  }

  public final void setRelation(final String relation) {
    assert !StringUtils.isBlank(relation) : "The Link relation (rel) must be specified!";
    this.relation = relation;
  }

  @Override
  public int compareTo(final Link link) {
    int compareValue = getRelation().compareTo(link.getRelation());
    compareValue = (compareValue != 0 ? compareValue : getHref().compareTo(link.getHref()));
    return (compareValue != 0 ? compareValue : getMethod().compareTo(link.getMethod()));
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }

    if (!(obj instanceof Link)) {
      return false;
    }

    final Link that = (Link) obj;

    return ObjectUtils.equals(getHref(), that.getHref())
      && ObjectUtils.equals(getMethod(), that.getMethod());
  }

  @Override
  public int hashCode() {
    int hashValue = 17;
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getHref());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getMethod());
    return hashValue;
  }

  /**
   * The HTTP Request-Line begins with a method token, followed by the Request-URI and the protocol version, and ending
   * with CRLF.  However, this method just returns a String representation similar to the HTTP Request-Line based on
   * values of the Link's properties, which only includes method and request URI.
   * <p/>
   * @return a String representation of the HTTP request-line.
   * @see java.net.URI
   * @see com.gemstone.gemfire.management.internal.web.http.HttpMethod
   * @see <a href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html">http://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html</a>
   */
  public String toHttpRequestLine() {
    return getMethod().name().concat(StringUtils.SPACE).concat(UriUtils.decode(getHref().toString()));
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("{ class = ").append(getClass().getName());
    buffer.append(", rel = ").append(getRelation());
    buffer.append(", href = ").append(getHref());
    buffer.append(", method = ").append(getMethod());
    buffer.append(" }");
    return buffer.toString();
  }

}
