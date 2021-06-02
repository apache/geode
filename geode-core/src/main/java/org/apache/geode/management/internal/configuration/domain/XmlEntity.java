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

package org.apache.geode.management.internal.configuration.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.VersionedDataSerializable;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.geode.management.internal.configuration.utils.XmlUtils.XPathContext;

/**
 * Domain class for defining a GemFire entity in XML.
 */
public class XmlEntity implements VersionedDataSerializable {
  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogService.getLogger();

  private transient volatile CacheProvider cacheProvider;

  private String type;

  @SuppressWarnings("unused")
  private String parentType;

  private Map<String, String> attributes = new HashMap<>();

  private String xmlDefinition;

  private String searchString;

  private String prefix = CacheXml.PREFIX;

  private String namespace = CacheXml.GEODE_NAMESPACE;

  private String childPrefix;

  private String childNamespace;

  /**
   * Produce a new XmlEntityBuilder.
   *
   * @return new XmlEntityBuilder.
   * @since GemFire 8.1
   */
  public static XmlEntityBuilder builder() {
    return new XmlEntityBuilder();
  }

  private static CacheProvider createDefaultCacheProvider() {
    return () -> ((InternalCache) CacheFactory.getAnyInstance())
        .getCacheForProcessingClientRequests();
  }

  /**
   * Default constructor for serialization only.
   *
   * @deprecated Use {@link XmlEntity#builder()}.
   */
  @Deprecated
  public XmlEntity() {
    cacheProvider = createDefaultCacheProvider();
  }

  /**
   * Construct a new XmlEntity while creating XML from the cache using the element which has a type
   * and attribute matching those given.
   *
   * @param type Type of the XML element to search for. Should be one of the constants from the
   *        {@link CacheXml} class. For example, CacheXml.REGION.
   * @param key Key of the attribute to match, for example, "name" or "id".
   * @param value Value of the attribute to match.
   */
  public XmlEntity(final String type, final String key, final String value) {
    cacheProvider = createDefaultCacheProvider();
    this.type = type;
    attributes.put(key, value);

    init();
  }

  /**
   * Construct a new XmlEntity while creating Xml from the cache using the element which has
   * attributes matching those given
   *
   * @param parentType Parent type of the XML element to search for. Should be one of the constants
   *        from the {@link CacheXml} class. For example, CacheXml.REGION.
   *
   * @param parentKey Identifier for the parent elements such "name/id"
   * @param parentValue Value of the identifier
   * @param childType Child type of the XML element to search for within the parent . Should be one
   *        of the constants from the {@link CacheXml} class. For example, CacheXml.INDEX.
   * @param childKey Identifier for the child element such as "name/id"
   * @param childValue Value of the child element identifier
   */
  public XmlEntity(final String parentType, final String parentKey, final String parentValue,
      final String childType, final String childKey, final String childValue) {
    cacheProvider = createDefaultCacheProvider();
    this.parentType = parentType;
    type = childType;
    initializeSearchString(parentKey, parentValue, prefix, childKey, childValue);
  }

  /**
   * Construct a new XmlEntity while creating Xml from the cache using the element which has
   * attributes matching those given
   *
   * @param parentType Parent type of the XML element to search for. Should be one of the constants
   *        from the {@link CacheXml} class. For example, CacheXml.REGION.
   *
   * @param parentKey Identifier for the parent elements such "name/id"
   * @param parentValue Value of the identifier
   * @param childPrefix Namespace prefix for the child element such as "lucene"
   * @param childNamespace Namespace for the child element such as
   *        "http://geode.apache.org/schema/lucene"
   * @param childType Child type of the XML element to search for within the parent . Should be one
   *        of the constants from the {@link CacheXml} class. For example, CacheXml.INDEX.
   * @param childKey Identifier for the child element such as "name/id"
   * @param childValue Value of the child element identifier
   */
  public XmlEntity(final String parentType, final String parentKey, final String parentValue,
      final String childPrefix, final String childNamespace, final String childType,
      final String childKey, final String childValue) {
    // Note: Do not invoke init
    cacheProvider = createDefaultCacheProvider();
    this.parentType = parentType;
    type = childType;
    this.childPrefix = childPrefix;
    this.childNamespace = childNamespace;
    initializeSearchString(parentKey, parentValue, childPrefix, childKey, childValue);
  }

  public XmlEntity(final CacheProvider cacheProvider, final String parentType,
      final String childPrefix, final String childNamespace, final String childType,
      final String key, final String value) {
    this.cacheProvider = cacheProvider;
    this.parentType = parentType;
    type = childType;
    prefix = childPrefix;
    namespace = childNamespace;
    this.childPrefix = childPrefix;
    this.childNamespace = childNamespace;
    attributes.put(key, value);

    searchString = "//" + this.parentType + '/' + childPrefix + ':' + type;
    xmlDefinition = parseXmlForDefinition();
  }

  private String parseXmlForDefinition() {
    final Cache cache = cacheProvider.getCache();

    final StringWriter stringWriter = new StringWriter();
    final PrintWriter printWriter = new PrintWriter(stringWriter);
    CacheXmlGenerator.generate(cache, printWriter, false, false);
    printWriter.close();
    InputSource inputSource = new InputSource(new StringReader(stringWriter.toString()));

    try {
      Document document = XmlUtils.getDocumentBuilder().parse(inputSource);
      Node element = document.getElementsByTagNameNS(childNamespace, type).item(0);
      if (null != element) {
        return XmlUtils.elementToString(element);
      }
    } catch (IOException | ParserConfigurationException | RuntimeException | SAXException
        | TransformerException e) {
      throw new InternalGemFireError("Could not parse XML when creating XMLEntity", e);
    }

    logger.warn("No XML definition could be found with name={} and attributes={}", type,
        attributes);
    return null;
  }

  private void initializeSearchString(final String parentKey, final String parentValue,
      final String childPrefix, final String childKey, final String childValue) {
    StringBuilder sb = new StringBuilder();
    sb.append("//").append(prefix).append(':').append(parentType);

    if (StringUtils.isNotBlank(parentKey) && StringUtils.isNotBlank(parentValue)) {
      sb.append("[@").append(parentKey).append("='").append(parentValue).append("']");
    }

    sb.append('/').append(childPrefix).append(':').append(type);

    if (StringUtils.isNotBlank(childKey) && StringUtils.isNotBlank(childValue)) {
      sb.append("[@").append(childKey).append("='").append(childValue).append("']");
    }
    searchString = sb.toString();
  }

  /**
   * Initialize new instances. Called from {@link #XmlEntity(String, String, String)} and
   * {@link XmlEntityBuilder#build()}.
   *
   * @since GemFire 8.1
   */
  private void init() {
    Assert.assertTrue(StringUtils.isNotBlank(type));
    Assert.assertTrue(StringUtils.isNotBlank(prefix));
    Assert.assertTrue(StringUtils.isNotBlank(namespace));
    Assert.assertTrue(attributes != null);

    if (null == xmlDefinition) {
      xmlDefinition = loadXmlDefinition();
    }
  }

  /**
   * Use the CacheXmlGenerator to create XML from the entity associated with the current cache.
   *
   * @return XML string representation of the entity.
   */
  private String loadXmlDefinition() {
    final Cache cache = cacheProvider.getCache();

    final StringWriter stringWriter = new StringWriter();
    final PrintWriter printWriter = new PrintWriter(stringWriter);
    CacheXmlGenerator.generate(cache, printWriter, false, false);
    printWriter.close();

    return loadXmlDefinition(stringWriter.toString());
  }

  /**
   * Used supplied xmlDocument to extract the XML for the defined XmlEntity.
   *
   * @param xmlDocument to extract XML from.
   * @return XML for XmlEntity if found, otherwise {@code null}.
   * @since GemFire 8.1
   */
  private String loadXmlDefinition(final String xmlDocument) {
    try {
      InputSource inputSource = new InputSource(new StringReader(xmlDocument));
      return loadXmlDefinition(XmlUtils.getDocumentBuilder().parse(inputSource));
    } catch (IOException | SAXException | ParserConfigurationException | XPathExpressionException
        | TransformerFactoryConfigurationError | TransformerException e) {
      throw new InternalGemFireError("Could not parse XML when creating XMLEntity", e);
    }
  }

  /**
   * Used supplied XML {@link Document} to extract the XML for the defined XmlEntity.
   *
   * @param document to extract XML from.
   * @return XML for XmlEntity if found, otherwise {@code null}.
   * @since GemFire 8.1
   */
  private String loadXmlDefinition(final Document document)
      throws XPathExpressionException, TransformerFactoryConfigurationError, TransformerException {
    searchString = createQueryString(prefix, type, attributes);
    logger.info("XmlEntity:searchString: {}", searchString);

    if (document != null) {
      XPathContext xpathContext = new XPathContext();
      xpathContext.addNamespace(prefix, namespace);

      // TODO: wrap this line with conditional
      xpathContext.addNamespace(childPrefix, childNamespace);

      // Create an XPathContext here
      Node element = XmlUtils.querySingleElement(document, searchString, xpathContext);

      // Must copy to preserve namespaces.
      if (null != element) {
        return XmlUtils.elementToString(element);
      }
    }

    logger.warn("No XML definition could be found with name={} and attributes={}", type,
        attributes);
    return null;
  }

  /**
   * Create an XmlPath query string from the given element name and attributes.
   *
   * @param element Name of the XML element to search for.
   * @param attributes Attributes of the element that should match, for example "name" or "id" and
   *        the value they should equal. This list may be empty.
   *
   * @return An XmlPath query string.
   */
  private String createQueryString(final String prefix, final String element,
      final Map<String, String> attributes) {
    StringBuilder queryStringBuilder = new StringBuilder();
    Iterator<Entry<String, String>> attributeIter = attributes.entrySet().iterator();
    queryStringBuilder.append("//").append(prefix).append(':').append(element);

    if (!attributes.isEmpty()) {
      queryStringBuilder.append('[');
      Entry<String, String> attrEntry = attributeIter.next();
      queryStringBuilder.append('@').append(attrEntry.getKey()).append("='")
          .append(attrEntry.getValue()).append('\'');
      while (attributeIter.hasNext()) {
        attrEntry = attributeIter.next();
        queryStringBuilder.append(" and @").append(attrEntry.getKey()).append("='")
            .append(attrEntry.getValue()).append('\'');
      }

      queryStringBuilder.append(']');
    }

    return queryStringBuilder.toString();
  }

  public String getSearchString() {
    return searchString;
  }

  public String getType() {
    return type;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  /**
   * Return the value of a single attribute.
   *
   * @param key Key of the attribute whose while will be returned.
   *
   * @return The value of the attribute.
   */
  public String getAttribute(String key) {
    return attributes.get(key);
  }

  /**
   * A convenience method to get a name or id attributes from the list of attributes if one of them
   * has been set. Name takes precedence.
   *
   * @return The name or id attribute or null if neither is found.
   */
  public String getNameOrId() {
    if (attributes.containsKey("name")) {
      return attributes.get("name");
    }

    return attributes.get("id");
  }

  public String getXmlDefinition() {
    return xmlDefinition;
  }

  /**
   * Gets the namespace for the element. Defaults to {@link CacheXml#GEODE_NAMESPACE} if not set.
   *
   * @return XML element namespace
   * @since GemFire 8.1
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * Gets the prefix for the element. Defaults to {@link CacheXml#PREFIX} if not set.
   *
   * @return XML element prefix
   * @since GemFire 8.1
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * Gets the prefix for the child element.
   *
   * @return XML element prefix for the child element
   */
  public String getChildPrefix() {
    return childPrefix;
  }

  /**
   * Gets the namespace for the child element.
   *
   * @return XML element namespace for the child element
   */
  public String getChildNamespace() {
    return childNamespace;
  }

  @Override
  public Version[] getSerializationVersions() {
    return new Version[] {Version.GEODE_1_1_1};
  }

  @Override
  public String toString() {
    return "XmlEntity [namespace=" + namespace + ", type=" + type + ", attributes="
        + attributes + ", xmlDefinition=" + xmlDefinition + ']';
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    XmlEntity other = (XmlEntity) obj;
    if (attributes == null) {
      if (other.attributes != null) {
        return false;
      }
    } else if (!attributes.equals(other.attributes)) {
      return false;
    }
    if (namespace == null) {
      if (other.namespace != null) {
        return false;
      }
    } else if (!namespace.equals(other.namespace)) {
      return false;
    }
    if (type == null) {
      return other.type == null;
    } else {
      return type.equals(other.type);
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    toDataPre_GEODE_1_1_1_0(out);
    DataSerializer.writeString(childPrefix, out);
    DataSerializer.writeString(childNamespace, out);
  }

  public void toDataPre_GEODE_1_1_1_0(DataOutput out) throws IOException {
    DataSerializer.writeString(type, out);
    DataSerializer.writeObject(attributes, out);
    DataSerializer.writeString(xmlDefinition, out);
    DataSerializer.writeString(searchString, out);
    DataSerializer.writeString(prefix, out);
    DataSerializer.writeString(namespace, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    fromDataPre_GEODE_1_1_1_0(in);
    childPrefix = DataSerializer.readString(in);
    childNamespace = DataSerializer.readString(in);
  }

  public void fromDataPre_GEODE_1_1_1_0(DataInput in) throws IOException, ClassNotFoundException {
    type = DataSerializer.readString(in);
    attributes = DataSerializer.readObject(in);
    xmlDefinition = DataSerializer.readString(in);
    searchString = DataSerializer.readString(in);
    prefix = DataSerializer.readString(in);
    namespace = DataSerializer.readString(in);
    cacheProvider = createDefaultCacheProvider();
  }

  /**
   * Defines how XmlEntity gets a reference to the Cache.
   */
  public interface CacheProvider {
    InternalCache getCache();
  }

  /**
   * Builder for XmlEntity. Default values are as described in XmlEntity.
   *
   * @since GemFire 8.1
   */
  public static class XmlEntityBuilder {

    private XmlEntity xmlEntity;

    /**
     * Private constructor.
     *
     * @since GemFire 8.1
     */
    @SuppressWarnings("deprecation")
    XmlEntityBuilder() {
      xmlEntity = new XmlEntity();
    }

    /**
     * Produce an XmlEntity with the supplied values. Builder is reset after #build() is called.
     * Subsequent calls will produce a new XmlEntity.
     *
     * You are required to at least call {@link #withType(String)}.
     *
     * @since GemFire 8.1
     */
    @SuppressWarnings("deprecation")
    public XmlEntity build() {
      xmlEntity.init();

      final XmlEntity built = xmlEntity;
      xmlEntity = new XmlEntity();

      return built;
    }

    /**
     * Sets the type or element name value as returned by {@link XmlEntity#getType()}
     *
     * @param type Name of element type.
     * @return this XmlEntityBuilder
     * @since GemFire 8.1
     */
    public XmlEntityBuilder withType(final String type) {
      xmlEntity.type = type;

      return this;
    }

    /**
     * Sets the element prefix and namespace as returned by {@link XmlEntity#getPrefix()} and
     * {@link XmlEntity#getNamespace()} respectively. Defaults are {@link CacheXml#PREFIX} and
     * {@link CacheXml#GEODE_NAMESPACE} respectively.
     *
     * @param prefix Prefix of element
     * @param namespace Namespace of element
     * @return this XmlEntityBuilder
     * @since GemFire 8.1
     */
    public XmlEntityBuilder withNamespace(final String prefix, final String namespace) {
      xmlEntity.prefix = prefix;
      xmlEntity.namespace = namespace;

      return this;
    }

    /**
     * Adds an attribute for the given <code>name</code> and <code>value</code> to the attributes
     * map returned by {@link XmlEntity#getAttributes()} or {@link XmlEntity#getAttribute(String)}.
     *
     * @param name Name of attribute to set.
     * @param value Value of attribute to set.
     * @return this XmlEntityBuilder
     * @since GemFire 8.1
     */
    public XmlEntityBuilder withAttribute(final String name, final String value) {
      xmlEntity.attributes.put(name, value);

      return this;
    }

    /**
     * Replaces all attributes with the supplied attributes {@link Map}.
     *
     * @param attributes {@link Map} to use.
     * @return this XmlEntityBuilder
     * @since GemFire 8.1
     */
    public XmlEntityBuilder withAttributes(final Map<String, String> attributes) {
      xmlEntity.attributes = attributes;

      return this;
    }

    /**
     * Sets a config xml document source to get the entity XML Definition from as returned by
     * {@link XmlEntity#getXmlDefinition()}. Defaults to current active configuration for
     * {@link Cache}.
     *
     * <b>Should only be used for testing.</b>
     *
     * @param document Config XML {@link Document}.
     * @return this XmlEntityBuilder
     * @since GemFire 8.1
     */
    public XmlEntityBuilder withConfig(final Document document) throws XPathExpressionException,
        TransformerFactoryConfigurationError, TransformerException {
      xmlEntity.xmlDefinition = xmlEntity.loadXmlDefinition(document);

      return this;
    }
  }
}
