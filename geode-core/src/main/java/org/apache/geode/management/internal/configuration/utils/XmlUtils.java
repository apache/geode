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
package org.apache.geode.management.internal.configuration.utils;

import static javax.xml.XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI;
import static org.apache.geode.management.internal.configuration.utils.XmlConstants.W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION;
import static org.apache.geode.management.internal.configuration.utils.XmlConstants.W3C_XML_SCHEMA_INSTANCE_PREFIX;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.cache.xmlcache.CacheXmlParser;
import org.apache.geode.management.internal.configuration.domain.CacheElement;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

public class XmlUtils {

  /**
   * Create an XML {@link Document} from the given {@link Reader}.
   *
   * @param reader to create document from.
   * @return {@link Document} if successful, otherwise false.
   * @since GemFire 8.1
   */
  public static Document createDocumentFromReader(final Reader reader)
      throws SAXException, ParserConfigurationException, IOException {
    Document doc;
    InputSource inputSource = new InputSource(reader);

    doc = getDocumentBuilder().parse(inputSource);

    return doc;
  }

  public static NodeList query(Node node, String searchString) throws XPathExpressionException {
    XPath xpath = XPathFactory.newInstance().newXPath();
    return (NodeList) xpath.evaluate(searchString, node, XPathConstants.NODESET);
  }

  public static NodeList query(Node node, String searchString, XPathContext xpathcontext)
      throws XPathExpressionException {
    XPath xpath = XPathFactory.newInstance().newXPath();
    xpath.setNamespaceContext(xpathcontext);
    return (NodeList) xpath.evaluate(searchString, node, XPathConstants.NODESET);
  }

  public static Element querySingleElement(Node node, String searchString,
      final XPathContext xPathContext) throws XPathExpressionException {
    XPath xpath = XPathFactory.newInstance().newXPath();
    xpath.setNamespaceContext(xPathContext);
    Object result = xpath.evaluate(searchString, node, XPathConstants.NODE);
    try {
      return (Element) result;
    } catch (ClassCastException e) {
      throw new XPathExpressionException("Not an org.w3c.dom.Element: " + result);
    }
  }

  public static DocumentBuilder getDocumentBuilder() throws ParserConfigurationException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    // the actual builder or parser
    DocumentBuilder builder = factory.newDocumentBuilder();
    builder.setEntityResolver(new CacheXmlParser());
    return builder;
  }

  /*****
   * Adds a new node or replaces an existing node in the Document
   *
   * @param doc Target document where the node will added
   * @param xmlEntity contains definition of the xml entity
   */
  public static void addNewNode(final Document doc, final XmlEntity xmlEntity)
      throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
    // Build up map per call to avoid issues with caching wrong version of the map.
    final LinkedHashMap<String, CacheElement> elementOrderMap = CacheElement.buildElementMap(doc);

    final Node newNode = createNode(doc, xmlEntity.getXmlDefinition());
    final Node root = doc.getDocumentElement();
    final int incomingElementOrder =
        getElementOrder(elementOrderMap, xmlEntity.getNamespace(), xmlEntity.getType());

    boolean nodeAdded = false;
    NodeList nodes = root.getChildNodes();
    final int length = nodes.getLength();
    for (int i = 0; i < length; i++) {
      final Node node = nodes.item(i);

      if (node instanceof Element) {
        final Element childElement = (Element) node;
        final String type = childElement.getLocalName();
        final String namespace = childElement.getNamespaceURI();

        if (namespace.equals(xmlEntity.getNamespace()) && type.equals(xmlEntity.getType())) {
          // First check if the element has a name
          String nameOrId = getAttribute(childElement, "name");
          // If not then check if the element has an Id
          if (nameOrId == null) {
            nameOrId = getAttribute(childElement, "id");
          }

          if (nameOrId != null) {
            // If there is a match , then replace the existing node with the incoming node
            if (nameOrId.equals(xmlEntity.getNameOrId())) {
              root.replaceChild(newNode, node);
              nodeAdded = true;
              break;
            }
          } else {
            // This element does not have any name or id identifier for e.g PDX and gateway-receiver
            // If there is only one element of that type then replace it with the incoming node
            if (!isMultiple(elementOrderMap, namespace, type)) {
              root.replaceChild(newNode, node);
              nodeAdded = true;
              break;
            }
          }
        } else {
          if (incomingElementOrder < getElementOrder(elementOrderMap, namespace, type)) {
            root.insertBefore(newNode, node);
            nodeAdded = true;
            break;
          }
        }
      }
    }
    if (!nodeAdded) {
      root.appendChild(newNode);
    }
  }

  /**
   * @return <code>true</code> if element allows multiple, otherwise <code>false</code>.
   * @since GemFire 8.1
   */
  private static boolean isMultiple(final LinkedHashMap<String, CacheElement> elementOrderMap,
      final String namespace, final String type) {
    if (CacheXml.GEODE_NAMESPACE.equals(namespace)) {
      // We only keep the cache elements in elementOrderMap
      final CacheElement cacheElement = elementOrderMap.get(type);
      if (null != cacheElement) {
        return cacheElement.isMultiple();
      }
    }
    // Assume all extensions are not multiples.
    // To support multiple on extensions our map needs to included other
    // namespaces
    return false;
  }

  /**
   * @return position of the element if in map, otherwise {@link Integer#MAX_VALUE}.
   * @since GemFire 8.1
   */
  private static int getElementOrder(final LinkedHashMap<String, CacheElement> elementOrderMap,
      final String namespace, final String type) {
    if (CacheXml.GEODE_NAMESPACE.equals(namespace)) {
      // We only keep the cache elements in elementOrderMap
      final CacheElement cacheElement = elementOrderMap.get(type);
      if (null != cacheElement) {

        return cacheElement.getOrder();
      }
    }
    // Assume all extensions are order independent.
    return Integer.MAX_VALUE;
  }

  /****
   * Creates a node from the String xml definition
   *
   * @return Node representing the xml definition
   */
  private static Node createNode(Document owner, String xmlDefinition)
      throws SAXException, IOException, ParserConfigurationException {
    InputSource inputSource = new InputSource(new StringReader(xmlDefinition));
    Document document = getDocumentBuilder().parse(inputSource);
    Node newNode = document.getDocumentElement();
    return owner.importNode(newNode, true);
  }

  public static String getAttribute(Node node, String name) {
    NamedNodeMap attributes = node.getAttributes();
    if (attributes == null) {
      return null;
    }

    Node attributeNode = node.getAttributes().getNamedItem(name);
    if (attributeNode == null) {
      return null;
    }
    return attributeNode.getTextContent();
  }

  public static String getAttribute(Node node, String localName, String namespaceURI) {
    Node attributeNode = node.getAttributes().getNamedItemNS(namespaceURI, localName);
    if (attributeNode == null) {
      return null;
    }
    return attributeNode.getTextContent();
  }

  /**
   * Build schema location map of schemas used in given <code>schemaLocationAttribute</code>.
   *
   * @see <a href="http://www.w3.org/TR/xmlschema-0/#schemaLocation">XML Schema Part 0: Primer
   *      Second Edition | 5.6 schemaLocation</a>
   *
   * @param schemaLocation attribute value to build schema location map from.
   * @return {@link Map} of schema namespace URIs to location URLs.
   * @since GemFire 8.1
   */
  public static Map<String, String> buildSchemaLocationMap(final String schemaLocation) {
    Map<String, String> schemaLocationMap = new HashMap<>();
    if (StringUtils.isBlank(schemaLocation)) {
      return schemaLocationMap;
    }

    final StringTokenizer st = new StringTokenizer(schemaLocation, " \n\t\r");
    while (st.hasMoreElements()) {
      final String ns = st.nextToken();
      final String loc = st.nextToken();
      schemaLocationMap.put(ns, loc);
    }

    return schemaLocationMap;
  }

  /*****
   * Deletes all the node from the document which match the definition provided by xmlEntity
   *
   */
  public static void deleteNode(Document doc, XmlEntity xmlEntity) throws Exception {
    NodeList nodes = getNodes(doc, xmlEntity);
    if (nodes != null) {
      int length = nodes.getLength();

      for (int i = 0; i < length; i++) {
        Node node = nodes.item(i);
        node.getParentNode().removeChild(node);
      }
    }
  }

  /****
   * Gets all the nodes matching the definition given by the xml entity
   *
   */
  public static NodeList getNodes(Document doc, XmlEntity xmlEntity)
      throws XPathExpressionException {
    XPathContext context = new XPathContext();
    context.addNamespace(xmlEntity.getPrefix(), xmlEntity.getNamespace());
    if (xmlEntity.getChildPrefix() != null) {
      context.addNamespace(xmlEntity.getChildPrefix(), xmlEntity.getChildNamespace());
    }
    return query(doc, xmlEntity.getSearchString(), context);
  }

  /**
   * An object used by an XPath query that maps namespaces to uris.
   *
   * @see NamespaceContext
   *
   */
  public static class XPathContext implements NamespaceContext {
    private final HashMap<String, String> prefixToUri = new HashMap<>();
    private final HashMap<String, String> uriToPrefix = new HashMap<>();


    public XPathContext() {}

    public XPathContext(String prefix, String uri) {
      addNamespace(prefix, uri);
    }

    public void addNamespace(String prefix, String uri) {
      prefixToUri.put(prefix, uri);
      uriToPrefix.put(uri, prefix);
    }

    @Override
    public String getNamespaceURI(String prefix) {
      return prefixToUri.get(prefix);
    }

    @Override
    public String getPrefix(String namespaceURI) {
      return uriToPrefix.get(namespaceURI);
    }

    @Override
    public Iterator<String> getPrefixes(String namespaceURI) {
      return Collections.singleton(getPrefix(namespaceURI)).iterator();
    }

  }

  /****
   * Converts the document to a well formatted Xml string
   *
   * @return pretty xml string
   */
  public static String prettyXml(Node doc)
      throws TransformerFactoryConfigurationError, TransformerException {
    Transformer transformer = TransformerFactory.newInstance().newTransformer();
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");

    return transform(transformer, doc);
  }

  public static String elementToString(Node element)
      throws TransformerFactoryConfigurationError, TransformerException {
    Transformer transformer = TransformerFactory.newInstance().newTransformer();

    return transform(transformer, element);
  }

  private static String transform(Transformer transformer, Node element)
      throws TransformerException {
    StreamResult result = new StreamResult(new StringWriter());
    DOMSource source = new DOMSource(element);
    transformer.transform(source, result);

    return result.getWriter().toString();
  }

  /****
   * Convert the xmlString to pretty well formatted xmlString
   *
   * @return pretty xml string
   */
  public static String prettyXml(String xmlContent)
      throws IOException, TransformerFactoryConfigurationError, TransformerException, SAXException,
      ParserConfigurationException {
    Document doc = createDocumentFromXml(xmlContent);
    return prettyXml(doc);
  }

  /***
   * Create a document from the xml
   *
   */
  public static Document createDocumentFromXml(String xmlContent)
      throws SAXException, ParserConfigurationException, IOException {
    return createDocumentFromReader(new StringReader(xmlContent));
  }

  /**
   * Create a {@link Document} using {@link XmlUtils#createDocumentFromXml(String)} and if the
   * version attribute is not equal to the current version then update the XML to the current schema
   * and return the document.
   *
   * @param xmlContent XML content to load and upgrade.
   * @return {@link Document} from xmlContent.
   * @since GemFire 8.1
   */
  public static Document createAndUpgradeDocumentFromXml(String xmlContent)
      throws SAXException, ParserConfigurationException, IOException, XPathExpressionException {
    Document doc = XmlUtils.createDocumentFromXml(xmlContent);
    if (!CacheXml.VERSION_LATEST.equals(XmlUtils.getAttribute(doc.getDocumentElement(),
        CacheXml.VERSION, CacheXml.GEODE_NAMESPACE))) {
      doc = upgradeSchema(doc, CacheXml.GEODE_NAMESPACE, CacheXml.LATEST_SCHEMA_LOCATION,
          CacheXml.VERSION_LATEST);
    }
    return doc;
  }

  /**
   * Upgrade the schema of a given Config XMl <code>document</code> to the given
   * <code>namespace</code>, <code>schemaLocation</code> and <code>version</code>.
   *
   * @param document Config XML {@link Document} to upgrade.
   * @param namespaceUri Namespace URI to upgrade to.
   * @param schemaLocation Schema location to upgrade to.
   * @since GemFire 8.1
   */
  public static Document upgradeSchema(Document document, final String namespaceUri,
      final String schemaLocation, String schemaVersion)
      throws XPathExpressionException, ParserConfigurationException {
    if (StringUtils.isBlank(namespaceUri)) {
      throw new IllegalArgumentException("namespaceUri");
    }
    if (StringUtils.isBlank(schemaLocation)) {
      throw new IllegalArgumentException("schemaLocation");
    }
    if (StringUtils.isBlank(schemaVersion)) {
      throw new IllegalArgumentException("schemaVersion");
    }

    if (null != document.getDoctype()) {
      Node root = document.getDocumentElement();
      Document copiedDocument = getDocumentBuilder().newDocument();
      Node copiedRoot = copiedDocument.importNode(root, true);
      copiedDocument.appendChild(copiedRoot);
      document = copiedDocument;
    }

    final Element root = document.getDocumentElement();
    // since root is the cache element, then this oldNamespace will be the cache's namespaceURI
    String oldNamespaceUri = root.getNamespaceURI();

    // update the namespace
    if (!namespaceUri.equals(oldNamespaceUri)) {
      changeNamespace(root, oldNamespaceUri, namespaceUri);
    }

    // update the version
    root.setAttribute("version", schemaVersion);

    // update the schemaLocation attribute
    Node schemaLocationAttr = root.getAttributeNodeNS(W3C_XML_SCHEMA_INSTANCE_NS_URI,
        W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION);
    String xsiPrefix = findPrefix(root, W3C_XML_SCHEMA_INSTANCE_NS_URI);
    Map<String, String> uriToLocation = new HashMap<>();
    if (schemaLocationAttr != null) {
      uriToLocation = buildSchemaLocationMap(schemaLocationAttr.getNodeValue());
    } else if (xsiPrefix == null) {
      // this namespace is not defined yet, define it
      xsiPrefix = W3C_XML_SCHEMA_INSTANCE_PREFIX;
      root.setAttribute("xmlns:" + xsiPrefix, W3C_XML_SCHEMA_INSTANCE_NS_URI);
    }

    uriToLocation.remove(oldNamespaceUri);
    uriToLocation.put(namespaceUri, schemaLocation);

    root.setAttributeNS(W3C_XML_SCHEMA_INSTANCE_NS_URI,
        xsiPrefix + ":" + W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION,
        getSchemaLocationValue(uriToLocation));

    return document;
  }


  /**
   * Set the <code>schemaLocationAttribute</code> to the values of the
   * <code>schemaLocationMap</code>.
   *
   * @see <a href="http://www.w3.org/TR/xmlschema-0/#schemaLocation">XML Schema Part 0: Primer
   *      Second Edition | 5.6 schemaLocation</a>
   *
   * @param schemaLocationMap {@link Map} to get schema locations from.
   * @since GemFire 8.1
   */
  private static String getSchemaLocationValue(final Map<String, String> schemaLocationMap) {
    final StringBuilder sb = new StringBuilder();
    for (final Map.Entry<String, String> entry : schemaLocationMap.entrySet()) {
      if (sb.length() > 0) {
        sb.append(' ');
      }
      sb.append(entry.getKey()).append(' ').append(entry.getValue());
    }
    return sb.toString();
  }

  static String findPrefix(final Element root, final String namespaceUri) {
    // Look for all of the attributes of cache that start with xmlns
    NamedNodeMap attributes = root.getAttributes();
    for (int i = 0; i < attributes.getLength(); i++) {
      Node item = attributes.item(i);
      if (item.getNodeName().startsWith("xmlns")) {
        if (item.getNodeValue().equals(namespaceUri)) {
          String[] splitName = item.getNodeName().split(":");
          if (splitName.length > 1) {
            return splitName[1];
          } else {
            return "";
          }
        }
      }
    }
    return null;
  }

  /**
   * Change the namespace URI of a <code>node</code> and its children to
   * <code>newNamespaceUri</code> if that node is in the given <code>oldNamespaceUri</code>
   * namespace URI.
   *
   *
   * @param node {@link Node} to change namespace URI on.
   * @param oldNamespaceUri old namespace URI to change from.
   * @param newNamespaceUri new Namespace URI to change to.
   * @return the modified version of the passed in node.
   * @since GemFire 8.1
   */
  static Node changeNamespace(final Node node, final String oldNamespaceUri,
      final String newNamespaceUri) throws XPathExpressionException {
    Node result = null;
    final NodeList nodes = query(node, "//*");
    for (int i = 0; i < nodes.getLength(); i++) {
      final Node element = nodes.item(i);
      if (element.getNamespaceURI() == null || element.getNamespaceURI().equals(oldNamespaceUri)) {
        Node renamed =
            node.getOwnerDocument().renameNode(element, newNamespaceUri, element.getNodeName());
        if (element == node) {
          result = renamed;
        }
      }
    }
    return result;
  }

  /****
   * Method to modify the root attribute (cache) of the XML
   *
   * @param doc Target document whose root attributes must be modified
   * @param xmlEntity xml entity for the root , it also contains the attributes
   */
  public static void modifyRootAttributes(Document doc, XmlEntity xmlEntity) {
    if (xmlEntity == null || xmlEntity.getAttributes() == null) {
      return;
    }
    String type = xmlEntity.getType();
    Map<String, String> attributes = xmlEntity.getAttributes();

    Element root = doc.getDocumentElement();
    if (root.getLocalName().equals(type)) {

      for (Entry<String, String> entry : attributes.entrySet()) {
        String attributeName = entry.getKey();
        String attributeValue = entry.getValue();

        // Remove the existing attribute
        String rootAttribute = getAttribute(root, attributeName);
        if (null != rootAttribute) {
          root.removeAttribute(rootAttribute);
        }

        // Add the new attribute with new value
        root.setAttribute(attributeName, attributeValue);
      }
    }
  }
}
