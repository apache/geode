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
package com.gemstone.gemfire.management.internal.configuration.utils;

import static com.gemstone.gemfire.management.internal.configuration.utils.XmlConstants.W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION;
import static com.gemstone.gemfire.management.internal.configuration.utils.XmlConstants.W3C_XML_SCHEMA_INSTANCE_PREFIX;
import static javax.xml.XMLConstants.NULL_NS_URI;
import static javax.xml.XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
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

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlParser;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.internal.configuration.domain.CacheElement;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;

public class XmlUtils {

  /**
   * Create an XML {@link Document} from the given {@link Reader}.
   * 
   * @param reader
   *          to create document from.
   * @return {@link Document} if successful, otherwise false.
   * @throws ParserConfigurationException 
   * @throws SAXException 
   * @throws IOException 
   * @since GemFire 8.1
   */
  public static Document createDocumentFromReader(final Reader reader) throws SAXException, ParserConfigurationException, IOException {
    Document doc = null;
    InputSource inputSource = new InputSource(reader);
   
    doc = getDocumentBuilder().parse(inputSource);
    
    return doc;
  }
  
  public static NodeList query(Node node, String searchString) throws XPathExpressionException {
    XPath xpath = XPathFactory.newInstance().newXPath();
    return (NodeList) xpath.evaluate(searchString, node, XPathConstants.NODESET);
  }
  
  public static NodeList query(Node node, String searchString, XPathContext xpathcontext) throws XPathExpressionException {
    XPath xpath = XPathFactory.newInstance().newXPath();
    xpath.setNamespaceContext(xpathcontext);
    return (NodeList) xpath.evaluate(searchString, node, XPathConstants.NODESET);
  }

  public static Element querySingleElement(Node node, String searchString, final XPathContext xPathContext) throws XPathExpressionException {
    XPath xpath = XPathFactory.newInstance().newXPath();
    xpath.setNamespaceContext(xPathContext);
    Object result = xpath.evaluate(searchString, node, XPathConstants.NODE);
      try {
      return (Element) result;
    } catch(ClassCastException e) {
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
   * @param doc Target document where the node will added
   * @param xmlEntity contains definition of the xml entity
   * @throws IOException
   * @throws ParserConfigurationException 
   * @throws SAXException 
   * @throws XPathExpressionException 
   */
  public static void addNewNode(final Document doc, final XmlEntity xmlEntity) throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
    // Build up map per call to avoid issues with caching wrong version of the map.
    final LinkedHashMap<String, CacheElement> elementOrderMap = CacheElement.buildElementMap(doc);
    
    final Node newNode = createNode(doc, xmlEntity.getXmlDefinition());
    final Node root = doc.getDocumentElement();
    final int incomingElementOrder = getElementOrder(elementOrderMap, xmlEntity.getNamespace(), xmlEntity.getType());

    boolean nodeAdded = false;
    NodeList nodes = root.getChildNodes();
    final int length = nodes.getLength();
    for (int i=0; i < length; i++) {
      final Node node = nodes.item(i);
      
      if (node instanceof Element) {
        final Element childElement = (Element) node;
        final String type = childElement.getLocalName();
        final String namespace = childElement.getNamespaceURI();
        
        if (namespace.equals(xmlEntity.getNamespace()) 
            && type.equals(xmlEntity.getType())) {
          // TODO this should really be checking all attributes in xmlEntity.getAttributes
          //First check if the element has a name
          String nameOrId = getAttribute(childElement, "name");
          //If not then check if the element has an Id
          if (nameOrId == null) {
            nameOrId = getAttribute(childElement, "id");
          } 
          
          if (nameOrId != null) {
            //If there is a match , then replace the existing node with the incoming node
            if (nameOrId.equals(xmlEntity.getNameOrId())) {
              root.replaceChild(newNode, node);
              nodeAdded = true;
              break;
            }
          } else {
            //This element does not have any name or id identifier for e.g PDX and gateway-receiver
            //If there is only one element of that type then replace it with the incoming node
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
   * @param elementOrderMap
   * @param namespace
   * @param type
   * @return <code>true</code> if element allows multiple, otherwise
   *         <code>false</code>.
   * @since GemFire 8.1
   */
  private static boolean isMultiple(final LinkedHashMap<String, CacheElement> elementOrderMap, final String namespace, final String type) {
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
   * @param elementOrderMap
   * @param namespace
   * @param type
   * @return position of the element if in map, otherwise
   *         {@link Integer#MAX_VALUE}.
   * @since GemFire 8.1
   */
  private static int getElementOrder(final LinkedHashMap<String, CacheElement> elementOrderMap, final String namespace, final String type) {
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
   * @param owner 
   * @param xmlDefintion
   * @return Node representing the xml definition 
   * @throws ParserConfigurationException 
   * @throws IOException 
   * @throws SAXException 
   */
  private static Node createNode(Document owner, String xmlDefintion) throws SAXException, IOException, ParserConfigurationException  {
    InputSource inputSource = new InputSource(new StringReader(xmlDefintion));
    Document document = getDocumentBuilder().parse(inputSource);
    Node newNode = document.getDocumentElement();
    return owner.importNode(newNode, true);
  }
  
  public static String getAttribute(Node node, String name) {
    NamedNodeMap attributes = node.getAttributes();
    if(attributes == null) {
      return null;
    }
    
    Node attributeNode = node.getAttributes().getNamedItem(name);
    if(attributeNode == null) {
      return null;
    }
    return attributeNode.getTextContent();
  }
  
  public static String getAttribute(Node node, String localName, String namespaceURI) {
    Node attributeNode = node.getAttributes().getNamedItemNS(namespaceURI, localName);
    if(attributeNode == null) {
      return null;
    }
    return attributeNode.getTextContent();
  }
  
  /**
   * Build schema location map of schemas used in given
   * <code>schemaLocationAttribute</code>.
   * 
   * @see <a href="http://www.w3.org/TR/xmlschema-0/#schemaLocation">XML Schema
   *      Part 0: Primer Second Edition | 5.6 schemaLocation</a>
   * 
   * @param schemaLocation
   *          attribute value to build schema location map from.
   * @return {@link Map} of schema namespace URIs to location URLs.
   * @since GemFire 8.1
   */
  public static final Map<String, List<String>> buildSchemaLocationMap(final String schemaLocation) {
    return buildSchemaLocationMap(new HashMap<String, List<String>>(), schemaLocation);
  }
  
  /**
  * Build schema location map of schemas used in given
  * <code>schemaLocationAttribute</code> and adds them to the given
  * <code>schemaLocationMap</code>.
  * 
  * @see <a href="http://www.w3.org/TR/xmlschema-0/#schemaLocation">XML Schema
  *      Part 0: Primer Second Edition | 5.6 schemaLocation</a>
  * 
  * @param schemaLocationMap
  *          {@link Map} to add schema locations to.
  * @param schemaLocation
  *          attribute value to build schema location map from.
  * @return {@link Map} of schema namespace URIs to location URLs.
  * @since GemFire 8.1
  */
 static final Map<String, List<String>> buildSchemaLocationMap(Map<String, List<String>> schemaLocationMap, final String schemaLocation) {
   if (null == schemaLocation) {
     return schemaLocationMap;
   }
   
   if (null == schemaLocation || schemaLocation.isEmpty()) {
     // should really ever be null but being safe.
     return schemaLocationMap;
   }

   final StringTokenizer st = new StringTokenizer(schemaLocation, " \n\t\r");
   while (st.hasMoreElements()) {
     final String ns = st.nextToken();
     final String loc = st.nextToken();
     List<String> locs = schemaLocationMap.get(ns);
     if (null == locs) {
       locs = new ArrayList<>();
       schemaLocationMap.put(ns, locs);
     }
     if (!locs.contains(loc)) {
       locs.add(loc);
     }
   }

   return schemaLocationMap;
 }
 
 /*****
  * Deletes all the node from the document which match the definition provided by xmlentity
  * @param doc 
  * @param xmlEntity
  * @throws Exception
  */
 public static void deleteNode(Document doc , XmlEntity xmlEntity) throws Exception {
   NodeList nodes = getNodes(doc, xmlEntity);
   if (nodes != null) {
     int length = nodes.getLength();
     
     for (int i=0; i<length; i++) {
       Node node = nodes.item(i);
       node.getParentNode().removeChild(node);
     }
   } 
 }
 
 /****
  * Gets all the nodes matching the definition given by the xml entity
  * @param doc
  * @param xmlEntity
  * @return Nodes 
 * @throws XPathExpressionException 
  */
 public static NodeList getNodes(Document doc, XmlEntity xmlEntity) throws XPathExpressionException {
   return query(doc, xmlEntity.getSearchString(), new XPathContext(xmlEntity.getPrefix(), xmlEntity.getNamespace()));
 }
  
  /**
   * An object used by an XPath query that maps namespaces to uris.
   * 
   * @see NamespaceContext
   *
   */
  public static class XPathContext implements NamespaceContext {
    private HashMap<String, String> prefixToUri = new HashMap<String, String>();
    private HashMap<String, String> uriToPrefix = new HashMap<String, String>();
    

    public XPathContext() {
    }
    
    public XPathContext(String prefix, String uri) {
      addNamespace(prefix, uri);
    }

    public void addNamespace(String prefix, String uri) {
      this.prefixToUri.put(prefix, uri);
      this.uriToPrefix.put(uri, prefix);
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
   * @param doc
   * @return pretty xml string
   * @throws IOException
   * @throws TransformerException 
   * @throws TransformerFactoryConfigurationError 
   */
  public static String prettyXml(Node doc) throws IOException, TransformerFactoryConfigurationError, TransformerException {
    Transformer transformer = TransformerFactory.newInstance().newTransformer();
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");

    return transform(transformer, doc);
  }
  
  public static final String elementToString(Node element) throws TransformerFactoryConfigurationError, TransformerException {
    Transformer transformer = TransformerFactory.newInstance().newTransformer();

    return transform(transformer, element);
  }
  
  private static final String transform(Transformer transformer, Node element) throws TransformerException {
    StreamResult result = new StreamResult(new StringWriter());
    DOMSource source = new DOMSource(element);
    transformer.transform(source, result);

    String xmlString = result.getWriter().toString();
    return xmlString;
  }
  
  /****
   * Convert the xmlString to pretty well formatted xmlString
   * @param xmlContent
   * @return pretty xml string
   * @throws IOException
   * @throws TransformerException 
   * @throws TransformerFactoryConfigurationError 
   * @throws ParserConfigurationException 
   * @throws SAXException 
   */
  public static String prettyXml(String xmlContent) throws IOException, TransformerFactoryConfigurationError, TransformerException, SAXException, ParserConfigurationException {
    Document doc = createDocumentFromXml(xmlContent);
    return prettyXml(doc);
  }
  
  /***
   * Create a document from the xml 
   * @param xmlContent
   * @return Document 
   * @throws IOException 
   * @throws ParserConfigurationException 
   * @throws SAXException 
   */
  public static Document createDocumentFromXml(String xmlContent) throws SAXException, ParserConfigurationException, IOException {
    return createDocumentFromReader(new StringReader(xmlContent));
  }
  
  /**
   * Upgrade the schema of a given Config XMl <code>document</code> to the given
   * <code>namespace</code>, <code>schemaLocation</code> and
   * <code>version</code>.
   * 
   * @param document
   *          Config XML {@link Document} to upgrade.
   * @param namespaceUri
   *          Namespace URI to upgrade to.
   * @param schemaLocation
   *          Schema location to upgrade to.
   * @throws XPathExpressionException 
   * @throws ParserConfigurationException 
   * @since GemFire 8.1
   */
  // UnitTest SharedConfigurationTest.testCreateAndUpgradeDocumentFromXml()
  public static Document upgradeSchema(Document document, final String namespaceUri, final String schemaLocation, String schemaVersion) throws XPathExpressionException, ParserConfigurationException {
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
      //doc.setDocType(null);
      Node root = document.getDocumentElement();

      Document copiedDocument = getDocumentBuilder().newDocument();
      Node copiedRoot = copiedDocument.importNode(root, true);
      copiedDocument.appendChild(copiedRoot);

      document = copiedDocument;
    }
    
    final Element root = document.getDocumentElement();
    
    final Map<String, String> namespacePrefixMap = buildNamespacePrefixMap(root);
    
    // Add CacheXml namespace if missing.
    String cachePrefix = namespacePrefixMap.get(namespaceUri);
    if (null == cachePrefix) {
      // Default to null prefix.
      cachePrefix = NULL_NS_URI;
      // Move all into new namespace
      changeNamespace(root, NULL_NS_URI, namespaceUri);      
      namespacePrefixMap.put(namespaceUri, cachePrefix);
    }
    
    // Add schema instance namespace if missing.
    String xsiPrefix = namespacePrefixMap.get(W3C_XML_SCHEMA_INSTANCE_NS_URI);
    if (null == xsiPrefix) {
      xsiPrefix = W3C_XML_SCHEMA_INSTANCE_PREFIX;
      root.setAttribute("xmlns:" + xsiPrefix, W3C_XML_SCHEMA_INSTANCE_NS_URI);
      namespacePrefixMap.put(W3C_XML_SCHEMA_INSTANCE_NS_URI, xsiPrefix);
    }
    
    // Create schemaLocation attribute if missing.
    final String schemaLocationAttribute = getAttribute(root, W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION, W3C_XML_SCHEMA_INSTANCE_NS_URI);
    
    // Update schemaLocation for namespace.
    final Map<String, List<String>> schemaLocationMap = buildSchemaLocationMap(schemaLocationAttribute);
    List<String> schemaLocations = schemaLocationMap.get(namespaceUri);
    if (null == schemaLocations) {
      schemaLocations = new ArrayList<String>();
      schemaLocationMap.put(namespaceUri, schemaLocations);
    }
    schemaLocations.clear();
    schemaLocations.add(schemaLocation);
    String schemaLocationValue = getSchemaLocationValue(schemaLocationMap);
    root.setAttributeNS(W3C_XML_SCHEMA_INSTANCE_NS_URI, xsiPrefix + ":" + W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION, schemaLocationValue);

    // Set schema version
    if(cachePrefix== null || cachePrefix.isEmpty()) {
      root.setAttribute("version", schemaVersion);
    } else {
      root.setAttributeNS(namespaceUri, cachePrefix + ":version", schemaVersion);
    }
        
    return document;
  }
  
  
  /**
   * Set the <code>schemaLocationAttribute</code> to the values of the
   * <code>schemaLocationMap</code>.
   * 
   * @see <a href="http://www.w3.org/TR/xmlschema-0/#schemaLocation">XML Schema
   *      Part 0: Primer Second Edition | 5.6 schemaLocation</a>
   * 
   * @param schemaLocationMap
   *          {@link Map} to get schema locations from.
   * @since GemFire 8.1
   */
  private static final String getSchemaLocationValue(final Map<String, List<String>> schemaLocationMap) {
    final StringBuilder sb = new StringBuilder();
    for (final Map.Entry<String, List<String>> entry : schemaLocationMap.entrySet()) {
      for (final String schemaLocation : entry.getValue()) {
        if (sb.length() > 0) {
          sb.append(' ');
        }
        sb.append(entry.getKey()).append(' ').append(schemaLocation);
      }
    }
    return sb.toString();
  }
  
  /**
   * Build {@link Map} of namespace URIs to prefixes.
   * 
   * @param root
   *          {@link Element} to get namespaces and prefixes from.
   * @return {@link Map} of namespace URIs to prefixes.
   * @since GemFire 8.1
   */
  private static final Map<String, String> buildNamespacePrefixMap(final Element root) {
    final HashMap<String, String> namespacePrefixMap = new HashMap<>();

    //Look for all of the attributes of cache that start with
    //xmlns
    NamedNodeMap attributes = root.getAttributes();
    for(int i = 0; i < attributes.getLength(); i++) {
      Node item = attributes.item(i);
      if(item.getNodeName().startsWith("xmlns")) {
        //Anything after the colon is the prefix
        //eg xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        //has a prefix of xsi
        String[] splitName = item.getNodeName().split(":");
        String prefix;
        if(splitName.length > 1) {
          prefix = splitName[1];
        } else {
          prefix = "";
        }
        String uri = item.getTextContent();
        namespacePrefixMap.put(uri, prefix);
      }
    }
    
    return namespacePrefixMap;
  }
  
  /**
   * Change the namespace URI of a <code>node</code> and its children to
   * <code>newNamespaceUri</code> if that node is in the given
   * <code>oldNamespaceUri</code> namespace URI.
   * 
   * 
   * @param node
   *          {@link Node} to change namespace URI on.
   * @param oldNamespaceUri
   *          old namespace URI to change from.
   * @param newNamespaceUri
   *          new Namespace URI to change to.
   * @throws XPathExpressionException
   * @return the modified version of the passed in node. 
   * @since GemFire 8.1
   */
  static final Node changeNamespace(final Node node, final String oldNamespaceUri, final String newNamespaceUri) throws XPathExpressionException {
    Node result = null;
    final NodeList nodes = query(node, "//*");
    for (int i = 0; i < nodes.getLength(); i++) {
      final Node element = nodes.item(i);
      if (element.getNamespaceURI() == null || element.getNamespaceURI().equals(oldNamespaceUri)) {
        Node renamed = node.getOwnerDocument().renameNode(element, newNamespaceUri, element.getNodeName());
        if(element == node) {
          result = renamed;
        }
      }
    }
    return result;
  }
  
  /****
   * Method to modify the root attribute (cache) of the XML 
   * @param doc Target document whose root attributes must be modified
   * @param xmlEntity xml entity for the root , it also contains the attributes
   * @throws IOException
   */
  public static void modifyRootAttributes(Document doc, XmlEntity xmlEntity) throws IOException {
    if (xmlEntity == null || xmlEntity.getAttributes() == null) {
      return;
    }
    String type =  xmlEntity.getType();
    Map<String, String> attributes = xmlEntity.getAttributes();
    
    Element root = doc.getDocumentElement();
    if (root.getLocalName().equals(type)) {
      
      for (Entry<String, String> entry : attributes.entrySet()) {
        String attributeName = entry.getKey();
        String attributeValue = entry.getValue();
        
        //Remove the existing attribute
        String rootAttribute = getAttribute(root, attributeName);
        if (null != rootAttribute) {
          root.removeAttribute(rootAttribute);
        }
        
        //Add the new attribute with new value
        root.setAttribute(attributeName, attributeValue);
      }
    }
  }
  
  /***
   * Reads the xml file as a String
   * @param xmlFilePath
   * @return String containing xml read from the file.
   * @throws IOException
   * @throws ParserConfigurationException 
   * @throws SAXException 
   * @throws TransformerException 
   * @throws TransformerFactoryConfigurationError 
   */
  public static String readXmlAsStringFromFile(String xmlFilePath) throws IOException, SAXException, ParserConfigurationException, TransformerFactoryConfigurationError, TransformerException{
	File file = new File(xmlFilePath);
	//The file can be empty if the only command we have issued for this group is deployJar
	if(file.length() == 0) {
      return "";
	}
	
	Document doc = getDocumentBuilder().parse(file);
	return elementToString(doc);
  }
}
