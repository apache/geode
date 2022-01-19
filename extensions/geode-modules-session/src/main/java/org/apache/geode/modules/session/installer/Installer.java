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

package org.apache.geode.modules.session.installer;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.apache.geode.internal.ExitCode;
import org.apache.geode.modules.session.installer.args.Argument;
import org.apache.geode.modules.session.installer.args.ArgumentProcessor;
import org.apache.geode.modules.session.installer.args.ArgumentValues;
import org.apache.geode.modules.session.installer.args.UsageException;

public class Installer {

  private static final String GEMFIRE_FILTER_CLASS =
      "org.apache.geode.modules.session.filter.SessionCachingFilter";

  private ArgumentValues argValues;

  private static final Argument ARG_HELP =
      new Argument("-h", false).setDescription("Displays this help message.");

  private static final Argument ARG_GEMFIRE_PARAMETERS = new Argument("-p", false, "param=value")
      .setDescription("Specific parameter for inclusion into the "
          + "session filter definition as a regular " + "init-param. Can be given multiple times.");

  private static final Argument ARG_CACHE_TYPE = new Argument("-t", false, "cache-type")
      .setDescription("Type of cache. Must be one of 'peer-to-peer' or "
          + "'client-server'. Default is peer-to-peer.")
      .setDefaults("peer-to-peer");

  private static final Argument ARG_WEB_XML_FILE =
      new Argument("-w", true, "web.xml file").setDescription("The web.xml file to be modified.");


  /**
   * Class main method
   *
   * @param args Arguments passed in via the command line
   * @throws Exception in the event of any errors
   */
  public static void main(final String[] args) throws Exception {
    new Installer(args).process();
  }

  private static void log(String message) {
    System.err.println(message);
  }


  public Installer(String[] args) {
    final ArgumentProcessor processor = new ArgumentProcessor("Installer");

    argValues = null;
    try {
      // These are ordered so as to keep the options alphabetical
      processor.addArgument(ARG_HELP);
      processor.addArgument(ARG_GEMFIRE_PARAMETERS);
      processor.addArgument(ARG_CACHE_TYPE);
      processor.addArgument(ARG_WEB_XML_FILE);

      processor.setUnknownArgumentHandler((form, params) -> {
        log("Unknown argument being ignored: " + form + " (" + params.length + " params)");
        log("Use '-h' argument to display usage");
      });
      argValues = processor.process(args);

      if (argValues.isDefined(ARG_HELP)) {
        final UsageException usageException = new UsageException("Usage requested by user");
        usageException.setUsage(processor.getUsage());
        throw (usageException);
      }

    } catch (UsageException ux) {
      final StringBuilder error = new StringBuilder();
      error.append("\nERROR: ");
      error.append(ux.getMessage());
      error.append("\n");
      if (ux.getUsage() != null) {
        error.append(ux.getUsage());
      }
      log(error.toString());
      ExitCode.INSTALL_FAILURE.doSystemExit();
    }

  }


  /**
   * The main entry point for processing
   *
   * @throws Exception if any errors occur.
   */
  private void process() throws Exception {
    String argInputFile = argValues.getFirstResult(ARG_WEB_XML_FILE);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    InputStream input = new FileInputStream(argInputFile);

    processWebXml(input, output);
    input.close();

    System.out.println(output);
  }


  public void processWebXml(final InputStream webXml, final OutputStream out) throws Exception {

    Document doc = createWebXmlDoc(webXml);
    mangleWebXml(doc);

    streamXML(doc, out);
  }


  private Document createWebXmlDoc(final InputStream webXml) throws Exception {
    Document doc;
    final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    final DocumentBuilder builder = factory.newDocumentBuilder();
    doc = builder.parse(webXml);

    return doc;
  }


  private Document mangleWebXml(final Document doc) {
    final Element docElement = doc.getDocumentElement();
    final NodeList nodelist = docElement.getChildNodes();
    Node firstFilter = null;
    Node displayElement = null;
    Node afterDisplayElement = null;

    for (int i = 0; i < nodelist.getLength(); i++) {
      final Node node = nodelist.item(i);
      final String name = node.getNodeName();
      if ("display-name".equals(name)) {
        displayElement = node;
      } else {
        if ("filter".equals(name)) {
          if (firstFilter == null) {
            firstFilter = node;
          }
        }
        if (displayElement != null && afterDisplayElement == null) {
          afterDisplayElement = node;
        }
      }
    }

    Node initParam;
    final Element filter = doc.createElement("filter");
    append(doc, filter, "filter-name", "gemfire-session-filter");
    append(doc, filter, "filter-class", GEMFIRE_FILTER_CLASS);

    // Set the type of cache
    initParam = append(doc, filter, "init-param", null);
    append(doc, initParam, "param-name", "cache-type");
    append(doc, initParam, "param-value", argValues.getFirstResult(ARG_CACHE_TYPE));


    if (argValues.isDefined(ARG_GEMFIRE_PARAMETERS)) {
      for (String[] val : argValues.getAllResults(ARG_GEMFIRE_PARAMETERS)) {
        String gfParam = val[0];
        int idx = gfParam.indexOf("=");
        initParam = append(doc, filter, "init-param", null);
        append(doc, initParam, "param-name", gfParam.substring(0, idx));
        append(doc, initParam, "param-value", gfParam.substring(idx + 1));
      }
    }

    Node first = firstFilter;
    if (first == null) {
      if (afterDisplayElement != null) {
        first = afterDisplayElement;
      }
    }
    if (first == null) {
      first = docElement.getFirstChild();
    }
    docElement.insertBefore(filter, first);
    final Element filterMapping = doc.createElement("filter-mapping");
    append(doc, filterMapping, "filter-name", "gemfire-session-filter");
    append(doc, filterMapping, "url-pattern", "/*");
    docElement.insertBefore(filterMapping, after(docElement, "filter"));
    return doc;
  }

  private Node after(final Node parent, final String nodeName) {
    final NodeList nodelist = parent.getChildNodes();
    int index = -1;
    for (int i = 0; i < nodelist.getLength(); i++) {
      final Node node = nodelist.item(i);
      final String name = node.getNodeName();
      if (nodeName.equals(name)) {
        index = i;
      }
    }
    if (index == -1) {
      return null;
    }
    if (nodelist.getLength() > (index + 1)) {
      return nodelist.item(index + 1);
    }
    return null;
  }

  private Node append(final Document doc, final Node parent, final String element,
      final String value) {
    final Element child = doc.createElement(element);
    if (value != null) {
      child.setTextContent(value);
    }
    parent.appendChild(child);
    return child;
  }

  private void streamXML(final Document doc, final OutputStream out) {
    try {// Use a Transformer for output
      final TransformerFactory tFactory = TransformerFactory.newInstance();
      final Transformer transformer = tFactory.newTransformer();
      if (doc.getDoctype() != null) {
        final String systemId = doc.getDoctype().getSystemId();
        final String publicId = doc.getDoctype().getPublicId();
        transformer.setOutputProperty(OutputKeys.DOCTYPE_PUBLIC, publicId);
        transformer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM, systemId);
      }
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
      final DOMSource source = new DOMSource(doc);
      final StreamResult result = new StreamResult(out);
      transformer.transform(source, result);
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

}
