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
package org.apache.geode.internal.config;

import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.lang3.StringUtils;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import org.apache.geode.cache.configuration.XSDRootElement;
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.internal.lang.SystemProperty;
import org.apache.geode.internal.lang.SystemPropertyHelper;
import org.apache.geode.management.internal.util.ClasspathScanLoadHelper;

public class JAXBService {

  private final Marshaller marshaller;
  private final Unmarshaller unmarshaller;
  private final NameSpaceFilter nameSpaceFilter;

  public JAXBService(Class<?>... xsdRootClasses) {
    try {
      JAXBContext jaxbContext = JAXBContext.newInstance(xsdRootClasses);
      marshaller = jaxbContext.createMarshaller();
      unmarshaller = jaxbContext.createUnmarshaller();
      marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
      marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);

      String schemas = Arrays.stream(xsdRootClasses).map(c -> {
        XSDRootElement element = c.getAnnotation(XSDRootElement.class);
        if (element != null && StringUtils.isNotEmpty(element.namespace())
            && StringUtils.isNotEmpty(element.schemaLocation())) {
          return element.namespace() + " " + element.schemaLocation();
        }
        return null;
      }).filter(Objects::nonNull).collect(Collectors.joining(" "));

      marshaller.setProperty(Marshaller.JAXB_SCHEMA_LOCATION, schemas);
      // use a custom Filter so that we can unmarshall older namespace or no namespace xml
      XMLReader reader = XMLReaderFactory.createXMLReader();
      nameSpaceFilter = new NameSpaceFilter();
      nameSpaceFilter.setParent(reader);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public static JAXBService create(Class<?>... xsdClasses) {
    if (xsdClasses != null && xsdClasses.length > 0) {
      return new JAXBService(xsdClasses);
    }
    // else, scan the classpath to find all the classes annotated with XSDRootElement
    return new JAXBService(scanForClasses().toArray(new Class[0]));
  }

  public static JAXBService createWithValidation(Class<?>... xsdClasses) {
    JAXBService jaxbService = create(xsdClasses);
    jaxbService.validateWithLocalCacheXSD();
    return jaxbService;
  }

  static Set<String> getPackagesToScan() {
    Set<String> packages = new HashSet<>();
    String sysProperty =
        SystemProperty.getProductStringProperty(SystemPropertyHelper.PACKAGES_TO_SCAN).get();
    if (sysProperty != null) {
      packages = Arrays.stream(sysProperty.split(",")).collect(Collectors.toSet());
    } else {
      packages.add("*");
    }
    return packages;
  }

  private static Set<Class<?>> scanForClasses() {
    // scan the classpath to find all the classes annotated with XSDRootElement
    Set<String> packages = getPackagesToScan();
    try (ClasspathScanLoadHelper scanner = new ClasspathScanLoadHelper(packages)) {
      return scanner.scanClasspathForAnnotation(XSDRootElement.class,
          packages.toArray(new String[] {}));
    }
  }

  private void validateWith(URL url) {
    SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    try {
      Schema schema = factory.newSchema(url);
      marshaller.setSchema(schema);
    } catch (SAXException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public void validateWithLocalCacheXSD() {
    // find the local Cache-1.0.xsd
    URL local_cache_xsd = ClassPathLoader.getLatest()
        .getResource("META-INF/schemas/geode.apache.org/schema/cache/cache-1.0.xsd");
    validateWith(local_cache_xsd);
  }

  public String marshall(Object object) {
    StringWriter sw = new StringWriter();
    try {
      sw.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>");
      marshaller.marshal(object, sw);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    return sw.toString();
  }

  public <T> T unMarshall(String xml) {
    try {
      Source source = new SAXSource(nameSpaceFilter, new InputSource(new StringReader(xml)));
      return (T) unmarshaller.unmarshal(source);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public <T> T unMarshall(String xml, Class<T> klass) {
    try {
      Source source = new SAXSource(nameSpaceFilter, new InputSource(new StringReader(xml)));
      return unmarshaller.unmarshal(source, klass).getValue();
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }
}
