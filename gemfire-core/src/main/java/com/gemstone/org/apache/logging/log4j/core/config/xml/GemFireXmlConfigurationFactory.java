/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package com.gemstone.org.apache.logging.log4j.core.config.xml;

import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Order;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;

/**
 * Factory to construct an XmlConfiguration.
 */
@Plugin(name = "GemFireXmlConfigurationFactory", category = "ConfigurationFactory")
@Order(1)
public class GemFireXmlConfigurationFactory extends ConfigurationFactory {

    static {
        //System.out.println("KIRK: loading GemFireXmlConfigurationFactory");
    }
  
    /**
     * Valid file extensions for XML files.
     */
    public static final String[] SUFFIXES = new String[] {".xml", "*"};

    /**
     * Returns the Configuration.
     * @param source The InputSource.
     * @return The Configuration.
     */
    @Override
    public Configuration getConfiguration(final ConfigurationSource source) {
        return new XmlConfiguration(source);
    }

    /**
     * Returns the file suffixes for XML files.
     * @return An array of File extensions.
     */
    @Override
    public String[] getSupportedTypes() {
        return SUFFIXES;
    }
}
