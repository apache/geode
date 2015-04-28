/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ConfiguratorFactory.java,v 1.17 2005/08/08 14:58:32 belaban Exp $

package com.gemstone.org.jgroups.conf;

import org.w3c.dom.Element;

import com.gemstone.org.jgroups.util.GemFireTracer;



import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.ChannelException;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import java.security.AccessControlException;

/**
 * The ConfigurationFactory is a factory that returns a protocol stack configurator.
 * The protocol stack configurator is an object that read a stack configuration and
 * parses it so that the ProtocolStack can create a stack.
 * <BR>
 * Currently the factory returns one of the following objects:<BR>
 * 1. XmlConfigurator - parses XML files that are according to the jgroups-protocol.dtd<BR>
 * 2. PlainConfigurator - uses the old style strings UDP:FRAG: etc etc<BR>
 *
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @version 1.0
 */
public class ConfiguratorFactory  {
    public static final String JAXP_MISSING_ERROR_MSG=
            "JAXP Error: the required XML parsing classes are not available; " +
            "make sure that JAXP compatible libraries are in the classpath.";

    static final String FORCE_CONFIGURATION="force.properties";

    static final GemFireTracer log=GemFireTracer.getLog(ConfiguratorFactory.class);

    static String propertiesOverride=null;

    // Check for the presence of the system property "force.properties", and
    // act appropriately if it is set.  We only need to do this once since the
    // system properties are highly unlikely to change.
    static {
        try {
            Properties properties = System.getProperties();
            propertiesOverride = properties.getProperty(FORCE_CONFIGURATION);
        }
        catch (SecurityException e) {
            propertiesOverride = null;
        }

        if(propertiesOverride != null && log.isInfoEnabled()) {
            log.info(ExternalStrings.ConfiguratorFactory_USING_PROPERTIES_OVERRIDE__0, propertiesOverride);
        }
    }

    protected ConfiguratorFactory() {
    }

    /**
     * Returns a protocol stack configurator based on the XML configuration
     * provided by the specified File.
     *
     * @param file a File with a JGroups XML configuration.
     *
     * @return a <code>ProtocolStackConfigurator</code> containing the stack
     *         configuration.
     *
     * @throws ChannelException if problems occur during the configuration of
     *                          the protocol stack.
     */
    public static ProtocolStackConfigurator getStackConfigurator(File file)
    throws ChannelException {
        ProtocolStackConfigurator returnValue;

        if (propertiesOverride != null) {
            returnValue = getStackConfigurator(propertiesOverride);
        }
        else {
            checkForNullConfiguration(file);
            checkJAXPAvailability();

            try {
              FileInputStream fis = new FileInputStream(file); // GemStoneAddition ensure stream closure
              try {
                returnValue=
                  XmlConfigurator.getInstance(fis);
              }
              finally { 
                fis.close();
              }
            }
            catch (IOException ioe) {
                throw createChannelConfigurationException(ioe);
            }
        }

        return returnValue;
    }

    /**
     * Returns a protocol stack configurator based on the XML configuration
     * provided at the specified URL.
     *
     * @param url a URL pointing to a JGroups XML configuration.
     *
     * @return a <code>ProtocolStackConfigurator</code> containing the stack
     *         configuration.
     *
     * @throws ChannelException if problems occur during the configuration of
     *                          the protocol stack.
     */
    public static ProtocolStackConfigurator getStackConfigurator(URL url)
    throws ChannelException {
        ProtocolStackConfigurator returnValue;

        if (propertiesOverride != null) {
            returnValue = getStackConfigurator(propertiesOverride);
        }
        else {
            checkForNullConfiguration(url);
            checkJAXPAvailability();

            try {
                returnValue=XmlConfigurator.getInstance(url);
            }
            catch (IOException ioe) {
                throw createChannelConfigurationException(ioe);
            }
        }

        return returnValue;
    }

    /**
     * Returns a protocol stack configurator based on the XML configuration
     * provided by the specified XML element.
     *
     * @param element a XML element containing a JGroups XML configuration.
     *
     * @return a <code>ProtocolStackConfigurator</code> containing the stack
     *         configuration.
     *
     * @throws ChannelException if problems occur during the configuration of
     *                          the protocol stack.
     */
    public static ProtocolStackConfigurator getStackConfigurator(Element element)
    throws ChannelException {
        ProtocolStackConfigurator returnValue;

        if (propertiesOverride != null) {
            returnValue = getStackConfigurator(propertiesOverride);
        }
        else {
            checkForNullConfiguration(element);

            // Since Element is a part of the JAXP specification and because an
            // Element instance already exists, there is no need to check for
            // JAXP availability.
            //
            // checkJAXPAvailability();

            try {
                returnValue=XmlConfigurator.getInstance(element);
            }
            catch (IOException ioe) {
                throw createChannelConfigurationException(ioe);
            }
        }

        return returnValue;
    }

    /**
     * Returns a protocol stack configurator based on the provided properties
     * string.
     *
     * @param properties an old style property string, a string representing a
     *                   system resource containing a JGroups XML configuration,
     *                   a string representing a URL pointing to a JGroups XML
     *                   XML configuration, or a string representing a file name
     *                   that contains a JGroups XML configuration.
     */
    public static ProtocolStackConfigurator getStackConfigurator(String properties) throws ChannelException {
        if (propertiesOverride != null) {
            properties = propertiesOverride;
        }

        // added by bela: for null String props we use the default properties
        if(properties == null)
            properties=JChannel.DEFAULT_PROTOCOL_STACK;

        checkForNullConfiguration(properties);

        ProtocolStackConfigurator returnValue;

        // Attempt to treat the properties string as a pointer to an XML
        // configuration.
        XmlConfigurator configurator = null;

        try {
            configurator=getXmlConfigurator(properties);
        }
        catch (IOException ioe) {
            throw createChannelConfigurationException(ioe);
        }

        // Did the properties string point to a JGroups XML configuration?
        if (configurator != null) {
            returnValue=configurator;
        }
        else {
            // Attempt to process the properties string as the old style
            // property string.
            returnValue=new PlainConfigurator(properties);
        }

        return returnValue;
    }

    /**
     * Returns a protocol stack configurator based on the properties passed in.<BR>
     * If the properties parameter is a plain string UDP:FRAG:MERGE:GMS etc, a PlainConfigurator is returned.<BR>
     * If the properties parameter is a string that represents a url for example http://www.filip.net/test.xml
     * or the parameter is a java.net.URL object, an XmlConfigurator is returned<BR>
     *
     * @param properties old style property string, url string, or java.net.URL object
     * @return a ProtocolStackConfigurator containing the stack configuration
     * @throws IOException if it fails to parse the XML content
     * @throws IOException if the URL is invalid or a the content can not be reached
     * @deprecated Used by the JChannel(Object) constructor which has been deprecated.
     */
    @Deprecated // GemStoneAddition
    public static ProtocolStackConfigurator getStackConfigurator(Object properties) throws IOException {
        InputStream input=null;

        if (propertiesOverride != null) {
            properties = propertiesOverride;
        }

        // added by bela: for null String props we use the default properties
        if(properties == null)
            properties=JChannel.DEFAULT_PROTOCOL_STACK;

        if(properties instanceof URL) {
            try {
                input=((URL)properties).openStream();
            }
        catch(RuntimeException t) {
            }
        }

        // if it is a string, then it could be a plain string or a url
        if(input == null && properties instanceof String) {
            try {
                input=new URL((String)properties).openStream();
            }
            catch(Exception ignore) {
                // if we get here this means we don't have a URL
            }

            // another try - maybe it is a resource, e.g. default.xml
            if(input == null && ((String)properties).endsWith("xml")) {
                try {
                    input=Util.getResourceAsStream((String)properties, ConfiguratorFactory.class);
                }
                catch(RuntimeException ignore) {
                }
            }

            // try a regular file name
            //
            // This code was moved from the parent block (below) because of the
            // possibility of causing a ClassCastException.

            if(input == null) {
                try {
                    input=new FileInputStream((String)properties);
                }
                catch(RuntimeException t) {
                }
            }
        }

        // try a regular file
        if(input == null && properties instanceof File) {
            try {
                input=new FileInputStream((File)properties);
            }
            catch(RuntimeException t) {
            }
        }

        if(input != null) {
            return XmlConfigurator.getInstance(input);
        }

        if(properties instanceof Element) {
            return XmlConfigurator.getInstance((Element)properties);
        }

        return new PlainConfigurator((String)properties);
    }


    /**
     * Returns a JGroups XML configuration InputStream based on the provided
     * properties string.
     *
     * @param properties a string representing a system resource containing a
     *                   JGroups XML configuration, a string representing a URL
     *                   pointing to a JGroups ML configuration, or a string
     *                   representing a file name that contains a JGroups XML
     *                   configuration.
     *
     * @throws IOException  if the provided properties string appears to be a
     *                      valid URL but is unreachable.
     */
    static InputStream getConfigStream(String properties) throws IOException {
        InputStream configStream = null;

        // Check to see if the properties string is a URL.
        try {
            configStream=new URL(properties).openStream();
        }
        catch (MalformedURLException mre) {
            // the properties string is not a URL
        }
        // Commented so the caller is notified of this condition, but left in
        // the code for documentation purposes.
        //
        // catch (IOException ioe) {
            // the specified URL string was not reachable
        // }

        // Check to see if the properties string is the name of a resource,
        // e.g. default.xml.
        if(configStream == null && properties.endsWith("xml")) {
            configStream=Util.getResourceAsStream(properties, ConfiguratorFactory.class);
        }

        // Check to see if the properties string is the name of a file.
        if (configStream == null) {
            try {
                configStream=new FileInputStream(properties);
            }
            catch(FileNotFoundException fnfe) {
                // the properties string is likely not a file
            }
            catch(AccessControlException access_ex) {
                // fixes http://jira.jboss.com/jira/browse/JGRP-94
            }
        }

        return configStream;
    }

    /**
     * Returns an XmlConfigurator based on the provided properties string (if
     * possible).
     *
     * @param properties a string representing a system resource containing a
     *                   JGroups XML configuration, a string representing a URL
     *                   pointing to a JGroups ML configuration, or a string
     *                   representing a file name that contains a JGroups XML
     *                   configuration.
     *
     * @return an XmlConfigurator instance based on the provided properties
     *         string; <code>null</code> if the provided properties string does
     *         not point to an XML configuration.
     *
     * @throws IOException  if the provided properties string appears to be a
     *                      valid URL but is unreachable, or if the JGroups XML
     *                      configuration pointed to by the URL can not be
     *                      parsed.
     */
    static XmlConfigurator getXmlConfigurator(String properties) throws IOException {
        XmlConfigurator returnValue=null;
        InputStream configStream=getConfigStream(properties);

        if (configStream != null) {
            checkJAXPAvailability();

            returnValue=XmlConfigurator.getInstance(configStream);
        }

        return returnValue;
    }

    /**
     * Creates a <code>ChannelException</code> instance based upon a
     * configuration problem.
     *
     * @param cause the exceptional configuration condition to be used as the
     *              created <code>ChannelException</code>'s cause.
     */
    static ChannelException createChannelConfigurationException(Throwable cause) {
        return new ChannelException("unable to load the protocol stack", cause);
    }

    /**
     * Check to see if the specified configuration properties are
     * <code>null</null> which is not allowed.
     *
     * @param properties the specified protocol stack configuration.
     *
     * @throws NullPointerException if the specified configuration properties
     *                              are <code>null</code>.
     */
    static void checkForNullConfiguration(Object properties) {
        if (properties == null) {
            final String msg =
                "the specifed protocol stack configuration was null.";

            throw new NullPointerException(msg);
        }
    }

    /**
     * Checks the availability of the JAXP classes on the classpath.
     *
     * @throws NoClassDefFoundError if the required JAXP classes are not
     *                              availabile on the classpath.
     */
    static void checkJAXPAvailability() {
        try {
            // TODO: Do some real class checking here instead of forcing the
            //       load of a JGroups class that happens (by default) to do it
            //       for us.
            XmlConfigurator.class.getName();
        }
        catch (NoClassDefFoundError error) {
            throw new NoClassDefFoundError(JAXP_MISSING_ERROR_MSG);
        }
    }
}
