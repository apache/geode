/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: JChannelFactory.java,v 1.4 2005/07/17 11:38:05 chrislott Exp $

package com.gemstone.org.jgroups;

import org.w3c.dom.Element;

import java.io.File;

import java.net.URL;

import com.gemstone.org.jgroups.conf.ConfiguratorFactory;
import com.gemstone.org.jgroups.conf.ProtocolStackConfigurator;

/**
 * JChannelFactory creates pure Java implementations of the <code>Channel</code>
 * interface.
 * See {@link JChannel} for a discussion of channel properties.
 */
public class JChannelFactory implements ChannelFactory {
    private ProtocolStackConfigurator _configuration;

    /**
     * Constructs a <code>JChannelFactory</code> instance that contains no
     * protocol stack configuration.
     *
     * @deprecated This constructor should only be used in conjunction with the
     *             deprecated <code>getChannel(Object)</code> method of this
     *             class.
     */
    @Deprecated
    public JChannelFactory() {
    }

    /**
     * Constructs a <code>JChannelFactory</code> instance that utilizes the
     * specified file for protocl stack configuration.
     *
     * @param properties a file containing a JGroups XML protocol stack
     *                   configuration.
     *
     * @throws ChannelException if problems occur during the interpretation of
     *                          the protocol stack configuration.
     */
    public JChannelFactory(File properties) throws ChannelException {
        _configuration=ConfiguratorFactory.getStackConfigurator(properties);
    }

    /**
     * Constructs a <code>JChannelFactory</code> instance that utilizes the
     * specified file for protocl stack configuration.
     *
     * @param properties a XML element containing a JGroups XML protocol stack
     *                   configuration.
     *
     * @throws ChannelException if problems occur during the interpretation of
     *                          the protocol stack configuration.
     */
    public JChannelFactory(Element properties) throws ChannelException {
        _configuration =ConfiguratorFactory.getStackConfigurator(properties);
    }

    /**
     * Constructs a <code>JChannelFactory</code> instance that utilizes the
     * specified file for protocl stack configuration.
     *
     * @param properties a URL pointing to a JGroups XML protocol stack
     *                   configuration.
     *
     * @throws ChannelException if problems occur during the interpretation of
     *                          the protocol stack configuration.
     */
    public JChannelFactory(URL properties) throws ChannelException {
        _configuration=ConfiguratorFactory.getStackConfigurator(properties);
    }

    /**
     * Constructs a <code>JChannel</code> instance with the protocol stack
     * configuration based upon the specified properties parameter.
     *
     * @param properties an old style property string, a string representing a
     *                   system resource containing a JGroups XML configuration,
     *                   a string representing a URL pointing to a JGroups XML
     *                   XML configuration, or a string representing a file name
     *                   that contains a JGroups XML configuration.
     *
     * @throws ChannelException if problems occur during the interpretation of
     *                          the protocol stack configuration.
     */
    public JChannelFactory(String properties) throws ChannelException {
        _configuration=ConfiguratorFactory.getStackConfigurator(properties);
    }

    /**
     * Creates a <code>JChannel</code> implementation of the
     * <code>Channel</code> interface.
     *
     * @param properties the protocol stack configuration information; a
     *                   <code>null</code> value means use the default protocol
     *                   stack configuration.
     *
     * @throws ChannelException if the creation of the channel failed.
     *
     * @deprecated <code>JChannel</code>'s conversion to type-specific
     *             construction, and the subsequent deprecation of its
     *             <code>JChannel(Object)</code> constructor, necessitate the
     *             deprecation of this factory method as well.  Type-specific
     *             protocol stack configuration should be specfied during
     *             construction of an instance of this factory.
     */
    @Deprecated // GemStoneAddition
    public Channel createChannel(Object properties) throws ChannelException {
        return new JChannel(properties);
    }

    /**
     * Creates a <code>JChannel</code> implementation of the
     * <code>Channel<code> interface using the protocol stack configuration
     * information specfied during construction of an instance of this factory.
     *
     * @throws ChannelException if the creation of the channel failed.
     */
     public Channel createChannel() throws ChannelException {
         return new JChannel(_configuration);
     }
}
