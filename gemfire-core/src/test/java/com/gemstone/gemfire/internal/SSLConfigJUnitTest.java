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
package com.gemstone.gemfire.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import junit.framework.AssertionFailedError;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test that DistributionConfigImpl handles SSL options correctly.
 * 
 */
@Category(IntegrationTest.class)
public class SSLConfigJUnitTest {

  private static final Properties SSL_PROPS_MAP     = new Properties();
  private static final Properties CLUSTER_SSL_PROPS_MAP     = new Properties();
  private static final Properties CLUSTER_SSL_PROPS_SUBSET_MAP     = new Properties();
  private static final Properties JMX_SSL_PROPS_MAP = new Properties();
  private static final Properties JMX_SSL_PROPS_SUBSET_MAP = new Properties();
  private static final Properties SERVER_SSL_PROPS_MAP = new Properties();
  private static final Properties SERVER_PROPS_SUBSET_MAP = new Properties();
  private static final Properties GATEWAY_SSL_PROPS_MAP = new Properties();
  private static final Properties GATEWAY_PROPS_SUBSET_MAP = new Properties();
  

  static {
    
    SSL_PROPS_MAP.put("javax.net.ssl.keyStoreType", "jks");
    SSL_PROPS_MAP.put("javax.net.ssl.keyStore", "/export/gemfire-configs/gemfire.keystore");
    SSL_PROPS_MAP.put("javax.net.ssl.keyStorePassword", "gemfire-key-password");
    SSL_PROPS_MAP.put("javax.net.ssl.trustStore", "/export/gemfire-configs/gemfire.truststore");
    SSL_PROPS_MAP.put("javax.net.ssl.trustStorePassword", "gemfire-trust-password");
    
    // SSL Properties for GemFire in-cluster connections
    CLUSTER_SSL_PROPS_MAP.put("cluster-ssl-keystore-type", "jks");
    CLUSTER_SSL_PROPS_MAP.put("cluster-ssl-keystore", "/export/gemfire-configs/gemfire.keystore");
    CLUSTER_SSL_PROPS_MAP.put("cluster-ssl-keystore-password", "gemfire-key-password");
    CLUSTER_SSL_PROPS_MAP.put("cluster-ssl-truststore", "/export/gemfire-configs/gemfire.truststore");
    CLUSTER_SSL_PROPS_MAP.put("cluster-ssl-truststore-password", "gemfire-trust-password");

     // Partially over-ridden SSL Properties for cluster
    CLUSTER_SSL_PROPS_SUBSET_MAP.put("cluster-ssl-keystore", "/export/gemfire-configs/gemfire.keystore");
    CLUSTER_SSL_PROPS_SUBSET_MAP.put("cluster-ssl-truststore", "/export/gemfire-configs/gemfire.truststore");
    
    // SSL Properties for GemFire JMX Manager connections
    JMX_SSL_PROPS_MAP.put("jmx-manager-ssl-keystore-type", "jks");
    JMX_SSL_PROPS_MAP.put("jmx-manager-ssl-keystore", "/export/gemfire-configs/manager.keystore");
    JMX_SSL_PROPS_MAP.put("jmx-manager-ssl-keystore-password", "manager-key-password");
    JMX_SSL_PROPS_MAP.put("jmx-manager-ssl-truststore", "/export/gemfire-configs/manager.truststore");
    JMX_SSL_PROPS_MAP.put("jmx-manager-ssl-truststore-password", "manager-trust-password");
    
    // SSL Properties for GemFire CacheServer connections
    SERVER_SSL_PROPS_MAP.put("server-ssl-keystore-type", "jks");
    SERVER_SSL_PROPS_MAP.put("server-ssl-keystore", "/export/gemfire-configs/cacheserver.keystore");
    SERVER_SSL_PROPS_MAP.put("server-ssl-keystore-password", "cacheserver-key-password");
    SERVER_SSL_PROPS_MAP.put("server-ssl-truststore", "/export/gemfire-configs/cacheserver.truststore");
    SERVER_SSL_PROPS_MAP.put("server-ssl-truststore-password", "cacheserver-trust-password");
    
   // SSL Properties for GemFire gateway connections
    GATEWAY_SSL_PROPS_MAP.put("gateway-ssl-keystore-type", "jks");
    GATEWAY_SSL_PROPS_MAP.put("gateway-ssl-keystore", "/export/gemfire-configs/gateway.keystore");
    GATEWAY_SSL_PROPS_MAP.put("gateway-ssl-keystore-password", "gateway-key-password");
    GATEWAY_SSL_PROPS_MAP.put("gateway-ssl-truststore", "/export/gemfire-configs/gateway.truststore");
    GATEWAY_SSL_PROPS_MAP.put("gateway-ssl-truststore-password", "gateway-trust-password");

    // Partially over-ridden SSL Properties for GemFire JMX Manager connections
    JMX_SSL_PROPS_SUBSET_MAP.put("jmx-manager-ssl-keystore", "/export/gemfire-configs/manager.keystore");
    JMX_SSL_PROPS_SUBSET_MAP.put("jmx-manager-ssl-truststore", "/export/gemfire-configs/manager.truststore");
    
    // Partially over-ridden SSL Properties for GemFire JMX Manager connections
    SERVER_PROPS_SUBSET_MAP.put("server-ssl-keystore", "/export/gemfire-configs/cacheserver.keystore");
    SERVER_PROPS_SUBSET_MAP.put("server-ssl-truststore", "/export/gemfire-configs/cacheserver.truststore");
    
    // Partially over-ridden SSL Properties for GemFire JMX Manager connections
    GATEWAY_PROPS_SUBSET_MAP.put("gateway-ssl-keystore", "/export/gemfire-configs/gateway.keystore");
    GATEWAY_PROPS_SUBSET_MAP.put("gateway-ssl-truststore", "/export/gemfire-configs/gateway.truststore");

  }
  
  //----- test methods ------

  @Test
  public void testMCastPortWithSSL() throws Exception {
    Properties props = new Properties( );
    // default mcast-port is not 0.
    props.setProperty( "ssl-enabled", "true" );
    
    try {
      new DistributionConfigImpl( props );
    } catch ( IllegalArgumentException e ) {
      if (! e.toString().matches( ".*Could not set \"ssl-enabled.*" ) ) {
        throw new Exception( "did not get expected exception, got this instead...", e );
      }
    }
    
    props.setProperty( "mcast-port", "0" );
    new DistributionConfigImpl( props );
  }
  
  @Test
  public void testMCastPortWithClusterSSL() throws Exception {
    Properties props = new Properties( );
    // default mcast-port is not 0.
    props.setProperty( "cluster-ssl-enabled", "true" );
    
    try {
      new DistributionConfigImpl( props );
    } catch ( IllegalArgumentException e ) {
      if (! e.toString().matches( ".*Could not set \"cluster-ssl-enabled.*" ) ) {
        throw new Exception( "did not get expected exception, got this instead...", e );
      }
    }
    
    props.setProperty( "mcast-port", "0" );
    new DistributionConfigImpl( props );
  }
  
  @Test
  public void testConfigCopyWithSSL( ) throws Exception {
    boolean sslenabled = false;
    String sslprotocols = "any";
    String sslciphers = "any";
    boolean requireAuth = true;
    
    DistributionConfigImpl config = new DistributionConfigImpl( new Properties() );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );
    
    Properties props = new Properties();
    sslciphers = "RSA_WITH_GARBAGE";
    props.setProperty("ssl-ciphers", sslciphers );

    config = new DistributionConfigImpl( props );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );
    
    sslprotocols = "SSLv7";
    props.setProperty("ssl-protocols", sslprotocols );

    config = new DistributionConfigImpl( props );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );

    requireAuth = false;
    props.setProperty("ssl-require-authentication", String.valueOf( requireAuth ) );

    config = new DistributionConfigImpl( props );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );

    sslenabled = true;
    props.setProperty("ssl-enabled", String.valueOf( sslenabled ) );
    props.setProperty("mcast-port", "0" );

    config = new DistributionConfigImpl( props );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );
    
    config = new DistributionConfigImpl( config );
    isEqual( config.getSSLEnabled(), sslenabled );
    isEqual( config.getSSLProtocols(), sslprotocols );
    isEqual( config.getSSLCiphers(), sslciphers );
    isEqual( config.getSSLRequireAuthentication(), requireAuth );
  }
  
  @Test
  public void testConfigCopyWithClusterSSL( ) throws Exception {
    boolean sslenabled = false;
    String sslprotocols = "any";
    String sslciphers = "any";
    boolean requireAuth = true;
    
    DistributionConfigImpl config = new DistributionConfigImpl( new Properties() );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );
    
    Properties props = new Properties();
    sslciphers = "RSA_WITH_GARBAGE";
    props.setProperty("cluster-ssl-ciphers", sslciphers );

    config = new DistributionConfigImpl( props );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );
    
    sslprotocols = "SSLv7";
    props.setProperty("cluster-ssl-protocols", sslprotocols );

    config = new DistributionConfigImpl( props );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    requireAuth = false;
    props.setProperty("cluster-ssl-require-authentication", String.valueOf( requireAuth ) );

    config = new DistributionConfigImpl( props );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    sslenabled = true;
    props.setProperty("cluster-ssl-enabled", String.valueOf( sslenabled ) );
    props.setProperty("mcast-port", "0" );

    config = new DistributionConfigImpl( props );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );
    
    config = new DistributionConfigImpl( config );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );
  }

  @Test
  public void testManagerDefaultConfig() throws Exception {
    boolean sslenabled = false;
    String sslprotocols = "any";
    String sslciphers = "any";
    boolean requireAuth = true;

    boolean jmxManagerSsl = false;
    boolean jmxManagerSslenabled = false;
    String jmxManagerSslprotocols = "any";
    String jmxManagerSslciphers = "any";
    boolean jmxManagerSslRequireAuth = true;

    DistributionConfigImpl config = new DistributionConfigImpl( new Properties() );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );
    
    isEqual( config.getJmxManagerSSLEnabled(), jmxManagerSsl);
    isEqual( config.getJmxManagerSSLEnabled(), jmxManagerSslenabled );
    isEqual( config.getJmxManagerSSLProtocols(), jmxManagerSslprotocols );
    isEqual( config.getJmxManagerSSLCiphers(), jmxManagerSslciphers );
    isEqual( config.getJmxManagerSSLRequireAuthentication(), jmxManagerSslRequireAuth );
  }
  
  @Test
  public void testCacheServerDefaultConfig() throws Exception {
    boolean sslenabled = false;
    String sslprotocols = "any";
    String sslciphers = "any";
    boolean requireAuth = true;

    boolean cacheServerSslenabled = false;
    String cacheServerSslprotocols = "any";
    String cacheServerSslciphers = "any";
    boolean cacheServerSslRequireAuth = true;

    DistributionConfigImpl config = new DistributionConfigImpl( new Properties() );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getServerSSLEnabled(), cacheServerSslenabled );
    isEqual( config.getServerSSLProtocols(), cacheServerSslprotocols );
    isEqual( config.getServerSSLCiphers(), cacheServerSslciphers );
    isEqual( config.getServerSSLRequireAuthentication(), cacheServerSslRequireAuth );
  }
  
  @Test
  public void testGatewayDefaultConfig() throws Exception {
    boolean sslenabled = false;
    String sslprotocols = "any";
    String sslciphers = "any";
    boolean requireAuth = true;

    boolean gatewaySslenabled = false;
    String gatewaySslprotocols = "any";
    String gatewaySslciphers = "any";
    boolean gatewaySslRequireAuth = true;

    DistributionConfigImpl config = new DistributionConfigImpl( new Properties() );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getGatewaySSLEnabled(), gatewaySslenabled );
    isEqual( config.getGatewaySSLProtocols(), gatewaySslprotocols );
    isEqual( config.getGatewaySSLCiphers(), gatewaySslciphers );
    isEqual( config.getGatewaySSLRequireAuthentication(), gatewaySslRequireAuth );
  }
  

  @Test
  public void testManagerConfig() throws Exception {
    boolean sslenabled = false;
    String  sslprotocols = "any";
    String  sslciphers = "any";
    boolean requireAuth = true;

    boolean jmxManagerSsl = true;
    boolean jmxManagerSslenabled = true;
    String  jmxManagerSslprotocols = "SSLv7";
    String  jmxManagerSslciphers = "RSA_WITH_GARBAGE";
    boolean jmxManagerSslRequireAuth = true;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_NAME, String.valueOf(jmxManagerSsl));
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_ENABLED_NAME, String.valueOf(jmxManagerSslenabled));
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_PROTOCOLS_NAME, jmxManagerSslprotocols);
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_CIPHERS_NAME, jmxManagerSslciphers);
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(jmxManagerSslRequireAuth));
    try{
      DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    }catch(IllegalArgumentException e){
      if (! e.toString().contains( "Gemfire property \'jmx-manager-ssl\' and \'jmx-manager-ssl-enabled\' can not be used at the same time")) {
        throw new Exception( "did not get expected exception, got this instead...", e );
      }
    }
    
    gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));

    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_NAME, String.valueOf(jmxManagerSsl));
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_PROTOCOLS_NAME, jmxManagerSslprotocols);
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_CIPHERS_NAME, jmxManagerSslciphers);
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(jmxManagerSslRequireAuth));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getJmxManagerSSLEnabled(), jmxManagerSslenabled );
    isEqual( config.getJmxManagerSSLProtocols(), jmxManagerSslprotocols );
    isEqual( config.getJmxManagerSSLCiphers(), jmxManagerSslciphers );
    isEqual( config.getJmxManagerSSLRequireAuthentication(), jmxManagerSslRequireAuth );
  }
  
  
  @Test
  public void testCacheServerConfig() throws Exception {
    boolean sslenabled = false;
    String  sslprotocols = "any";
    String  sslciphers = "any";
    boolean requireAuth = true;

    boolean cacheServerSslenabled = true;
    String  cacheServerSslprotocols = "SSLv7";
    String  cacheServerSslciphers = "RSA_WITH_GARBAGE";
    boolean cacheServerSslRequireAuth = true;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));

    gemFireProps.put(DistributionConfig.SERVER_SSL_ENABLED_NAME, String.valueOf(cacheServerSslenabled));
    gemFireProps.put(DistributionConfig.SERVER_SSL_PROTOCOLS_NAME, cacheServerSslprotocols);
    gemFireProps.put(DistributionConfig.SERVER_SSL_CIPHERS_NAME, cacheServerSslciphers);
    gemFireProps.put(DistributionConfig.SERVER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(cacheServerSslRequireAuth));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getServerSSLEnabled(), cacheServerSslenabled );
    isEqual( config.getServerSSLProtocols(), cacheServerSslprotocols );
    isEqual( config.getServerSSLCiphers(), cacheServerSslciphers );
    isEqual( config.getServerSSLRequireAuthentication(), cacheServerSslRequireAuth );
  }

  @Test
  public void testGatewayConfig() throws Exception {
    boolean sslenabled = false;
    String  sslprotocols = "any";
    String  sslciphers = "any";
    boolean requireAuth = true;

    boolean gatewaySslenabled = true;
    String  gatewaySslprotocols = "SSLv7";
    String  gatewaySslciphers = "RSA_WITH_GARBAGE";
    boolean gatewaySslRequireAuth = true;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));

    gemFireProps.put(DistributionConfig.GATEWAY_SSL_ENABLED_NAME, String.valueOf(gatewaySslenabled));
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_PROTOCOLS_NAME, gatewaySslprotocols);
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_CIPHERS_NAME, gatewaySslciphers);
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(gatewaySslRequireAuth));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getGatewaySSLEnabled(), gatewaySslenabled );
    isEqual( config.getGatewaySSLProtocols(), gatewaySslprotocols );
    isEqual( config.getGatewaySSLCiphers(), gatewaySslciphers );
    isEqual( config.getGatewaySSLRequireAuthentication(), gatewaySslRequireAuth );
  }
  
  @Test
  public void testCustomizedClusterSslConfig() throws Exception {
    
    boolean sslenabled = true;
    String  sslprotocols = "SSLv1";
    String  sslciphers = "RSA_WITH_NOTHING";
    boolean requireAuth = true;

    boolean clusterSslenabled = true;
    String  clusterSslprotocols = "SSLv7";
    String  clusterSslciphers = "RSA_WITH_GARBAGE";
    boolean clusterSslRequireAuth = true;
    
    //sslEnabled and clusterSSLEnabled set at the same time
    Properties gemFireProps = new Properties();
    gemFireProps.setProperty( "mcast-port", "0" );
    gemFireProps.put(DistributionConfig.SSL_ENABLED_NAME, "true");
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, "true");
    DistributionConfigImpl config = null;
    try{
      config = new DistributionConfigImpl( gemFireProps );
    }catch(IllegalArgumentException e){
      if (! e.toString().contains( "Gemfire property \'ssl-enabled\' and \'cluster-ssl-enabled\' can not be used at the same time")) {
        throw new Exception( "did not get expected exception, got this instead...", e );
      }
    }
    
    //ssl-protocol and clsuter-ssl-protocol set at the same time
    gemFireProps = new Properties();
    gemFireProps.setProperty( "mcast-port", "0" );
    gemFireProps.put(DistributionConfig.SSL_ENABLED_NAME, "true");
    gemFireProps.put(DistributionConfig.SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, "false");
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, clusterSslprotocols);
    try{
      config = new DistributionConfigImpl( gemFireProps );
    }catch(IllegalArgumentException e){
      if (! e.toString().contains( "Gemfire property \'ssl-protocols\' and \'cluster-ssl-protocols\' can not be used at the same time") ) {
        throw new Exception( "did not get expected exception, got this instead...", e );
      }
    }
    
    //ssl-cipher and clsuter-ssl-cipher set at the same time
    gemFireProps = new Properties();
    gemFireProps.setProperty( "mcast-port", "0" );
    gemFireProps.put(DistributionConfig.SSL_ENABLED_NAME, "true");
    gemFireProps.put(DistributionConfig.SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, "false");
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, clusterSslciphers);
    try{
      config = new DistributionConfigImpl( gemFireProps );
    }catch(IllegalArgumentException e){
      if (! e.toString().contains( "Gemfire property \'ssl-cipher\' and \'cluster-ssl-cipher\' can not be used at the same time") ) {
        throw new Exception( "did not get expected exception, got this instead...", e );
      }
    }
    
  //ssl-require-authentication and clsuter-ssl-require-authentication set at the same time
    gemFireProps = new Properties();
    gemFireProps.setProperty( "mcast-port", "0" );
    gemFireProps.put(DistributionConfig.SSL_ENABLED_NAME, "true");
    gemFireProps.put(DistributionConfig.SSL_REQUIRE_AUTHENTICATION_NAME, "true");
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, "false");
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, "true");
    try{
      config = new DistributionConfigImpl( gemFireProps );
    }catch(IllegalArgumentException e){
      if (! e.toString().contains( "Gemfire property \'ssl-require-authentication\' and \'cluster-ssl-require-authentication\' can not be used at the same time") ) {
        throw new Exception( "did not get expected exception, got this instead...", e );
      }
    }
    
    // only ssl-* properties provided. same should reflect in cluster-ssl properties
    gemFireProps = new Properties();
    gemFireProps.setProperty("mcast-port", "0");
    gemFireProps.put(DistributionConfig.SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));
    gemFireProps.put(DistributionConfig.SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.SSL_PROTOCOLS_NAME, sslprotocols);

    gemFireProps.putAll(getGfSecurityPropertiesSSL());
    
    config = new DistributionConfigImpl(gemFireProps);

    isEqual(sslenabled, config.getSSLEnabled());
    isEqual(sslprotocols, config.getSSLProtocols());
    isEqual(sslciphers, config.getSSLCiphers());
    isEqual(requireAuth, config.getSSLRequireAuthentication());

    isEqual(sslenabled, config.getClusterSSLEnabled());
    isEqual(sslprotocols, config.getClusterSSLProtocols());
    isEqual(sslciphers, config.getClusterSSLCiphers());
    isEqual(requireAuth, config.getClusterSSLRequireAuthentication());
    
    Properties sslProperties = config.getSSLProperties();
    isEqual( SSL_PROPS_MAP , sslProperties);

    Properties clusterSSLProperties = config.getClusterSSLProperties();
    isEqual( SSL_PROPS_MAP, clusterSSLProperties );
    
    //only clutser-ssl-properties provided.
    gemFireProps = new Properties();
    gemFireProps.setProperty("mcast-port", "0");
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, String.valueOf(clusterSslenabled));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(clusterSslRequireAuth));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, clusterSslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, clusterSslprotocols);

    gemFireProps.putAll(getGfSecurityPropertiesCluster(false));
    
    config = new DistributionConfigImpl(gemFireProps);

    isEqual(clusterSslenabled, config.getClusterSSLEnabled());
    isEqual(clusterSslprotocols, config.getClusterSSLProtocols());
    isEqual(clusterSslciphers, config.getClusterSSLCiphers());
    isEqual(clusterSslRequireAuth, config.getClusterSSLRequireAuthentication());
    
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore"), config.getClusterSSLKeyStore() );
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getClusterSSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getClusterSSLKeyStorePassword());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore"), config.getClusterSSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"), config.getClusterSSLTrustStorePassword());
    
    clusterSSLProperties = config.getClusterSSLProperties();
    isEqual( SSL_PROPS_MAP, clusterSSLProperties );
    
  }
  
  @Test
  public void testCustomizedManagerSslConfig() throws Exception {
    boolean sslenabled = false;
    String  sslprotocols = "any";
    String  sslciphers = "any";
    boolean requireAuth = true;

    boolean jmxManagerSslenabled = true;
    String  jmxManagerSslprotocols = "SSLv7";
    String  jmxManagerSslciphers = "RSA_WITH_GARBAGE";
    boolean jmxManagerSslRequireAuth = true;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));

    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_ENABLED_NAME, String.valueOf(jmxManagerSslenabled));
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_PROTOCOLS_NAME, jmxManagerSslprotocols);
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_CIPHERS_NAME, jmxManagerSslciphers);
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(jmxManagerSslRequireAuth));

    gemFireProps.putAll(getGfSecurityPropertiesJMX(false /*partialJmxSslConfigOverride*/));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getJmxManagerSSLEnabled(), jmxManagerSslenabled );
    isEqual( config.getJmxManagerSSLProtocols(), jmxManagerSslprotocols );
    isEqual( config.getJmxManagerSSLCiphers(), jmxManagerSslciphers );
    isEqual( config.getJmxManagerSSLRequireAuthentication(), jmxManagerSslRequireAuth );

    isEqual( JMX_SSL_PROPS_MAP.get("jmx-manager-ssl-keystore") , config.getJmxManagerSSLKeyStore());
    isEqual( JMX_SSL_PROPS_MAP.get("jmx-manager-ssl-keystore-type"), config.getJmxManagerSSLKeyStoreType());
    isEqual( JMX_SSL_PROPS_MAP.get("jmx-manager-ssl-keystore-password"), config.getJmxManagerSSLKeyStorePassword());
    isEqual( JMX_SSL_PROPS_MAP.get("jmx-manager-ssl-truststore"), config.getJmxManagerSSLTrustStore());
    isEqual( JMX_SSL_PROPS_MAP.get("jmx-manager-ssl-truststore-password"),config.getJmxManagerSSLTrustStorePassword());
  }
  
  @Test
  public void testCustomizedCacheServerSslConfig() throws Exception {
    boolean sslenabled = false;
    String  sslprotocols = "any";
    String  sslciphers = "any";
    boolean requireAuth = true;

    boolean cacheServerSslenabled = true;
    String  cacheServerSslprotocols = "SSLv7";
    String  cacheServerSslciphers = "RSA_WITH_GARBAGE";
    boolean cacheServerSslRequireAuth = true;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));

    gemFireProps.put(DistributionConfig.SERVER_SSL_ENABLED_NAME, String.valueOf(cacheServerSslenabled));
    gemFireProps.put(DistributionConfig.SERVER_SSL_PROTOCOLS_NAME, cacheServerSslprotocols);
    gemFireProps.put(DistributionConfig.SERVER_SSL_CIPHERS_NAME, cacheServerSslciphers);
    gemFireProps.put(DistributionConfig.SERVER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(cacheServerSslRequireAuth));

    gemFireProps.putAll(getGfSecurityPropertiesforCS(false));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getServerSSLEnabled(), cacheServerSslenabled );
    isEqual( config.getServerSSLProtocols(), cacheServerSslprotocols );
    isEqual( config.getServerSSLCiphers(), cacheServerSslciphers );
    isEqual( config.getServerSSLRequireAuthentication(), cacheServerSslRequireAuth );

    isEqual( SERVER_SSL_PROPS_MAP.get("server-ssl-keystore") , config.getServerSSLKeyStore());
    isEqual( SERVER_SSL_PROPS_MAP.get("server-ssl-keystore-type"), config.getServerSSLKeyStoreType());
    isEqual( SERVER_SSL_PROPS_MAP.get("server-ssl-keystore-password"), config.getServerSSLKeyStorePassword());
    isEqual( SERVER_SSL_PROPS_MAP.get("server-ssl-truststore"), config.getServerSSLTrustStore());
    isEqual( SERVER_SSL_PROPS_MAP.get("server-ssl-truststore-password"),config.getServerSSLTrustStorePassword());
  }

  @Test
  public void testCustomizedGatewaySslConfig() throws Exception {
    boolean sslenabled = false;
    String  sslprotocols = "any";
    String  sslciphers = "any";
    boolean requireAuth = true;

    boolean gatewaySslenabled = true;
    String  gatewaySslprotocols = "SSLv7";
    String  gatewaySslciphers = "RSA_WITH_GARBAGE";
    boolean gatewaySslRequireAuth = true;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));

    gemFireProps.put(DistributionConfig.GATEWAY_SSL_ENABLED_NAME, String.valueOf(gatewaySslenabled));
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_PROTOCOLS_NAME, gatewaySslprotocols);
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_CIPHERS_NAME, gatewaySslciphers);
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(gatewaySslRequireAuth));

    gemFireProps.putAll(getGfSecurityPropertiesforGateway(false));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getGatewaySSLEnabled(), gatewaySslenabled );
    isEqual( config.getGatewaySSLProtocols(), gatewaySslprotocols );
    isEqual( config.getGatewaySSLCiphers(), gatewaySslciphers );
    isEqual( config.getGatewaySSLRequireAuthentication(), gatewaySslRequireAuth );

    isEqual( GATEWAY_SSL_PROPS_MAP.get("gateway-ssl-keystore") , config.getGatewaySSLKeyStore());
    isEqual( GATEWAY_SSL_PROPS_MAP.get("gateway-ssl-keystore-type"), config.getGatewaySSLKeyStoreType());
    isEqual( GATEWAY_SSL_PROPS_MAP.get("gateway-ssl-keystore-password"), config.getGatewaySSLKeyStorePassword());
    isEqual( GATEWAY_SSL_PROPS_MAP.get("gateway-ssl-truststore"), config.getGatewaySSLTrustStore());
    isEqual( GATEWAY_SSL_PROPS_MAP.get("gateway-ssl-truststore-password"),config.getGatewaySSLTrustStorePassword());
    
  }
  
  @Test
  public void testPartialCustomizedManagerSslConfig() throws Exception {
    boolean sslenabled = false;
    String  sslprotocols = "any";
    String  sslciphers = "any";
    boolean requireAuth = true;

    boolean jmxManagerSslenabled = true;
    String  jmxManagerSslprotocols = "SSLv7";
    String  jmxManagerSslciphers = "RSA_WITH_GARBAGE";
    boolean jmxManagerSslRequireAuth = true;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));

    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_ENABLED_NAME, String.valueOf(jmxManagerSslenabled));
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_PROTOCOLS_NAME, jmxManagerSslprotocols);
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_CIPHERS_NAME, jmxManagerSslciphers);
    gemFireProps.put(DistributionConfig.JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(jmxManagerSslRequireAuth));

    gemFireProps.putAll(getGfSecurityPropertiesJMX(true /*partialJmxSslConfigOverride*/));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getJmxManagerSSLEnabled(), jmxManagerSslenabled );
    isEqual( config.getJmxManagerSSLProtocols(), jmxManagerSslprotocols );
    isEqual( config.getJmxManagerSSLCiphers(), jmxManagerSslciphers );
    isEqual( config.getJmxManagerSSLRequireAuthentication(), jmxManagerSslRequireAuth );

    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore"), config.getClusterSSLKeyStore() );
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getClusterSSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getClusterSSLKeyStorePassword());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore"), config.getClusterSSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"), config.getClusterSSLTrustStorePassword());
    
    isEqual( JMX_SSL_PROPS_SUBSET_MAP.get("jmx-manager-ssl-keystore") , config.getJmxManagerSSLKeyStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getJmxManagerSSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getJmxManagerSSLKeyStorePassword());
    isEqual( JMX_SSL_PROPS_SUBSET_MAP.get("jmx-manager-ssl-truststore"), config.getJmxManagerSSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"),config.getJmxManagerSSLTrustStorePassword());
  }
  
  
  @Test
  public void testPartialCustomizedCacheServerSslConfig() throws Exception {
    boolean sslenabled = false;
    String  sslprotocols = "any";
    String  sslciphers = "any";
    boolean requireAuth = true;

    boolean cacheServerSslenabled = true;
    String  cacheServerSslprotocols = "SSLv7";
    String  cacheServerSslciphers = "RSA_WITH_GARBAGE";
    boolean cacheServerSslRequireAuth = true;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));

    gemFireProps.put(DistributionConfig.SERVER_SSL_ENABLED_NAME, String.valueOf(cacheServerSslenabled));
    gemFireProps.put(DistributionConfig.SERVER_SSL_PROTOCOLS_NAME, cacheServerSslprotocols);
    gemFireProps.put(DistributionConfig.SERVER_SSL_CIPHERS_NAME, cacheServerSslciphers);
    gemFireProps.put(DistributionConfig.SERVER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(cacheServerSslRequireAuth));

    gemFireProps.putAll(getGfSecurityPropertiesforCS(true));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getServerSSLEnabled(), cacheServerSslenabled );
    isEqual( config.getServerSSLProtocols(), cacheServerSslprotocols );
    isEqual( config.getServerSSLCiphers(), cacheServerSslciphers );
    isEqual( config.getServerSSLRequireAuthentication(), cacheServerSslRequireAuth );

    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore"), config.getClusterSSLKeyStore() );
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getClusterSSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getClusterSSLKeyStorePassword());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore"), config.getClusterSSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"), config.getClusterSSLTrustStorePassword());
    
    isEqual( SERVER_PROPS_SUBSET_MAP.get("server-ssl-keystore") , config.getServerSSLKeyStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getServerSSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getServerSSLKeyStorePassword());
    isEqual( SERVER_PROPS_SUBSET_MAP.get("server-ssl-truststore"), config.getServerSSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"),config.getServerSSLTrustStorePassword());
  }
  
  @Test
  public void testPartialCustomizedGatewaySslConfig() throws Exception {
    boolean sslenabled = false;
    String  sslprotocols = "any";
    String  sslciphers = "any";
    boolean requireAuth = true;

    boolean gatewaySslenabled = true;
    String  gatewaySslprotocols = "SSLv7";
    String  gatewaySslciphers = "RSA_WITH_GARBAGE";
    boolean gatewaySslRequireAuth = true;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));

    gemFireProps.put(DistributionConfig.GATEWAY_SSL_ENABLED_NAME, String.valueOf(gatewaySslenabled));
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_PROTOCOLS_NAME, gatewaySslprotocols);
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_CIPHERS_NAME, gatewaySslciphers);
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(gatewaySslRequireAuth));

    gemFireProps.putAll(getGfSecurityPropertiesforGateway(true));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getGatewaySSLEnabled(), gatewaySslenabled );
    isEqual( config.getGatewaySSLProtocols(), gatewaySslprotocols );
    isEqual( config.getGatewaySSLCiphers(), gatewaySslciphers );
    isEqual( config.getGatewaySSLRequireAuthentication(), gatewaySslRequireAuth );
    
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore"), config.getClusterSSLKeyStore() );
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getClusterSSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getClusterSSLKeyStorePassword());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore"), config.getClusterSSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"), config.getClusterSSLTrustStorePassword());
    
    isEqual( GATEWAY_PROPS_SUBSET_MAP.get("gateway-ssl-keystore") , config.getGatewaySSLKeyStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getGatewaySSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getGatewaySSLKeyStorePassword());
    isEqual( GATEWAY_PROPS_SUBSET_MAP.get("gateway-ssl-truststore"), config.getGatewaySSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"),config.getGatewaySSLTrustStorePassword());

  }
  
  @Test
  public void testP2pSSLPropsOverriden_ServerPropsNotOverriden(){
    boolean sslenabled = true;
    String  sslprotocols = "overrriden";
    String  sslciphers = "overrriden";
    boolean requireAuth = true;

    boolean cacheServerSslenabled = false;
    String  cacheServerSslprotocols = "SSLv7";
    String  cacheServerSslciphers = "RSA_WITH_GARBAGE";
    boolean cacheServerSslRequireAuth = false;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.MCAST_PORT_NAME,"0");
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));

    gemFireProps.putAll(getGfSecurityPropertiesforCS(true));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getServerSSLEnabled(), sslenabled );
    isEqual( config.getServerSSLProtocols(), sslprotocols );
    isEqual( config.getServerSSLCiphers(), sslciphers );
    isEqual( config.getServerSSLRequireAuthentication(), requireAuth );
    
    assertFalse(config.getServerSSLEnabled()==cacheServerSslenabled);
    assertFalse(config.getServerSSLProtocols().equals(cacheServerSslprotocols));
    assertFalse(config.getServerSSLCiphers().equals(cacheServerSslciphers));
    assertFalse(config.getServerSSLRequireAuthentication()==cacheServerSslRequireAuth);
    
    System.out.println(config.toLoggerString());

    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore"), config.getClusterSSLKeyStore() );
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getClusterSSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getClusterSSLKeyStorePassword());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore"), config.getClusterSSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"), config.getClusterSSLTrustStorePassword());
    
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore") , config.getServerSSLKeyStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getServerSSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getServerSSLKeyStorePassword());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore"), config.getServerSSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"),config.getServerSSLTrustStorePassword());
    
  }
  
  @Test
  public void testP2pSSLPropsOverriden_ServerPropsOverriden(){
    boolean sslenabled = true;
    String  sslprotocols = "overrriden";
    String  sslciphers = "overrriden";
    boolean requireAuth = true;

    boolean cacheServerSslenabled = false;
    String  cacheServerSslprotocols = "SSLv7";
    String  cacheServerSslciphers = "RSA_WITH_GARBAGE";
    boolean cacheServerSslRequireAuth = false;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.MCAST_PORT_NAME,"0");
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));
    
    gemFireProps.put(DistributionConfig.SERVER_SSL_ENABLED_NAME, String.valueOf(cacheServerSslenabled));
    gemFireProps.put(DistributionConfig.SERVER_SSL_PROTOCOLS_NAME, cacheServerSslprotocols);
    gemFireProps.put(DistributionConfig.SERVER_SSL_CIPHERS_NAME, cacheServerSslciphers);
    gemFireProps.put(DistributionConfig.SERVER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(cacheServerSslRequireAuth));

    gemFireProps.putAll(getGfSecurityPropertiesforCS(true));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getServerSSLEnabled(), cacheServerSslenabled );
    isEqual( config.getServerSSLProtocols(), cacheServerSslprotocols );
    isEqual( config.getServerSSLCiphers(), cacheServerSslciphers );
    isEqual( config.getServerSSLRequireAuthentication(), cacheServerSslRequireAuth );
    
    assertFalse(config.getServerSSLEnabled()==sslenabled);
    assertFalse(config.getServerSSLProtocols().equals(sslprotocols));
    assertFalse(config.getServerSSLCiphers().equals(sslciphers));
    assertFalse(config.getServerSSLRequireAuthentication()==requireAuth);
    
    System.out.println(config.toLoggerString());

    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore"), config.getClusterSSLKeyStore() );
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getClusterSSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getClusterSSLKeyStorePassword());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore"), config.getClusterSSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"), config.getClusterSSLTrustStorePassword());
    
    isEqual( SERVER_PROPS_SUBSET_MAP.get("server-ssl-keystore") , config.getServerSSLKeyStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getServerSSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getServerSSLKeyStorePassword());
    isEqual( SERVER_PROPS_SUBSET_MAP.get("server-ssl-truststore"), config.getServerSSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"),config.getServerSSLTrustStorePassword());
  }
  
  @Test
  public void testClusterSSLPropsOverriden_GatewayPropsNotOverriden(){
    boolean sslenabled = true;
    String  sslprotocols = "overrriden";
    String  sslciphers = "overrriden";
    boolean requireAuth = true;

    boolean gatewayServerSslenabled = false;
    String  gatewayServerSslprotocols = "SSLv7";
    String  gatewayServerSslciphers = "RSA_WITH_GARBAGE";
    boolean gatewayServerSslRequireAuth = false;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.MCAST_PORT_NAME,"0");
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));

    gemFireProps.putAll(getGfSecurityPropertiesforGateway(true));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getGatewaySSLEnabled(), sslenabled );
    isEqual( config.getGatewaySSLProtocols(), sslprotocols );
    isEqual( config.getGatewaySSLCiphers(), sslciphers );
    isEqual( config.getGatewaySSLRequireAuthentication(), requireAuth );
    
    assertFalse(config.getGatewaySSLEnabled()==gatewayServerSslenabled);
    assertFalse(config.getGatewaySSLProtocols().equals(gatewayServerSslprotocols));
    assertFalse(config.getGatewaySSLCiphers().equals(gatewayServerSslciphers));
    assertFalse(config.getGatewaySSLRequireAuthentication()==gatewayServerSslRequireAuth);
    
    System.out.println(config.toLoggerString());

    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore"), config.getClusterSSLKeyStore() );
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getClusterSSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getClusterSSLKeyStorePassword());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore"), config.getClusterSSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"), config.getClusterSSLTrustStorePassword());
    
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore") , config.getGatewaySSLKeyStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getGatewaySSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getGatewaySSLKeyStorePassword());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore"), config.getGatewaySSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"),config.getGatewaySSLTrustStorePassword());
    
  }
  
  @Test
  public void testP2pSSLPropsOverriden_GatewayPropsOverriden(){
    boolean sslenabled = true;
    String  sslprotocols = "overrriden";
    String  sslciphers = "overrriden";
    boolean requireAuth = true;

    boolean gatewayServerSslenabled = false;
    String  gatewayServerSslprotocols = "SSLv7";
    String  gatewayServerSslciphers = "RSA_WITH_GARBAGE";
    boolean gatewayServerSslRequireAuth = false;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.MCAST_PORT_NAME,"0");
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));
    
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_ENABLED_NAME, String.valueOf(gatewayServerSslenabled));
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_PROTOCOLS_NAME, gatewayServerSslprotocols);
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_CIPHERS_NAME, gatewayServerSslciphers);
    gemFireProps.put(DistributionConfig.GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(gatewayServerSslRequireAuth));

    gemFireProps.putAll(getGfSecurityPropertiesforGateway(true));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getGatewaySSLEnabled(), gatewayServerSslenabled );
    isEqual( config.getGatewaySSLProtocols(), gatewayServerSslprotocols );
    isEqual( config.getGatewaySSLCiphers(), gatewayServerSslciphers );
    isEqual( config.getGatewaySSLRequireAuthentication(), gatewayServerSslRequireAuth );
    
    System.out.println(config.toLoggerString());

    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore"), config.getClusterSSLKeyStore() );
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getClusterSSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getClusterSSLKeyStorePassword());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore"), config.getClusterSSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"), config.getClusterSSLTrustStorePassword());
    
    isEqual( GATEWAY_PROPS_SUBSET_MAP.get("gateway-ssl-keystore") , config.getGatewaySSLKeyStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getGatewaySSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getGatewaySSLKeyStorePassword());
    isEqual( GATEWAY_PROPS_SUBSET_MAP.get("gateway-ssl-truststore"), config.getGatewaySSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"),config.getGatewaySSLTrustStorePassword());
    
  }
  
  @Test
  public void testP2pSSLPropsOverriden_JMXPropsNotOverriden(){
    boolean sslenabled = true;
    String  sslprotocols = "overrriden";
    String  sslciphers = "overrriden";
    boolean requireAuth = true;

    boolean jmxManagerSslenabled = false;
    String  jmxManagerSslprotocols = "SSLv7";
    String  jmxManagerSslciphers = "RSA_WITH_GARBAGE";
    boolean jmxManagerSslRequireAuth = false;

    Properties gemFireProps = new Properties();
    gemFireProps.put(DistributionConfig.MCAST_PORT_NAME,"0");
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, String.valueOf(sslenabled));
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslprotocols);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslciphers);
    gemFireProps.put(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, String.valueOf(requireAuth));

    gemFireProps.putAll(getGfSecurityPropertiesJMX(true));

    DistributionConfigImpl config = new DistributionConfigImpl( gemFireProps );
    isEqual( config.getClusterSSLEnabled(), sslenabled );
    isEqual( config.getClusterSSLProtocols(), sslprotocols );
    isEqual( config.getClusterSSLCiphers(), sslciphers );
    isEqual( config.getClusterSSLRequireAuthentication(), requireAuth );

    isEqual( config.getJmxManagerSSLEnabled(), sslenabled );
    isEqual( config.getJmxManagerSSLProtocols(), sslprotocols );
    isEqual( config.getJmxManagerSSLCiphers(), sslciphers );
    isEqual( config.getJmxManagerSSLRequireAuthentication(), requireAuth );
    
    assertFalse(config.getJmxManagerSSLEnabled()==jmxManagerSslenabled);
    assertFalse(config.getJmxManagerSSLProtocols().equals(jmxManagerSslprotocols));
    assertFalse(config.getJmxManagerSSLCiphers().equals(jmxManagerSslciphers));
    assertFalse(config.getJmxManagerSSLRequireAuthentication()==jmxManagerSslRequireAuth);
    
    System.out.println(config.toLoggerString());

    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore"), config.getClusterSSLKeyStore() );
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getClusterSSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getClusterSSLKeyStorePassword());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore"), config.getClusterSSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"), config.getClusterSSLTrustStorePassword());
    
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore") , config.getJmxManagerSSLKeyStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-type"), config.getJmxManagerSSLKeyStoreType());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-keystore-password"), config.getJmxManagerSSLKeyStorePassword());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore"), config.getJmxManagerSSLTrustStore());
    isEqual( CLUSTER_SSL_PROPS_MAP.get("cluster-ssl-truststore-password"),config.getJmxManagerSSLTrustStorePassword()); 
    
  }
  
  private static Properties getGfSecurityPropertiesSSL() {
    Properties gfSecurityProps = new Properties();

    Set<Entry<Object, Object>> entrySet = SSL_PROPS_MAP.entrySet();
    for (Entry<Object, Object> entry : entrySet) {
      gfSecurityProps.put(entry.getKey(), entry.getValue());
    }

    return gfSecurityProps;
  }
  
  private static Properties getGfSecurityPropertiesCluster(boolean partialClusterSslConfigOverride) {
    Properties gfSecurityProps = new Properties();

    Set<Entry<Object, Object>> entrySet = SSL_PROPS_MAP.entrySet();
    for (Entry<Object, Object> entry : entrySet) {
      gfSecurityProps.put(entry.getKey(), entry.getValue());
    }

    if (partialClusterSslConfigOverride) {
      entrySet = CLUSTER_SSL_PROPS_SUBSET_MAP.entrySet();
    } else {
      entrySet = CLUSTER_SSL_PROPS_MAP.entrySet();
    }
    for (Entry<Object, Object> entry : entrySet) {
      gfSecurityProps.put(entry.getKey(), entry.getValue());
    }
    return gfSecurityProps;
  }
  
  private static Properties getGfSecurityPropertiesJMX(boolean partialJmxSslConfigOverride) {
    Properties gfSecurityProps = new Properties();

    Set<Entry<Object, Object>> entrySet = CLUSTER_SSL_PROPS_MAP.entrySet();
    for (Entry<Object, Object> entry : entrySet) {
      gfSecurityProps.put(entry.getKey(), entry.getValue());
    }

    if (partialJmxSslConfigOverride) {
      entrySet = JMX_SSL_PROPS_SUBSET_MAP.entrySet();
    } else {
      entrySet = JMX_SSL_PROPS_MAP.entrySet();
    }
    for (Entry<Object, Object> entry : entrySet) {
      // Add "-jmx" suffix for JMX Manager properties.
      gfSecurityProps.put(entry.getKey(), entry.getValue());
    }

    return gfSecurityProps;
  }
  
  private static Properties getGfSecurityPropertiesforCS(boolean partialCSSslConfigOverride) {
    Properties gfSecurityProps = new Properties();

    Set<Entry<Object, Object>> entrySet = CLUSTER_SSL_PROPS_MAP.entrySet();
    for (Entry<Object, Object> entry : entrySet) {
      gfSecurityProps.put(entry.getKey(), entry.getValue());
    }

    if (partialCSSslConfigOverride) {
      entrySet = SERVER_PROPS_SUBSET_MAP.entrySet();
    } else {
      entrySet = SERVER_SSL_PROPS_MAP.entrySet();
    }
    for (Entry<Object, Object> entry : entrySet) {
      // Add "-cacheserver" suffix for CacheServer properties.
      gfSecurityProps.put(entry.getKey(), entry.getValue());
    }
    gfSecurityProps.list(System.out);
    return gfSecurityProps;
  }

  private static Properties getGfSecurityPropertiesforGateway(boolean partialGatewaySslConfigOverride) {
    Properties gfSecurityProps = new Properties();

    Set<Entry<Object, Object>> entrySet = CLUSTER_SSL_PROPS_MAP.entrySet();
    for (Entry<Object, Object> entry : entrySet) {
      gfSecurityProps.put(entry.getKey(), entry.getValue());
    }

    if (partialGatewaySslConfigOverride) {
      entrySet = GATEWAY_PROPS_SUBSET_MAP.entrySet();
    } else {
      entrySet = GATEWAY_SSL_PROPS_MAP.entrySet();
    }
    for (Entry<Object, Object> entry : entrySet) {
      gfSecurityProps.put(entry.getKey(), entry.getValue());
    }
    gfSecurityProps.list(System.out);
    return gfSecurityProps;
  }
  
  public void isEqual( boolean a, boolean e ) throws AssertionFailedError {
    assertEquals( a, e );
  }
  
  public void isEqual( Object a, Object e ) throws AssertionFailedError {
    assertEquals( a, e );
  } 
  
}
