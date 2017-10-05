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

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_PREFIX;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_PREFIX;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_PREFIX;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang.StringUtils;
import org.apache.geode.internal.DSFIDFactory;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.JmxManagerLocatorRequest;
import org.apache.geode.management.internal.JmxManagerLocatorResponse;
import org.apache.geode.management.internal.SSLUtil;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.converters.ConnectionEndpointConverter;
import org.apache.geode.management.internal.cli.domain.ConnectToLocatorResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.JmxOperationInvoker;
import org.apache.geode.management.internal.cli.util.ConnectionEndpoint;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.management.internal.web.shell.HttpOperationInvoker;
import org.apache.geode.security.AuthenticationFailedException;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

public class ConnectCommand implements GfshCommand {
  // millis that connect --locator will wait for a response from the locator.
  static final int CONNECT_LOCATOR_TIMEOUT_MS = 60000; // see bug 45971

  private static final UserInputProperty[] USER_INPUT_PROPERTIES =
      {UserInputProperty.KEYSTORE, UserInputProperty.KEYSTORE_PASSWORD,
          UserInputProperty.KEYSTORE_TYPE, UserInputProperty.TRUSTSTORE,
          UserInputProperty.TRUSTSTORE_PASSWORD, UserInputProperty.TRUSTSTORE_TYPE,
          UserInputProperty.CIPHERS, UserInputProperty.PROTOCOL, UserInputProperty.COMPONENT};

  @CliCommand(value = {CliStrings.CONNECT}, help = CliStrings.CONNECT__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH, CliStrings.TOPIC_GEODE_JMX,
      CliStrings.TOPIC_GEODE_MANAGER})
  public Result connect(
      @CliOption(key = {CliStrings.CONNECT__LOCATOR},
          unspecifiedDefaultValue = ConnectionEndpointConverter.DEFAULT_LOCATOR_ENDPOINTS,
          optionContext = ConnectionEndpoint.LOCATOR_OPTION_CONTEXT,
          help = CliStrings.CONNECT__LOCATOR__HELP) ConnectionEndpoint locatorEndPoint,
      @CliOption(key = {CliStrings.CONNECT__JMX_MANAGER},
          optionContext = ConnectionEndpoint.JMXMANAGER_OPTION_CONTEXT,
          help = CliStrings.CONNECT__JMX_MANAGER__HELP) ConnectionEndpoint jmxManagerEndPoint,
      @CliOption(key = {CliStrings.CONNECT__USE_HTTP}, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.CONNECT__USE_HTTP__HELP) boolean useHttp,
      @CliOption(key = {CliStrings.CONNECT__URL},
          unspecifiedDefaultValue = CliStrings.CONNECT__DEFAULT_BASE_URL,
          help = CliStrings.CONNECT__URL__HELP) String url,
      @CliOption(key = {CliStrings.CONNECT__USERNAME},
          help = CliStrings.CONNECT__USERNAME__HELP) String userName,
      @CliOption(key = {CliStrings.CONNECT__PASSWORD},
          help = CliStrings.CONNECT__PASSWORD__HELP) String password,
      @CliOption(key = {CliStrings.CONNECT__KEY_STORE},
          help = CliStrings.CONNECT__KEY_STORE__HELP) String keystore,
      @CliOption(key = {CliStrings.CONNECT__KEY_STORE_PASSWORD},
          help = CliStrings.CONNECT__KEY_STORE_PASSWORD__HELP) String keystorePassword,
      @CliOption(key = {CliStrings.CONNECT__TRUST_STORE},
          help = CliStrings.CONNECT__TRUST_STORE__HELP) String truststore,
      @CliOption(key = {CliStrings.CONNECT__TRUST_STORE_PASSWORD},
          help = CliStrings.CONNECT__TRUST_STORE_PASSWORD__HELP) String truststorePassword,
      @CliOption(key = {CliStrings.CONNECT__SSL_CIPHERS},
          help = CliStrings.CONNECT__SSL_CIPHERS__HELP) String sslCiphers,
      @CliOption(key = {CliStrings.CONNECT__SSL_PROTOCOLS},
          help = CliStrings.CONNECT__SSL_PROTOCOLS__HELP) String sslProtocols,
      @CliOption(key = CliStrings.CONNECT__SECURITY_PROPERTIES, optionContext = ConverterHint.FILE,
          help = CliStrings.CONNECT__SECURITY_PROPERTIES__HELP) final File gfSecurityPropertiesFile,
      @CliOption(key = {CliStrings.CONNECT__USE_SSL}, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.CONNECT__USE_SSL__HELP) boolean useSsl,
      @CliOption(key = {"skip-ssl-validation"}, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = "When connecting via HTTP, connects using 1-way SSL validation rather than 2-way SSL validation.") boolean skipSslValidation) {

    Result result;
    Gfsh gfsh = getGfsh();

    // bail out if gfsh is already connected.
    if (gfsh != null && gfsh.isConnectedAndReady()) {
      return ResultBuilder
          .createInfoResult("Already connected to: " + getGfsh().getOperationInvoker().toString());
    }

    // ssl options are passed in in the order defined in USER_INPUT_PROPERTIES, note the two types
    // are null, because we don't have connect command options for them yet
    Properties gfProperties = resolveSslProperties(gfsh, useSsl, null, gfSecurityPropertiesFile,
        keystore, keystorePassword, null, truststore, truststorePassword, null, sslCiphers,
        sslProtocols, null);

    if (containsSSLConfig(gfProperties) || containsLegacySSLConfig(gfProperties)) {
      useSsl = true;
    }

    // if username is specified in the option but password is not, prompt for the password
    // note if gfProperties has username but no password, we would not prompt for password yet,
    // because we may not need username/password combination to connect.
    if (userName != null) {
      gfProperties.setProperty(ResourceConstants.USER_NAME, userName);
      if (password == null) {
        password = UserInputProperty.PASSWORD.promptForAcceptableValue(gfsh);
      }
      gfProperties.setProperty(UserInputProperty.PASSWORD.getKey(), password);
    }

    if (useHttp) {
      result = httpConnect(gfProperties, url, skipSslValidation);
    } else {
      result = jmxConnect(gfProperties, useSsl, jmxManagerEndPoint, locatorEndPoint, false);
    }

    return result;
  }

  /**
   * @param useSsl if true, and no files/options passed, we would still insist on prompting for ssl
   *        config (considered only when the last three parameters are null)
   * @param gfPropertiesFile gemfire properties file, can be null
   * @param gfSecurityPropertiesFile gemfire security properties file, can be null
   * @param sslOptionValues an array of 9 in this order, as defined in USER_INPUT_PROPERTIES
   * @return the properties
   */
  Properties resolveSslProperties(Gfsh gfsh, boolean useSsl, File gfPropertiesFile,
      File gfSecurityPropertiesFile, String... sslOptionValues) {

    // first trying to load the sslProperties from the file
    Properties gfProperties = loadProperties(gfPropertiesFile, gfSecurityPropertiesFile);

    // if the security file is a legacy ssl security file, then the rest of the command options, if
    // any, are ignored. Because we are not trying to add/replace the legacy ssl values using the
    // command line values. all command line ssl values updates the ssl-* options.
    if (containsLegacySSLConfig(gfProperties)) {
      return gfProperties;
    }

    // if nothing indicates we should prompt for missing ssl config info, return immediately
    if (!(useSsl || containsSSLConfig(gfProperties) || isSslImpliedBySslOptions(sslOptionValues))) {
      return gfProperties;
    }

    // if use ssl is implied by any of the options, then command option will add to/update the
    // properties loaded from file. If the ssl config is not specified anywhere, prompt user for it.
    for (int i = 0; i < USER_INPUT_PROPERTIES.length; i++) {
      UserInputProperty userInputProperty = USER_INPUT_PROPERTIES[i];
      String sslOptionValue = null;
      if (sslOptionValues != null && sslOptionValues.length > i) {
        sslOptionValue = sslOptionValues[i];
      }
      String sslConfigValue = gfProperties.getProperty(userInputProperty.getKey());

      // if this option is specified, always use this value
      if (sslOptionValue != null) {
        gfProperties.setProperty(userInputProperty.getKey(), sslOptionValue);
      }
      // if option is not specified and not present in the original properties, prompt for it
      else if (sslConfigValue == null) {
        gfProperties.setProperty(userInputProperty.getKey(),
            userInputProperty.promptForAcceptableValue(gfsh));
      }
    }

    return gfProperties;
  }

  boolean isSslImpliedBySslOptions(String... sslOptions) {
    return sslOptions != null && Arrays.stream(sslOptions).anyMatch(Objects::nonNull);
  }

  Properties loadProperties(File... files) {
    Properties properties = new Properties();
    if (files == null) {
      return properties;
    }
    for (File file : files) {
      if (file != null) {
        properties.putAll(loadPropertiesFromFile(file));
      }
    }
    return properties;
  }

  static boolean containsLegacySSLConfig(Properties properties) {
    return properties.stringPropertyNames().stream()
        .anyMatch(key -> key.startsWith(CLUSTER_SSL_PREFIX)
            || key.startsWith(JMX_MANAGER_SSL_PREFIX) || key.startsWith(HTTP_SERVICE_SSL_PREFIX));
  }

  private static boolean containsSSLConfig(Properties properties) {
    return properties.stringPropertyNames().stream().anyMatch(key -> key.startsWith("ssl-"));
  }


  Result httpConnect(Properties gfProperties, String url, boolean skipSslVerification) {
    Gfsh gfsh = getGfsh();
    try {
      SSLConfig sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(gfProperties,
          SecurableCommunicationChannel.WEB);
      if (sslConfig.isEnabled()) {
        configureHttpsURLConnection(sslConfig, skipSslVerification);
        if (url.startsWith("http:")) {
          url = url.replace("http:", "https:");
        }
      }

      // authentication check will be triggered inside the constructor
      HttpOperationInvoker operationInvoker = new HttpOperationInvoker(gfsh, url, gfProperties);

      gfsh.setOperationInvoker(operationInvoker);

      LogWrapper.getInstance()
          .info(CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS, operationInvoker.toString()));
      return ResultBuilder.createInfoResult(
          CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS, operationInvoker.toString()));

    } catch (SecurityException | AuthenticationFailedException e) {
      // if it's security exception, and we already sent in username and password, still returns the
      // connection error
      if (gfProperties.containsKey(ResourceConstants.USER_NAME)) {
        return handleException(e);
      }

      // otherwise, prompt for username and password and retry the connection
      gfProperties.setProperty(UserInputProperty.USERNAME.getKey(),
          UserInputProperty.USERNAME.promptForAcceptableValue(gfsh));
      gfProperties.setProperty(UserInputProperty.PASSWORD.getKey(),
          UserInputProperty.PASSWORD.promptForAcceptableValue(gfsh));
      return httpConnect(gfProperties, url, skipSslVerification);
    } catch (Exception e) {
      // all other exceptions, just logs it and returns a connection error
      return handleException(e);
    } finally {
      Gfsh.redirectInternalJavaLoggers();
    }
  }

  Result jmxConnect(Properties gfProperties, boolean useSsl, ConnectionEndpoint memberRmiHostPort,
      ConnectionEndpoint locatorTcpHostPort, boolean retry) {
    ConnectionEndpoint jmxHostPortToConnect = null;
    Gfsh gfsh = getGfsh();

    try {
      // trying to find the rmi host and port, if rmi host port exists, use that, otherwise, use
      // locator to find the rmi host port
      if (memberRmiHostPort != null) {
        jmxHostPortToConnect = memberRmiHostPort;
      } else {
        if (useSsl) {
          gfsh.logToFile(
              CliStrings.CONNECT__USE_SSL + " is set to true. Connecting to Locator via SSL.",
              null);
        }

        Gfsh.println(CliStrings.format(CliStrings.CONNECT__MSG__CONNECTING_TO_LOCATOR_AT_0,
            new Object[] {locatorTcpHostPort.toString(false)}));
        ConnectToLocatorResult connectToLocatorResult =
            connectToLocator(locatorTcpHostPort.getHost(), locatorTcpHostPort.getPort(),
                CONNECT_LOCATOR_TIMEOUT_MS, gfProperties);
        jmxHostPortToConnect = connectToLocatorResult.getMemberEndpoint();

        // when locator is configured to use SSL (ssl-enabled=true) but manager is not
        // (jmx-manager-ssl=false)
        if (useSsl && !connectToLocatorResult.isJmxManagerSslEnabled()) {
          gfsh.logInfo(
              CliStrings.CONNECT__USE_SSL
                  + " is set to true. But JMX Manager doesn't support SSL, connecting without SSL.",
              null);
          useSsl = false;
        }
      }

      if (useSsl) {
        gfsh.logToFile("Connecting to manager via SSL.", null);
      }

      // print out the connecting endpoint
      if (!retry) {
        Gfsh.println(CliStrings.format(CliStrings.CONNECT__MSG__CONNECTING_TO_MANAGER_AT_0,
            new Object[] {jmxHostPortToConnect.toString(false)}));
      }

      InfoResultData infoResultData = ResultBuilder.createInfoResultData();
      JmxOperationInvoker operationInvoker = new JmxOperationInvoker(jmxHostPortToConnect.getHost(),
          jmxHostPortToConnect.getPort(), gfProperties);

      gfsh.setOperationInvoker(operationInvoker);
      infoResultData.addLine(CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS,
          jmxHostPortToConnect.toString(false)));
      LogWrapper.getInstance().info(CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS,
          jmxHostPortToConnect.toString(false)));
      return ResultBuilder.buildResult(infoResultData);
    } catch (SecurityException | AuthenticationFailedException e) {
      // if it's security exception, and we already sent in username and password, still returns the
      // connection error
      if (gfProperties.containsKey(ResourceConstants.USER_NAME)) {
        return handleException(e, jmxHostPortToConnect);
      }

      // otherwise, prompt for username and password and retry the connection
      gfProperties.setProperty(UserInputProperty.USERNAME.getKey(),
          UserInputProperty.USERNAME.promptForAcceptableValue(gfsh));
      gfProperties.setProperty(UserInputProperty.PASSWORD.getKey(),
          UserInputProperty.PASSWORD.promptForAcceptableValue(gfsh));
      return jmxConnect(gfProperties, useSsl, jmxHostPortToConnect, null, true);
    } catch (Exception e) {
      // all other exceptions, just logs it and returns a connection error
      return handleException(e, jmxHostPortToConnect);
    } finally {
      Gfsh.redirectInternalJavaLoggers();
    }
  }

  public static ConnectToLocatorResult connectToLocator(String host, int port, int timeout,
      Properties props) throws IOException, ClassNotFoundException {
    // register DSFID types first; invoked explicitly so that all message type
    // initializations do not happen in first deserialization on a possibly
    // "precious" thread
    DSFIDFactory.registerTypes();

    JmxManagerLocatorResponse locatorResponse =
        JmxManagerLocatorRequest.send(host, port, timeout, props);

    if (StringUtils.isBlank(locatorResponse.getHost()) || locatorResponse.getPort() == 0) {
      Throwable locatorResponseException = locatorResponse.getException();
      String exceptionMessage = CliStrings.CONNECT__MSG__LOCATOR_COULD_NOT_FIND_MANAGER;

      if (locatorResponseException != null) {
        String locatorResponseExceptionMessage = locatorResponseException.getMessage();
        locatorResponseExceptionMessage = (StringUtils.isNotBlank(locatorResponseExceptionMessage)
            ? locatorResponseExceptionMessage : locatorResponseException.toString());
        exceptionMessage = "Exception caused JMX Manager startup to fail because: '"
            .concat(locatorResponseExceptionMessage).concat("'");
      }

      throw new IllegalStateException(exceptionMessage, locatorResponseException);
    }

    ConnectionEndpoint memberEndpoint =
        new ConnectionEndpoint(locatorResponse.getHost(), locatorResponse.getPort());

    String resultMessage = CliStrings.format(CliStrings.CONNECT__MSG__CONNECTING_TO_MANAGER_AT_0,
        memberEndpoint.toString(false));

    return new ConnectToLocatorResult(memberEndpoint, resultMessage,
        locatorResponse.isJmxManagerSslEnabled());
  }

  private KeyManager[] getKeyManagers(SSLConfig sslConfig) throws Exception {
    FileInputStream keyStoreStream = null;
    KeyManagerFactory keyManagerFactory = null;

    try {
      if (StringUtils.isNotBlank(sslConfig.getKeystore())) {
        KeyStore clientKeys = KeyStore.getInstance(sslConfig.getKeystoreType());
        keyStoreStream = new FileInputStream(sslConfig.getKeystore());
        clientKeys.load(keyStoreStream, sslConfig.getKeystorePassword().toCharArray());

        keyManagerFactory =
            KeyManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(clientKeys, sslConfig.getKeystorePassword().toCharArray());
      }
    } finally {
      if (keyStoreStream != null) {
        keyStoreStream.close();
      }
    }

    return keyManagerFactory != null ? keyManagerFactory.getKeyManagers() : null;
  }

  private TrustManager[] getTrustManagers(SSLConfig sslConfig, boolean skipSslVerification)
      throws Exception {
    FileInputStream trustStoreStream = null;
    TrustManagerFactory trustManagerFactory = null;

    if (skipSslVerification) {
      TrustManager[] trustAllCerts = new TrustManager[] {new X509TrustManager() {
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
          return null;
        }

        public void checkClientTrusted(X509Certificate[] certs, String authType) {}

        public void checkServerTrusted(X509Certificate[] certs, String authType) {}

      }};
      return trustAllCerts;
    }

    try {
      // load server public key
      if (StringUtils.isNotBlank(sslConfig.getTruststore())) {
        KeyStore serverPub = KeyStore.getInstance(sslConfig.getTruststoreType());
        trustStoreStream = new FileInputStream(sslConfig.getTruststore());
        serverPub.load(trustStoreStream, sslConfig.getTruststorePassword().toCharArray());
        trustManagerFactory =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(serverPub);
      }
    } finally {
      if (trustStoreStream != null) {
        trustStoreStream.close();
      }
    }
    return trustManagerFactory != null ? trustManagerFactory.getTrustManagers() : null;
  }

  private void configureHttpsURLConnection(SSLConfig sslConfig, boolean skipSslVerification)
      throws Exception {
    KeyManager[] keyManagers = getKeyManagers(sslConfig);
    TrustManager[] trustManagers = getTrustManagers(sslConfig, skipSslVerification);

    if (skipSslVerification) {
      HttpsURLConnection.setDefaultHostnameVerifier((String s, SSLSession sslSession) -> true);
    }

    SSLContext ssl =
        SSLContext.getInstance(SSLUtil.getSSLAlgo(SSLUtil.readArray(sslConfig.getProtocols())));

    ssl.init(keyManagers, trustManagers, new SecureRandom());

    HttpsURLConnection.setDefaultSSLSocketFactory(ssl.getSocketFactory());
  }

  private Result handleException(Exception e) {
    return handleException(e, e.getMessage());
  }

  private Result handleException(Exception e, String errorMessage) {
    LogWrapper.getInstance().severe(errorMessage, e);
    return ResultBuilder.createConnectionErrorResult(errorMessage);
  }

  private Result handleException(Exception e, ConnectionEndpoint hostPortToConnect) {
    if (hostPortToConnect == null) {
      return handleException(e);
    }
    return handleException(e, CliStrings.format(CliStrings.CONNECT__MSG__ERROR,
        hostPortToConnect.toString(false), e.getMessage()));
  }

  private static Properties loadPropertiesFromFile(File propertyFile) {
    try {
      return loadPropertiesFromUrl(propertyFile.toURI().toURL());
    } catch (MalformedURLException e) {
      throw new RuntimeException(
          CliStrings.format("Failed to load configuration properties from pathname (%1$s)!",
              propertyFile.getAbsolutePath()),
          e);
    }
  }

  private static Properties loadPropertiesFromUrl(URL url) {
    Properties properties = new Properties();

    if (url == null) {
      return properties;
    }

    try (InputStream inputStream = url.openStream()) {
      properties.load(inputStream);
    } catch (IOException io) {
      throw new RuntimeException(
          CliStrings.format(CliStrings.CONNECT__MSG__COULD_NOT_READ_CONFIG_FROM_0,
              CliUtil.decodeWithDefaultCharSet(url.getPath())),
          io);
    }

    return properties;
  }
}
