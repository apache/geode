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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.management.internal.cli.shell.Gfsh.SSL_ENABLED_CIPHERS;
import static org.apache.geode.management.internal.cli.shell.Gfsh.SSL_ENABLED_PROTOCOLS;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.lang.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.internal.DSFIDFactory;
import org.apache.geode.internal.lang.Initializer;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.internal.util.PasswordUtil;
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
import org.apache.geode.management.internal.web.domain.LinkIndex;
import org.apache.geode.management.internal.web.http.support.SimpleHttpRequester;
import org.apache.geode.management.internal.web.shell.HttpOperationInvoker;
import org.apache.geode.management.internal.web.shell.RestHttpOperationInvoker;
import org.apache.geode.security.AuthenticationFailedException;

public class ConnectCommand implements GfshCommand {
  // millis that connect --locator will wait for a response from the locator.
  public final static int CONNECT_LOCATOR_TIMEOUT_MS = 60000; // see bug 45971

  @CliCommand(value = {CliStrings.CONNECT}, help = CliStrings.CONNECT__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH, CliStrings.TOPIC_GEODE_JMX,
      CliStrings.TOPIC_GEODE_MANAGER})
  public Result connect(
      @CliOption(key = {CliStrings.CONNECT__LOCATOR},
          unspecifiedDefaultValue = ConnectionEndpointConverter.DEFAULT_LOCATOR_ENDPOINTS,
          optionContext = ConnectionEndpoint.LOCATOR_OPTION_CONTEXT,
          help = CliStrings.CONNECT__LOCATOR__HELP) ConnectionEndpoint locatorTcpHostPort,
      @CliOption(key = {CliStrings.CONNECT__JMX_MANAGER},
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          optionContext = ConnectionEndpoint.JMXMANAGER_OPTION_CONTEXT,
          help = CliStrings.CONNECT__JMX_MANAGER__HELP) ConnectionEndpoint memberRmiHostPort,
      @CliOption(key = {CliStrings.CONNECT__USE_HTTP}, mandatory = false,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false",
          help = CliStrings.CONNECT__USE_HTTP__HELP) boolean useHttp,
      @CliOption(key = {CliStrings.CONNECT__URL}, mandatory = false,
          unspecifiedDefaultValue = CliStrings.CONNECT__DEFAULT_BASE_URL,
          help = CliStrings.CONNECT__URL__HELP) String url,
      @CliOption(key = {CliStrings.CONNECT__USERNAME},
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.CONNECT__USERNAME__HELP) String userName,
      @CliOption(key = {CliStrings.CONNECT__PASSWORD},
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.CONNECT__PASSWORD__HELP) String password,
      @CliOption(key = {CliStrings.CONNECT__KEY_STORE},
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.CONNECT__KEY_STORE__HELP) String keystore,
      @CliOption(key = {CliStrings.CONNECT__KEY_STORE_PASSWORD},
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.CONNECT__KEY_STORE_PASSWORD__HELP) String keystorePassword,
      @CliOption(key = {CliStrings.CONNECT__TRUST_STORE},
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.CONNECT__TRUST_STORE__HELP) String truststore,
      @CliOption(key = {CliStrings.CONNECT__TRUST_STORE_PASSWORD},
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.CONNECT__TRUST_STORE_PASSWORD__HELP) String truststorePassword,
      @CliOption(key = {CliStrings.CONNECT__SSL_CIPHERS},
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.CONNECT__SSL_CIPHERS__HELP) String sslCiphers,
      @CliOption(key = {CliStrings.CONNECT__SSL_PROTOCOLS},
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.CONNECT__SSL_PROTOCOLS__HELP) String sslProtocols,
      @CliOption(key = CliStrings.CONNECT__SECURITY_PROPERTIES,
          optionContext = ConverterHint.FILE_PATH,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.CONNECT__SECURITY_PROPERTIES__HELP) final String gfSecurityPropertiesPath,
      @CliOption(key = {CliStrings.CONNECT__USE_SSL}, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.CONNECT__USE_SSL__HELP) final boolean useSsl) {
    Result result;
    String passwordToUse = PasswordUtil.decrypt(password);
    String keystoreToUse = keystore;
    String keystorePasswordToUse = keystorePassword;
    String truststoreToUse = truststore;
    String truststorePasswordToUse = truststorePassword;
    String sslCiphersToUse = sslCiphers;
    String sslProtocolsToUse = sslProtocols;

    Gfsh gfsh = getGfsh();
    if (gfsh != null && gfsh.isConnectedAndReady()) {
      return ResultBuilder
          .createInfoResult("Already connected to: " + getGfsh().getOperationInvoker().toString());
    }

    Map<String, String> sslConfigProps = null;
    try {
      if (userName != null && userName.length() > 0) {
        if (passwordToUse == null || passwordToUse.length() == 0) {
          passwordToUse = gfsh.readPassword(CliStrings.CONNECT__PASSWORD + ": ");
        }
        if (passwordToUse == null || passwordToUse.length() == 0) {
          return ResultBuilder
              .createConnectionErrorResult(CliStrings.CONNECT__MSG__JMX_PASSWORD_MUST_BE_SPECIFIED);
        }
      }

      sslConfigProps = this.readSSLConfiguration(useSsl, keystoreToUse, keystorePasswordToUse,
          truststoreToUse, truststorePasswordToUse, sslCiphersToUse, sslProtocolsToUse,
          gfSecurityPropertiesPath);
    } catch (IOException e) {
      return handleExcpetion(e, null);
    }

    if (useHttp) {
      result = httpConnect(sslConfigProps, useSsl, url, userName, passwordToUse);
    } else {
      result = jmxConnect(sslConfigProps, memberRmiHostPort, locatorTcpHostPort, useSsl, userName,
          passwordToUse, gfSecurityPropertiesPath, false);
    }

    return result;
  }


  private Result httpConnect(Map<String, String> sslConfigProps, boolean useSsl, String url,
      String userName, String passwordToUse) {
    Gfsh gfsh = getGfsh();
    try {
      Map<String, String> securityProperties = new HashMap<String, String>();

      // at this point, if userName is not empty, password should not be empty either
      if (userName != null && userName.length() > 0) {
        securityProperties.put("security-username", userName);
        securityProperties.put("security-password", passwordToUse);
      }

      if (useSsl) {
        configureHttpsURLConnection(sslConfigProps);
        if (url.startsWith("http:")) {
          url = url.replace("http:", "https:");
        }
      }

      Iterator<String> it = sslConfigProps.keySet().iterator();
      while (it.hasNext()) {
        String secKey = it.next();
        securityProperties.put(secKey, sslConfigProps.get(secKey));
      }

      // This is so that SSL termination results in https URLs being returned
      String query = (url.startsWith("https")) ? "?scheme=https" : "";

      LogWrapper.getInstance().warning(String.format(
          "Sending HTTP request for Link Index at (%1$s)...", url.concat("/index").concat(query)));

      LinkIndex linkIndex =
          new SimpleHttpRequester(gfsh, CONNECT_LOCATOR_TIMEOUT_MS, securityProperties)
              .exchange(url.concat("/index").concat(query), LinkIndex.class);

      LogWrapper.getInstance()
          .warning(String.format("Received Link Index (%1$s)", linkIndex.toString()));

      HttpOperationInvoker operationInvoker =
          new RestHttpOperationInvoker(linkIndex, gfsh, url, securityProperties);

      Initializer.init(operationInvoker);
      gfsh.setOperationInvoker(operationInvoker);

      LogWrapper.getInstance()
          .info(CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS, operationInvoker.toString()));
      return ResultBuilder.createInfoResult(
          CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS, operationInvoker.toString()));

    } catch (Exception e) {
      // all other exceptions, just logs it and returns a connection error
      if (!(e instanceof SecurityException) && !(e instanceof AuthenticationFailedException)) {
        return handleExcpetion(e, null);
      }

      // if it's security exception, and we already sent in username and password, still retuns the
      // connection error
      if (userName != null) {
        return handleExcpetion(e, null);
      }

      // otherwise, prompt for username and password and retry the conenction
      try {
        userName = gfsh.readText(CliStrings.CONNECT__USERNAME + ": ");
        passwordToUse = gfsh.readPassword(CliStrings.CONNECT__PASSWORD + ": ");
        return httpConnect(sslConfigProps, useSsl, url, userName, passwordToUse);
      } catch (IOException ioe) {
        return handleExcpetion(ioe, null);
      }
    } finally {
      Gfsh.redirectInternalJavaLoggers();
    }
  }

  private Result jmxConnect(Map<String, String> sslConfigProps,
      ConnectionEndpoint memberRmiHostPort, ConnectionEndpoint locatorTcpHostPort, boolean useSsl,
      String userName, String passwordToUse, String gfSecurityPropertiesPath, boolean retry) {
    ConnectionEndpoint hostPortToConnect = null;
    Gfsh gfsh = getGfsh();

    try {

      // trying to find the hostPortToConnect, if rmi host port exists, use that, otherwise, use
      // locator to find the rmi host port
      if (memberRmiHostPort != null) {
        hostPortToConnect = memberRmiHostPort;
      } else {
        // Props required to configure a SocketCreator with SSL.
        // Used for gfsh->locator connection & not needed for gfsh->manager connection
        if (useSsl || !sslConfigProps.isEmpty()) {
          sslConfigProps.put(MCAST_PORT, String.valueOf(0));
          sslConfigProps.put(LOCATORS, "");

          String sslInfoLogMsg = "Connecting to Locator via SSL.";
          if (useSsl) {
            sslInfoLogMsg = CliStrings.CONNECT__USE_SSL + " is set to true. " + sslInfoLogMsg;
          }
          gfsh.logToFile(sslInfoLogMsg, null);
        }

        Gfsh.println(CliStrings.format(CliStrings.CONNECT__MSG__CONNECTING_TO_LOCATOR_AT_0,
            new Object[] {locatorTcpHostPort.toString(false)}));
        ConnectToLocatorResult connectToLocatorResult =
            connectToLocator(locatorTcpHostPort.getHost(), locatorTcpHostPort.getPort(),
                CONNECT_LOCATOR_TIMEOUT_MS, sslConfigProps);
        hostPortToConnect = connectToLocatorResult.getMemberEndpoint();

        // when locator is configured to use SSL (ssl-enabled=true) but manager is not
        // (jmx-manager-ssl=false)
        if ((useSsl || !sslConfigProps.isEmpty())
            && !connectToLocatorResult.isJmxManagerSslEnabled()) {
          gfsh.logInfo(
              CliStrings.CONNECT__USE_SSL
                  + " is set to true. But JMX Manager doesn't support SSL, connecting without SSL.",
              null);
          sslConfigProps.clear();
        }
      }

      if (!sslConfigProps.isEmpty()) {
        gfsh.logToFile("Connecting to manager via SSL.", null);
      }

      // print out the connecting endpoint
      if (!retry) {
        Gfsh.println(CliStrings.format(CliStrings.CONNECT__MSG__CONNECTING_TO_MANAGER_AT_0,
            new Object[] {hostPortToConnect.toString(false)}));
      }

      InfoResultData infoResultData = ResultBuilder.createInfoResultData();
      JmxOperationInvoker operationInvoker =
          new JmxOperationInvoker(hostPortToConnect.getHost(), hostPortToConnect.getPort(),
              userName, passwordToUse, sslConfigProps, gfSecurityPropertiesPath);

      gfsh.setOperationInvoker(operationInvoker);
      infoResultData.addLine(
          CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS, hostPortToConnect.toString(false)));
      LogWrapper.getInstance().info(
          CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS, hostPortToConnect.toString(false)));
      return ResultBuilder.buildResult(infoResultData);
    } catch (Exception e) {
      // all other exceptions, just logs it and returns a connection error
      if (!(e instanceof SecurityException) && !(e instanceof AuthenticationFailedException)) {
        return handleExcpetion(e, hostPortToConnect);
      }

      // if it's security exception, and we already sent in username and password, still returns the
      // connection error
      if (userName != null) {
        return handleExcpetion(e, hostPortToConnect);
      }

      // otherwise, prompt for username and password and retry the conenction
      try {
        userName = gfsh.readText(CliStrings.CONNECT__USERNAME + ": ");
        passwordToUse = gfsh.readPassword(CliStrings.CONNECT__PASSWORD + ": ");
        // GEODE-2250 If no value for both username and password, at this point we need to error to
        // avoid a stack overflow.
        if (userName == null && passwordToUse == null)
          return handleExcpetion(e, hostPortToConnect);
        return jmxConnect(sslConfigProps, hostPortToConnect, null, useSsl, userName, passwordToUse,
            gfSecurityPropertiesPath, true);
      } catch (IOException ioe) {
        return handleExcpetion(ioe, hostPortToConnect);
      }
    } finally {
      Gfsh.redirectInternalJavaLoggers();
    }
  }

  /**
   * Common code to read SSL information. Used by JMX, Locator & HTTP mode connect
   */
  private Map<String, String> readSSLConfiguration(boolean useSsl, String keystoreToUse,
      String keystorePasswordToUse, String truststoreToUse, String truststorePasswordToUse,
      String sslCiphersToUse, String sslProtocolsToUse, String gfSecurityPropertiesPath)
      throws IOException {

    Gfsh gfshInstance = getGfsh();
    final Map<String, String> sslConfigProps = new LinkedHashMap<String, String>();

    // JMX SSL Config 1:
    // First from gfsecurity properties file if it's specified OR
    // if the default gfsecurity.properties exists useSsl==true
    if (useSsl || gfSecurityPropertiesPath != null) {
      // reference to hold resolved gfSecurityPropertiesPath
      String gfSecurityPropertiesPathToUse = CliUtil.resolvePathname(gfSecurityPropertiesPath);
      URL gfSecurityPropertiesUrl = null;

      // Case 1: User has specified gfSecurity properties file
      if (StringUtils.isNotBlank(gfSecurityPropertiesPathToUse)) {
        // User specified gfSecurity properties doesn't exist
        if (!IOUtils.isExistingPathname(gfSecurityPropertiesPathToUse)) {
          gfshInstance
              .printAsSevere(CliStrings.format(CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE,
                  "Security ", gfSecurityPropertiesPathToUse));
        } else {
          gfSecurityPropertiesUrl = new File(gfSecurityPropertiesPathToUse).toURI().toURL();
        }
      } else if (useSsl && gfSecurityPropertiesPath == null) {
        // Case 2: User has specified to useSsl but hasn't specified
        // gfSecurity properties file. Use default "gfsecurity.properties"
        // in current dir, user's home or classpath
        gfSecurityPropertiesUrl = ShellCommands.getFileUrl("gfsecurity.properties");
      }
      // if 'gfSecurityPropertiesPath' OR gfsecurity.properties has resolvable path
      if (gfSecurityPropertiesUrl != null) {
        gfshInstance.logToFile("Using security properties file : "
            + CliUtil.decodeWithDefaultCharSet(gfSecurityPropertiesUrl.getPath()), null);
        Map<String, String> gfsecurityProps =
            ShellCommands.loadPropertiesFromURL(gfSecurityPropertiesUrl);
        // command line options (if any) would override props in gfsecurity.properties
        sslConfigProps.putAll(gfsecurityProps);
      }
    }

    int numTimesPrompted = 0;
    /*
     * Using do-while here for a case when --use-ssl=true is specified but no SSL options were
     * specified & there was no gfsecurity properties specified or readable in default gfsh
     * directory.
     *
     * NOTE: 2nd round of prompting is done only when sslConfigProps map is empty & useSsl is true -
     * so we won't over-write any previous values.
     */
    do {
      // JMX SSL Config 2: Now read the options
      if (numTimesPrompted > 0) {
        Gfsh.println("Please specify these SSL Configuration properties: ");
      }

      if (numTimesPrompted > 0) {
        // NOTE: sslConfigProps map was empty
        keystoreToUse = gfshInstance.readText(CliStrings.CONNECT__KEY_STORE + ": ");
      }
      if (keystoreToUse != null && keystoreToUse.length() > 0) {
        if (keystorePasswordToUse == null || keystorePasswordToUse.length() == 0) {
          // Check whether specified in gfsecurity props earlier
          keystorePasswordToUse = sslConfigProps.get(SSL_KEYSTORE_PASSWORD);
          if (keystorePasswordToUse == null || keystorePasswordToUse.length() == 0) {
            // not even in properties file, prompt user for it
            keystorePasswordToUse =
                gfshInstance.readPassword(CliStrings.CONNECT__KEY_STORE_PASSWORD + ": ");
            sslConfigProps.put(SSL_KEYSTORE_PASSWORD, keystorePasswordToUse);
          }
        } else {// For cases where password is already part of command option
          sslConfigProps.put(SSL_KEYSTORE_PASSWORD, keystorePasswordToUse);
        }
        sslConfigProps.put(SSL_KEYSTORE, keystoreToUse);
      }

      if (numTimesPrompted > 0) {
        truststoreToUse = gfshInstance.readText(CliStrings.CONNECT__TRUST_STORE + ": ");
      }
      if (truststoreToUse != null && truststoreToUse.length() > 0) {
        if (truststorePasswordToUse == null || truststorePasswordToUse.length() == 0) {
          // Check whether specified in gfsecurity props earlier?
          truststorePasswordToUse = sslConfigProps.get(SSL_TRUSTSTORE_PASSWORD);
          if (truststorePasswordToUse == null || truststorePasswordToUse.length() == 0) {
            // not even in properties file, prompt user for it
            truststorePasswordToUse =
                gfshInstance.readPassword(CliStrings.CONNECT__TRUST_STORE_PASSWORD + ": ");
            sslConfigProps.put(SSL_TRUSTSTORE_PASSWORD, truststorePasswordToUse);
          }
        } else {// For cases where password is already part of command option
          sslConfigProps.put(SSL_TRUSTSTORE_PASSWORD, truststorePasswordToUse);
        }
        sslConfigProps.put(SSL_TRUSTSTORE, truststoreToUse);
      }

      if (numTimesPrompted > 0) {
        sslCiphersToUse = gfshInstance.readText(CliStrings.CONNECT__SSL_CIPHERS + ": ");
      }
      if (sslCiphersToUse != null && sslCiphersToUse.length() > 0) {
        // sslConfigProps.put(DistributionConfig.CLUSTER_SSL_CIPHERS_NAME, sslCiphersToUse);
        sslConfigProps.put(SSL_ENABLED_CIPHERS, sslCiphersToUse);
      }

      if (numTimesPrompted > 0) {
        sslProtocolsToUse = gfshInstance.readText(CliStrings.CONNECT__SSL_PROTOCOLS + ": ");
      }
      if (sslProtocolsToUse != null && sslProtocolsToUse.length() > 0) {
        // sslConfigProps.put(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, sslProtocolsToUse);
        sslConfigProps.put(SSL_ENABLED_PROTOCOLS, sslProtocolsToUse);
      }

      // SSL is required to be used but no SSL config found
    } while (useSsl && sslConfigProps.isEmpty() && (0 == numTimesPrompted++)
        && !gfshInstance.isQuietMode());
    return sslConfigProps;
  }


  public static ConnectToLocatorResult connectToLocator(String host, int port, int timeout,
      Map<String, String> props) throws IOException {
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

  private void configureHttpsURLConnection(Map<String, String> sslConfigProps) throws Exception {
    String keystoreToUse = sslConfigProps.get(SSL_KEYSTORE);
    String keystorePasswordToUse = sslConfigProps.get(SSL_KEYSTORE_PASSWORD);
    String truststoreToUse = sslConfigProps.get(SSL_TRUSTSTORE);
    String truststorePasswordToUse = sslConfigProps.get(SSL_TRUSTSTORE_PASSWORD);
    // Ciphers are not passed to HttpsURLConnection. Could not find a clean way
    // to pass this attribute to socket layer (see #51645)
    String sslCiphersToUse = sslConfigProps.get(SSL_CIPHERS);
    String sslProtocolsToUse = sslConfigProps.get(SSL_PROTOCOLS);

    // Commenting the code to set cipher suites in GFSH rest connect (see #51645)
    /*
     * if(sslCiphersToUse != null){ System.setProperty("https.cipherSuites", sslCiphersToUse); }
     */
    FileInputStream keyStoreStream = null;
    FileInputStream trustStoreStream = null;
    try {

      KeyManagerFactory keyManagerFactory = null;
      if (StringUtils.isNotBlank(keystoreToUse)) {
        KeyStore clientKeys = KeyStore.getInstance("JKS");
        keyStoreStream = new FileInputStream(keystoreToUse);
        clientKeys.load(keyStoreStream, keystorePasswordToUse.toCharArray());

        keyManagerFactory =
            KeyManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(clientKeys, keystorePasswordToUse.toCharArray());
      }

      // load server public key
      TrustManagerFactory trustManagerFactory = null;
      if (StringUtils.isNotBlank(truststoreToUse)) {
        KeyStore serverPub = KeyStore.getInstance("JKS");
        trustStoreStream = new FileInputStream(truststoreToUse);
        serverPub.load(trustStoreStream, truststorePasswordToUse.toCharArray());
        trustManagerFactory =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(serverPub);
      }

      SSLContext ssl =
          SSLContext.getInstance(SSLUtil.getSSLAlgo(SSLUtil.readArray(sslProtocolsToUse)));

      ssl.init(keyManagerFactory != null ? keyManagerFactory.getKeyManagers() : null,
          trustManagerFactory != null ? trustManagerFactory.getTrustManagers() : null,
          new java.security.SecureRandom());

      HttpsURLConnection.setDefaultSSLSocketFactory(ssl.getSocketFactory());
    } finally {
      if (keyStoreStream != null) {
        keyStoreStream.close();
      }
      if (trustStoreStream != null) {
        trustStoreStream.close();
      }

    }


  }


  private Result handleExcpetion(Exception e, ConnectionEndpoint hostPortToConnect) {
    String errorMessage = e.getMessage();
    if (hostPortToConnect != null) {
      errorMessage = CliStrings.format(CliStrings.CONNECT__MSG__ERROR,
          hostPortToConnect.toString(false), e.getMessage());
    }
    LogWrapper.getInstance().severe(errorMessage, e);
    return ResultBuilder.createConnectionErrorResult(errorMessage);
  }

}
