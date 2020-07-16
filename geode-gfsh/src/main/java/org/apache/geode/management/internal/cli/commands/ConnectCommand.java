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
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.SSLUtil;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.JmxManagerLocatorRequest;
import org.apache.geode.management.internal.JmxManagerLocatorResponse;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.converters.ConnectionEndpointConverter;
import org.apache.geode.management.internal.cli.domain.ConnectToLocatorResult;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.JmxOperationInvoker;
import org.apache.geode.management.internal.cli.shell.OperationInvoker;
import org.apache.geode.management.internal.cli.util.ConnectionEndpoint;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.management.internal.web.shell.HttpOperationInvoker;
import org.apache.geode.security.AuthenticationFailedException;

public class ConnectCommand extends OfflineGfshCommand {
  // millis that connect --locator will wait for a response from the locator.
  static final int CONNECT_LOCATOR_TIMEOUT_MS = 60000; // see bug 45971

  private static final int VERSION_MAJOR = 0;
  private static final int VERSION_MINOR = 1;

  @Immutable
  private static final UserInputProperty[] USER_INPUT_PROPERTIES =
      {UserInputProperty.KEYSTORE, UserInputProperty.KEYSTORE_PASSWORD,
          UserInputProperty.KEYSTORE_TYPE, UserInputProperty.TRUSTSTORE,
          UserInputProperty.TRUSTSTORE_PASSWORD, UserInputProperty.TRUSTSTORE_TYPE,
          UserInputProperty.CIPHERS, UserInputProperty.PROTOCOL, UserInputProperty.COMPONENT};

  @CliCommand(value = {CliStrings.CONNECT}, help = CliStrings.CONNECT__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = {CliStrings.TOPIC_GFSH, CliStrings.TOPIC_GEODE_JMX,
      CliStrings.TOPIC_GEODE_MANAGER})
  public ResultModel connect(
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
      @CliOption(key = {CliStrings.CONNECT__URL}, help = CliStrings.CONNECT__URL__HELP) String url,
      @CliOption(key = {CliStrings.CONNECT__USERNAME}, specifiedDefaultValue = "",
          help = CliStrings.CONNECT__USERNAME__HELP) String userName,
      @CliOption(key = {CliStrings.CONNECT__PASSWORD}, specifiedDefaultValue = "",
          help = CliStrings.CONNECT__PASSWORD__HELP) String password,
      @CliOption(key = {CliStrings.CONNECT__TOKEN}, specifiedDefaultValue = "",
          help = CliStrings.CONNECT__TOKEN__HELP) String token,
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

    ResultModel result = new ResultModel();
    Gfsh gfsh = getGfsh();

    // bail out if gfsh is already connected.
    if (gfsh != null && gfsh.isConnectedAndReady()) {
      return ResultModel
          .createInfo("Already connected to: " + getGfsh().getOperationInvoker().toString());
    }

    if (StringUtils.startsWith(url, "https")) {
      useSsl = true;
    }

    if ("".equals(token)) {
      return ResultModel.createError("--token requires a value, for example --token=foo");
    }

    if (token != null && (userName != null || password != null)) {
      return ResultModel.createError("--token cannot be combined with --user or --password");
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
    if (userName != null && !"".equals(userName)) {
      gfProperties.setProperty(ResourceConstants.USER_NAME, userName);
      if (password == null || "".equals(password)) {
        password = UserInputProperty.PASSWORD.promptForAcceptableValue(gfsh);
      }
      gfProperties.setProperty(UserInputProperty.PASSWORD.getKey(), password);
    } else if (token != null) {
      gfProperties.setProperty(ResourceConstants.TOKEN, token);
    }

    if (StringUtils.isNotEmpty(url)) {
      result = httpConnect(gfProperties, url, skipSslValidation);
    } else {
      result = jmxConnect(gfProperties, useSsl, jmxManagerEndPoint, locatorEndPoint, false);
    }

    OperationInvoker invoker = gfsh.getOperationInvoker();
    if (invoker == null || !invoker.isConnected()) {
      return result;
    }

    // since 1.14, only allow gfsh to connect to cluster that's older than 1.10
    String remoteVersion = null;
    String gfshVersion = gfsh.getVersion();
    try {
      remoteVersion = invoker.getRemoteVersion();
      int minorVersion = Integer.parseInt(versionComponent(remoteVersion, VERSION_MINOR));
      if (versionComponent(remoteVersion, VERSION_MAJOR).equals("1") && minorVersion >= 10 ||
          versionComponent(remoteVersion, VERSION_MAJOR).equals("9") && minorVersion >= 9) {
        InfoResultModel versionInfo = result.addInfo("versionInfo");
        versionInfo.addLine("You are connected to a cluster of version: " + remoteVersion);
        return result;
      }
    } catch (Exception ex) {
      // if unable to get the remote version, we are certainly talking to
      // a pre-1.5 cluster
      gfsh.logInfo("failed to get the the remote version.", ex);
    }

    // will reach here only when remoteVersion is not available or does not match
    invoker.stop();
    if (remoteVersion == null) {
      return ResultModel.createError(
          String.format("Cannot use a %s gfsh client to connect to this cluster.", gfshVersion));
    } else {
      return ResultModel.createError(String.format(
          "Cannot use a %s gfsh client to connect to a %s cluster.", gfshVersion, remoteVersion));
    }
  }

  private static String versionComponent(String version, int component) {
    String[] versionComponents = StringUtils.split(version, '.');
    return versionComponents.length >= component + 1 ? versionComponents[component] : "";
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

  @SuppressWarnings("deprecation")
  static boolean containsLegacySSLConfig(Properties properties) {
    return properties.stringPropertyNames().stream()
        .anyMatch(key -> key.startsWith(CLUSTER_SSL_PREFIX)
            || key.startsWith(JMX_MANAGER_SSL_PREFIX) || key.startsWith(HTTP_SERVICE_SSL_PREFIX));
  }

  private static boolean containsSSLConfig(Properties properties) {
    return properties.stringPropertyNames().stream().anyMatch(key -> key.startsWith("ssl-"));
  }


  ResultModel httpConnect(Properties gfProperties, String url, boolean skipSslVerification) {
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

      LogWrapper.getInstance().info(
          CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS, operationInvoker.toString()));
      return ResultModel.createInfo(
          CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS, operationInvoker.toString()));

    } catch (SecurityException | AuthenticationFailedException e) {
      // if it's security exception, and we already sent in username and password, still returns the
      // connection error
      if (gfProperties.containsKey(ResourceConstants.USER_NAME)
          || gfProperties.containsKey(ResourceConstants.TOKEN)) {
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
    }
  }

  ResultModel jmxConnect(Properties gfProperties, boolean useSsl,
      ConnectionEndpoint memberRmiHostPort,
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

      ResultModel result = new ResultModel();
      InfoResultModel infoResultModel = result.addInfo();
      JmxOperationInvoker operationInvoker = new JmxOperationInvoker(jmxHostPortToConnect.getHost(),
          jmxHostPortToConnect.getPort(), gfProperties);

      gfsh.setOperationInvoker(operationInvoker);
      infoResultModel.addLine(CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS,
          jmxHostPortToConnect.toString(false)));
      LogWrapper.getInstance().info(CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS,
          jmxHostPortToConnect.toString(false)));
      return result;
    } catch (SecurityException | AuthenticationFailedException e) {
      // if it's security exception, and we already sent in username and password, still returns the
      // connection error
      if (gfProperties.containsKey(ResourceConstants.USER_NAME)
          || gfProperties.containsKey(ResourceConstants.TOKEN)) {
        return handleException(e, jmxHostPortToConnect);
      }

      // otherwise, prompt for username and password and retry the connection
      gfProperties.setProperty(UserInputProperty.USERNAME.getKey(),
          UserInputProperty.USERNAME.promptForAcceptableValue(gfsh));
      gfProperties.setProperty(UserInputProperty.PASSWORD.getKey(),
          UserInputProperty.PASSWORD.promptForAcceptableValue(gfsh));
      return jmxConnect(gfProperties, useSsl, jmxHostPortToConnect, null, true);
    } catch (UnknownHostException e) {
      return handleException(e,
          "JMX manager can't be reached. Hostname or IP address could not be found.");
    } catch (Exception e) {
      // all other exceptions, just logs it and returns a connection error
      return handleException(e, jmxHostPortToConnect);
    }
  }

  public static ConnectToLocatorResult connectToLocator(String host, int port, int timeout,
      Properties props) throws IOException, ClassNotFoundException {
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



  private void configureHttpsURLConnection(SSLConfig sslConfig, boolean skipSslVerification) {
    SSLContext ssl = SSLUtil.createAndConfigureSSLContext(sslConfig, skipSslVerification);
    if (skipSslVerification) {
      HttpsURLConnection.setDefaultHostnameVerifier((String s, SSLSession sslSession) -> true);
    }
    HttpsURLConnection.setDefaultSSLSocketFactory(ssl.getSocketFactory());
  }

  private ResultModel handleException(Exception e) {
    return handleException(e, e.getMessage());
  }

  private ResultModel handleException(Exception e, String errorMessage) {
    LogWrapper.getInstance().severe(errorMessage, e);
    return ResultModel.createError(errorMessage);
  }

  private ResultModel handleException(Exception e, ConnectionEndpoint hostPortToConnect) {
    if (hostPortToConnect == null) {
      return handleException(e);
    }
    return handleException(e, CliStrings.format(CliStrings.CONNECT__MSG__ERROR,
        hostPortToConnect.toString(false), e.getMessage()));
  }



}
