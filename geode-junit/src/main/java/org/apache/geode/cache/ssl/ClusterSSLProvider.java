package org.apache.geode.cache.ssl;

import static org.apache.geode.cache.ssl.TestSSLUtils.CertificateBuilder;
import static org.apache.geode.cache.ssl.TestSSLUtils.createKeyStore;
import static org.apache.geode.cache.ssl.TestSSLUtils.createTrustStore;
import static org.apache.geode.cache.ssl.TestSSLUtils.generateKeyPair;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_TYPE;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ClusterSSLProvider {

  private Map<String, KeyPair> keyPairs = new HashMap<>();
  private Map<String, X509Certificate> certs = new HashMap<>();

  private File locatorKeyStoreFile;
  private File serverKeyStoreFile;
  private File clientKeyStoreFile;

  private String trustStorePassword = "password";
  private String keyStorePassword = "password";

  public ClusterSSLProvider locatorCertificate(CertificateBuilder certificateBuilder)
      throws GeneralSecurityException, IOException {
    locatorKeyStoreFile = File.createTempFile("locatorKS", ".jks");
    certificate("locator", certificateBuilder, locatorKeyStoreFile);
    return this;
  }

  public ClusterSSLProvider serverCertificate(CertificateBuilder certificateBuilder)
      throws GeneralSecurityException, IOException {
    serverKeyStoreFile = File.createTempFile("serverKS", ".jks");
    certificate("server", certificateBuilder, serverKeyStoreFile);
    return this;
  }

  public ClusterSSLProvider clientCertificate(CertificateBuilder certificateBuilder)
      throws GeneralSecurityException, IOException {
    clientKeyStoreFile = File.createTempFile("clientKS", ".jks");
    certificate("client", certificateBuilder, clientKeyStoreFile);
    return this;
  }

  private void certificate(String alias, CertificateBuilder certificateBuilder,
      File keyStoreFile) throws GeneralSecurityException, IOException {
    KeyPair keyPair = generateKeyPair("RSA");
    keyPairs.put(alias, keyPair);

    X509Certificate cert = certificateBuilder.generate(keyPair);
    certs.put(alias, cert);

    createKeyStore(keyStoreFile.getPath(), keyStorePassword, alias, keyPair.getPrivate(), cert);
  }

  public Properties locatorPropertiesWith(String components, String protocols,
      String ciphers)
      throws GeneralSecurityException, IOException {
    File locatorTrustStore = File.createTempFile("locatorTS", ".jks");
    locatorTrustStore.deleteOnExit();

    // locator trusts all
    createTrustStore(locatorTrustStore.getPath(), trustStorePassword, certs);

    return generatePropertiesWith(components, protocols, ciphers, locatorTrustStore,
        locatorKeyStoreFile);
  }

  public Properties serverPropertiesWith(String components, String protocols,
      String ciphers)
      throws GeneralSecurityException, IOException {
    File serverTrustStoreFile = File.createTempFile("serverTS", ".jks");
    serverTrustStoreFile.deleteOnExit();

    // a server should trust itself, locator and client
    createTrustStore(serverTrustStoreFile.getPath(), trustStorePassword, certs);

    return generatePropertiesWith(components, protocols, ciphers, serverTrustStoreFile,
        serverKeyStoreFile);
  }

  public Properties clientPropertiesWith(String components, String protocols,
      String ciphers)
      throws GeneralSecurityException, IOException {
    File clientTrustStoreFile = File.createTempFile("clientTS", ".jks");
    clientTrustStoreFile.deleteOnExit();

    // only trust locator and server
    Map<String, X509Certificate> trustedCerts = new HashMap<>();
    if (certs.containsKey("locator")) {
      trustedCerts.put("locator", certs.get("locator"));
    }
    if (certs.containsKey("server")) {
      trustedCerts.put("server", certs.get("server"));
    }

    createTrustStore(clientTrustStoreFile.getPath(), trustStorePassword, trustedCerts);

    return generatePropertiesWith(components, protocols, ciphers, clientTrustStoreFile,
        clientKeyStoreFile);
  }

  private Properties generatePropertiesWith(String components, String protocols, String ciphers,
      File trustStoreFile, File keyStoreFile) {

    Properties sslConfigs = new Properties();
    sslConfigs.setProperty(SSL_ENABLED_COMPONENTS, components);
    sslConfigs.setProperty(SSL_KEYSTORE, keyStoreFile.getPath());
    sslConfigs.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    sslConfigs.setProperty(SSL_KEYSTORE_PASSWORD, keyStorePassword);
    sslConfigs.setProperty(SSL_TRUSTSTORE, trustStoreFile.getPath());
    sslConfigs.setProperty(SSL_TRUSTSTORE_PASSWORD, trustStorePassword);
    sslConfigs.setProperty(SSL_TRUSTSTORE_TYPE, "JKS");
    sslConfigs.setProperty(SSL_PROTOCOLS, protocols);
    sslConfigs.setProperty(SSL_CIPHERS, ciphers);

    return sslConfigs;
  }
}
