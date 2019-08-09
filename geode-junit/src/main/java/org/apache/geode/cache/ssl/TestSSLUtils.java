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

package org.apache.geode.cache.ssl;

import static java.util.stream.Collectors.toList;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.SubjectKeyIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.crypto.util.PublicKeyFactory;
import org.bouncycastle.crypto.util.SubjectPublicKeyInfoFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;

public class TestSSLUtils {

  public static KeyPair generateKeyPair(String algorithm) throws NoSuchAlgorithmException {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
    keyGen.initialize(2048);
    return keyGen.genKeyPair();
  }

  private static KeyStore createEmptyKeyStore() throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, null); // initialize
    return ks;
  }

  public static void createKeyStore(String filename,
      String password, String alias,
      Key privateKey, Certificate cert, Certificate issuer)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();

    List<Certificate> chain = new ArrayList<>();
    chain.add(cert);
    if (issuer != null) {
      chain.add(issuer);
    }

    ks.setKeyEntry(alias, privateKey, password.toCharArray(), chain.toArray(new Certificate[] {}));
    try (OutputStream out = Files.newOutputStream(Paths.get(filename))) {
      ks.store(out, password.toCharArray());
    }
  }

  public static void createTrustStore(String filename, String password,
      Map<String, X509Certificate> certs)
      throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance("JKS");
    try (InputStream in = Files.newInputStream(Paths.get(filename))) {
      ks.load(in, password.toCharArray());
    } catch (EOFException e) {
      ks = createEmptyKeyStore();
    }
    for (Map.Entry<String, X509Certificate> cert : certs.entrySet()) {
      ks.setCertificateEntry(cert.getKey(), cert.getValue());
    }
    try (OutputStream out = Files.newOutputStream(Paths.get(filename))) {
      ks.store(out, password.toCharArray());
    }
  }

  public static class CertificateBuilder {
    private final int days;
    private final String algorithm;
    private X500Name name;
    private List<String> dnsNames;
    private List<InetAddress> ipAddresses;
    private boolean isCA;
    private X509Certificate issuer;

    public CertificateBuilder() {
      this(30, "SHA256withRSA");
    }

    public CertificateBuilder(int days, String algorithm) {
      this.days = days;
      this.algorithm = algorithm;
      this.dnsNames = new ArrayList<>();
      this.ipAddresses = new ArrayList<>();
    }

    private static GeneralName dnsGeneralName(String name) {
      return new GeneralName(GeneralName.dNSName, name);
    }

    private static GeneralName ipGeneralName(InetAddress hostAddress) {
      return new GeneralName(GeneralName.iPAddress,
          new DEROctetString(hostAddress.getAddress()));
    }

    public CertificateBuilder commonName(String cn) {
      this.name = new X500Name("CN=" + cn + ", O=Geode");
      return this;
    }

    public CertificateBuilder sanDnsName(String hostName) {
      this.dnsNames.add(hostName);
      return this;
    }

    public CertificateBuilder sanIpAddress(InetAddress hostAddress) {
      this.ipAddresses.add(hostAddress);
      return this;
    }

    public CertificateBuilder isCA() {
      this.isCA = true;
      return this;
    }

    public CertificateBuilder issuedBy(X509Certificate issuer) {
      this.issuer = issuer;
      return this;
    }

    private byte[] san() throws IOException {
      List<GeneralName> names = dnsNames.stream()
          .map(CertificateBuilder::dnsGeneralName)
          .collect(toList());

      names.addAll(ipAddresses.stream()
          .map(CertificateBuilder::ipGeneralName)
          .collect(toList()));

      return names.isEmpty() ? null
          : new GeneralNames(names.toArray(new GeneralName[] {})).getEncoded();
    }

    public X509Certificate generate(PublicKey publicKey, PrivateKey privateKey)
        throws CertificateException {
      try {
        AlgorithmIdentifier sigAlgId =
            new DefaultSignatureAlgorithmIdentifierFinder().find(algorithm);
        AlgorithmIdentifier digAlgId = new DefaultDigestAlgorithmIdentifierFinder().find(sigAlgId);
        AsymmetricKeyParameter publicKeyAsymKeyParam =
            PublicKeyFactory.createKey(publicKey.getEncoded());
        AsymmetricKeyParameter privateKeyAsymKeyParam =
            PrivateKeyFactory.createKey(privateKey.getEncoded());
        SubjectPublicKeyInfo subPubKeyInfo =
            SubjectPublicKeyInfo.getInstance(publicKey.getEncoded());

        ContentSigner sigGen =
            new BcRSAContentSignerBuilder(sigAlgId, digAlgId).build(privateKeyAsymKeyParam);

        Date from = new Date();
        Date to = new Date(from.getTime() + days * 86400000L);
        BigInteger sn = new BigInteger(64, new SecureRandom());

        X509v3CertificateBuilder v3CertGen;
        if (issuer == null) {
          // This is a self-signed certificate
          v3CertGen = new X509v3CertificateBuilder(name, sn, from, to, name, subPubKeyInfo);
        } else {
          v3CertGen = new X509v3CertificateBuilder(new X500Name(issuer.getIssuerDN().getName()),
              sn, from, to, name, subPubKeyInfo);
        }

        byte[] subjectAltName = san();
        if (subjectAltName != null) {
          v3CertGen.addExtension(Extension.subjectAlternativeName, false, subjectAltName);
        }

        if (isCA) {
          v3CertGen.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign));
          v3CertGen.addExtension(Extension.basicConstraints, true, new BasicConstraints(0));
        }

        // Not strictly necessary, but this makes the certs look like those generated
        // by `keytool`.
        SubjectKeyIdentifier subjectKeyIdentifier =
            new SubjectKeyIdentifier(
                SubjectPublicKeyInfoFactory.createSubjectPublicKeyInfo(publicKeyAsymKeyParam)
                    .getEncoded());
        v3CertGen.addExtension(Extension.subjectKeyIdentifier, false, subjectKeyIdentifier);

        X509CertificateHolder certificateHolder = v3CertGen.build(sigGen);
        return new JcaX509CertificateConverter()
            .setProvider(new BouncyCastleProvider())
            .getCertificate(certificateHolder);
      } catch (CertificateException ce) {
        throw ce;
      } catch (Exception e) {
        throw new CertificateException(e);
      }
    }
  }
}
