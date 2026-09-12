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

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.security.auth.x500.X500Principal;

import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;


/**
 * Class which allows easily building certificates. It can also be used to build
 * Certificate Authorities. The class is intended to be used in conjunction with {@link CertStores}
 * to facilitate building key and trust stores.
 */
public class CertificateBuilder {
  private final int days;
  private final String algorithm;
  private X500Name name;
  private final List<String> dnsNames;
  private final List<InetAddress> ipAddresses;
  private boolean isCA;
  private CertificateMaterial issuer;
  private final List<KeyPurposeId> extendedKeyUsages;

  public CertificateBuilder() {
    this(30, "SHA256withRSA");
  }

  public CertificateBuilder(int days, String algorithm) {
    this.days = days;
    this.algorithm = algorithm;
    dnsNames = new ArrayList<>();
    ipAddresses = new ArrayList<>();
    extendedKeyUsages = new ArrayList<>();
  }

  private static GeneralName dnsGeneralName(String name) {
    return new GeneralName(GeneralName.dNSName, name);
  }

  private static GeneralName ipGeneralName(InetAddress hostAddress) {
    return new GeneralName(GeneralName.iPAddress, new DEROctetString(hostAddress.getAddress()));
  }

  public CertificateBuilder commonName(String cn) {
    name = X500Name.getInstance(new X500Principal("O=Geode, CN=" + cn).getEncoded());
    return this;
  }

  public CertificateBuilder sanDnsName(String hostName) {
    dnsNames.add(hostName);
    return this;
  }

  public CertificateBuilder sanIpAddress(InetAddress hostAddress) {
    ipAddresses.add(hostAddress);
    return this;
  }

  public CertificateBuilder sanIpAddress(String address) {
    try {
      ipAddresses.add(InetAddress.getByName(address));
    } catch (UnknownHostException ex) {
      throw new RuntimeException(ex);
    }
    return this;
  }

  public CertificateBuilder isCA() {
    isCA = true;
    return this;
  }

  public CertificateBuilder issuedBy(CertificateMaterial issuer) {
    this.issuer = issuer;
    return this;
  }

  /**
   * Add Extended Key Usage purposes to the certificate.
   * Common purposes:
   * - "1.3.6.1.5.5.7.3.1" = serverAuth (TLS Web Server Authentication)
   * - "1.3.6.1.5.5.7.3.2" = clientAuth (TLS Web Client Authentication)
   * - "1.3.6.1.5.5.7.3.3" = codeSigning
   */
  public CertificateBuilder extendedKeyUsage(String... oids) {
    for (String oid : oids) {
      extendedKeyUsages.add(KeyPurposeId.getInstance(new ASN1ObjectIdentifier(oid)));
    }
    return this;
  }

  /**
   * Add TLS Web Client Authentication Extended Key Usage (for client certificates).
   */
  public CertificateBuilder clientAuthEKU() {
    return extendedKeyUsage("1.3.6.1.5.5.7.3.2");
  }

  /**
   * Add TLS Web Server Authentication Extended Key Usage (for server certificates).
   */
  public CertificateBuilder serverAuthEKU() {
    return extendedKeyUsage("1.3.6.1.5.5.7.3.1");
  }

  private GeneralNames san() {
    List<GeneralName> names = new ArrayList<>();
    for (String name : dnsNames) {
      names.add(CertificateBuilder.dnsGeneralName(name));
    }

    for (InetAddress address : ipAddresses) {
      names.add(CertificateBuilder.ipGeneralName(address));
    }

    return new GeneralNames(names.toArray(new GeneralName[0]));
  }

  public CertificateMaterial generate() {
    KeyPair keyPair = generateKeyPair("RSA");
    PrivateKey privateKey;
    X509Certificate issuerCertificate = null;

    if (issuer == null) {
      privateKey = keyPair.getPrivate();
    } else {
      privateKey = issuer.getPrivateKey();
    }

    X509Certificate cert = generate(keyPair.getPublic(), privateKey);

    if (issuer != null) {
      issuerCertificate = issuer.getCertificate();
    }

    return new CertificateMaterial(cert, keyPair, issuerCertificate);
  }

  private X509Certificate generate(PublicKey publicKey, PrivateKey privateKey) {
    Date from = new Date();
    Date to = new Date(from.getTime() + days * 86_400_000L);

    BigInteger sn = new BigInteger(64, new SecureRandom());

    try {
      X500Name issuerName;
      if (issuer == null) {
        // This is a self-signed certificate
        issuerName = name;
      } else {
        issuerName =
            X500Name.getInstance(issuer.getCertificate().getSubjectX500Principal().getEncoded());
      }

      X509v3CertificateBuilder builder =
          new JcaX509v3CertificateBuilder(issuerName, sn, from, to, name, publicKey);

      builder.addExtension(Extension.subjectKeyIdentifier, false,
          new JcaX509ExtensionUtils().createSubjectKeyIdentifier(publicKey));

      GeneralNames subjectAltNames = san();
      if (subjectAltNames.getNames().length > 0) {
        builder.addExtension(Extension.subjectAlternativeName, false, subjectAltNames);
      }

      if (isCA) {
        builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign));
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(0));
      }

      if (!extendedKeyUsages.isEmpty()) {
        builder.addExtension(Extension.extendedKeyUsage, false,
            new ExtendedKeyUsage(extendedKeyUsages.toArray(new KeyPurposeId[0])));
      }

      return new JcaX509CertificateConverter()
          .getCertificate(builder.build(new JcaContentSignerBuilder(algorithm).build(privateKey)));
    } catch (Exception ex) {
      throw new RuntimeException("Unable to create certificate", ex);
    }
  }

  private KeyPair generateKeyPair(String algorithm) {
    try {
      KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
      keyGen.initialize(2048);
      return keyGen.genKeyPair();
    } catch (NoSuchAlgorithmException nex) {
      throw new RuntimeException("Unable to generate " + algorithm + " keypair");
    }
  }
}
