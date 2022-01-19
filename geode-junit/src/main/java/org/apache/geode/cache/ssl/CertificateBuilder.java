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

import java.io.IOException;
import java.io.UncheckedIOException;
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

import sun.security.x509.AlgorithmId;
import sun.security.x509.BasicConstraintsExtension;
import sun.security.x509.CertificateAlgorithmId;
import sun.security.x509.CertificateExtensions;
import sun.security.x509.CertificateSerialNumber;
import sun.security.x509.CertificateValidity;
import sun.security.x509.CertificateVersion;
import sun.security.x509.CertificateX509Key;
import sun.security.x509.DNSName;
import sun.security.x509.GeneralName;
import sun.security.x509.GeneralNames;
import sun.security.x509.IPAddressName;
import sun.security.x509.KeyIdentifier;
import sun.security.x509.KeyUsageExtension;
import sun.security.x509.SubjectAlternativeNameExtension;
import sun.security.x509.SubjectKeyIdentifierExtension;
import sun.security.x509.X500Name;
import sun.security.x509.X509CertImpl;
import sun.security.x509.X509CertInfo;


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

  public CertificateBuilder() {
    this(30, "SHA256withRSA");
  }

  public CertificateBuilder(int days, String algorithm) {
    this.days = days;
    this.algorithm = algorithm;
    dnsNames = new ArrayList<>();
    ipAddresses = new ArrayList<>();
  }

  private static GeneralName dnsGeneralName(String name) {
    try {
      return new GeneralName(new DNSName(name));
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  private static GeneralName ipGeneralName(InetAddress hostAddress) {
    try {
      return new GeneralName(new IPAddressName(hostAddress.getAddress()));
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  public CertificateBuilder commonName(String cn) {
    try {
      name = new X500Name("O=Geode, CN=" + cn);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
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

  private GeneralNames san() throws IOException {
    GeneralNames names = new GeneralNames();
    for (String name : dnsNames) {
      names.add(CertificateBuilder.dnsGeneralName(name));
    }

    for (InetAddress address : ipAddresses) {
      names.add(CertificateBuilder.ipGeneralName(address));
    }

    return names;
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

    CertificateValidity interval = new CertificateValidity(from, to);
    BigInteger sn = new BigInteger(64, new SecureRandom());

    X509CertInfo info = new X509CertInfo();

    try {
      info.set(X509CertInfo.VALIDITY, interval);
      info.set(X509CertInfo.SERIAL_NUMBER, new CertificateSerialNumber(sn));
      info.set(X509CertInfo.SUBJECT, name);
      info.set(X509CertInfo.KEY, new CertificateX509Key(publicKey));
      info.set(X509CertInfo.VERSION, new CertificateVersion(CertificateVersion.V3));
      AlgorithmId algo = new AlgorithmId(AlgorithmId.md5WithRSAEncryption_oid);
      info.set(X509CertInfo.ALGORITHM_ID, new CertificateAlgorithmId(algo));

      if (issuer == null) {
        // This is a self-signed certificate
        info.set(X509CertInfo.ISSUER, name);
      } else {
        info.set(X509CertInfo.ISSUER, issuer.getCertificate().getSubjectDN());
      }

      CertificateExtensions extensions = new CertificateExtensions();

      byte[] keyIdBytes = new KeyIdentifier(publicKey).getIdentifier();
      SubjectKeyIdentifierExtension keyIdentifier = new SubjectKeyIdentifierExtension(keyIdBytes);
      extensions.set(SubjectKeyIdentifierExtension.NAME, keyIdentifier);

      GeneralNames subjectAltNames = san();
      if (!subjectAltNames.isEmpty()) {
        SubjectAlternativeNameExtension altNames =
            new SubjectAlternativeNameExtension(subjectAltNames);
        extensions.set(SubjectAlternativeNameExtension.NAME, altNames);
      }

      if (isCA) {
        KeyUsageExtension usageExtension = new KeyUsageExtension();
        usageExtension.set(KeyUsageExtension.KEY_CERTSIGN, true);
        extensions.set(KeyUsageExtension.NAME, usageExtension);

        BasicConstraintsExtension basicConstraints = new BasicConstraintsExtension(true, 0);
        extensions.set(BasicConstraintsExtension.NAME, basicConstraints);
      }

      if (!extensions.getAllExtensions().isEmpty()) {
        info.set(X509CertInfo.EXTENSIONS, extensions);
      }

      // Sign the cert to identify the algorithm that's used.
      X509CertImpl cert = new X509CertImpl(info);
      cert.sign(privateKey, algorithm);

      // Update the algorithm, and resign.
      algo = (AlgorithmId) cert.get(X509CertImpl.SIG_ALG);
      info.set(CertificateAlgorithmId.NAME + "." + CertificateAlgorithmId.ALGORITHM, algo);
      cert = new X509CertImpl(info);
      cert.sign(privateKey, algorithm);

      return cert;
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
