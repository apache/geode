package org.apache.geode.cache.ssl;

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
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

/**
 * Class which allows easily building certificates. It can also be used to build
 * Certificate Authorities. The class is intended to be used in conjunction with {@link CertStores}
 * to facilitate building key and trust stores.
 */
public class CertificateBuilder {
  private final int days;
  private final String algorithm;
  private X500Name name;
  private List<String> dnsNames;
  private List<InetAddress> ipAddresses;
  private boolean isCA;
  private CertificateMaterial issuer;

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

  public CertificateBuilder issuedBy(CertificateMaterial issuer) {
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
        v3CertGen = new X509v3CertificateBuilder(
            new X500Name(issuer.getCertificate().getIssuerDN().getName()),
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
    } catch (Exception e) {
      throw new RuntimeException("Unable to create certificate", e);
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
