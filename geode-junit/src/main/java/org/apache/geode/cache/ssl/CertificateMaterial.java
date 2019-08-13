package org.apache.geode.cache.ssl;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.util.Optional;

/**
 * Class which encapsulates a {@link X509Certificate} as well as the associated
 * {@link KeyPair}. If the certificate is not self-signed it will also hold an issuer.
 * <p/>
 * {@code CertificateMaterial} is produced by {@link CertificateBuilder}s.
 *
 * @see CertificateBuilder
 * @see CertStores
 */
public class CertificateMaterial {
  private final X509Certificate certificate;
  private final KeyPair keyPair;
  private final Optional<X509Certificate> issuer;

  public CertificateMaterial(X509Certificate certificate, KeyPair keyPair, X509Certificate issuer) {
    this.certificate = certificate;
    this.keyPair = keyPair;
    this.issuer = Optional.ofNullable(issuer);
  }

  public X509Certificate getCertificate() {
    return certificate;
  }

  public PublicKey getPublicKey() {
    return keyPair.getPublic();
  }

  public PrivateKey getPrivateKey() {
    return keyPair.getPrivate();
  }

  public Optional<X509Certificate> getIssuer() {
    return issuer;
  }
}
