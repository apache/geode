/*=========================================================================
* This implementation is provided on an "AS IS" BASIS,  WITHOUT WARRANTIES
* OR CONDITIONS OF ANY KIND, either express or implied."
*==========================================================================
*/

package templates.security;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.Authenticator;
import com.gemstone.gemfire.security.GemFireSecurityException;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.Signature;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author kneeraj
 * 
 */
public class PKCSAuthenticator implements Authenticator {

  public static final String PUBLIC_KEY_FILE = "security-publickey-filepath";

  public static final String PUBLIC_KEYSTORE_PASSWORD = "security-publickey-pass";

  private String pubKeyFilePath;

  private String pubKeyPass;

  private Map aliasCertificateMap;

  protected LogWriter systemlog;

  protected LogWriter securitylog;

  public static Authenticator create() {
    return new PKCSAuthenticator();
  }

  public PKCSAuthenticator() {
  }

  private void populateMap() {
    try {
      KeyStore ks = KeyStore.getInstance("JKS");
      char[] passPhrase = (pubKeyPass != null ? pubKeyPass.toCharArray() : null);
      FileInputStream keystorefile = new FileInputStream(this.pubKeyFilePath);
      try {
        ks.load(keystorefile, passPhrase);
      }
      finally {
        keystorefile.close();
      }
      Enumeration e = ks.aliases();
      while (e.hasMoreElements()) {
        Object alias = e.nextElement();
        Certificate cert = ks.getCertificate((String)alias);
        if (cert instanceof X509Certificate) {
          this.aliasCertificateMap.put(alias, cert);
        }
      }
    }
    catch (Exception e) {
      throw new AuthenticationFailedException(
          "Exception while getting public keys: " + e.getMessage());
    }
  }

  public void init(Properties systemProps, LogWriter systemLogger,
      LogWriter securityLogger) throws AuthenticationFailedException {
    this.systemlog = systemLogger;
    this.securitylog = securityLogger;
    this.pubKeyFilePath = systemProps.getProperty(PUBLIC_KEY_FILE);
    if (this.pubKeyFilePath == null) {
      throw new AuthenticationFailedException("PKCSAuthenticator: property "
          + PUBLIC_KEY_FILE + " not specified as the public key file.");
    }
    this.pubKeyPass = systemProps.getProperty(PUBLIC_KEYSTORE_PASSWORD);
    this.aliasCertificateMap = new HashMap();
    populateMap();
  }

  private AuthenticationFailedException getException(String exStr,
      Exception cause) {

    String exMsg = "PKCSAuthenticator: Authentication of client failed due to: "
        + exStr;
    if (cause != null) {
      return new AuthenticationFailedException(exMsg, cause);
    }
    else {
      return new AuthenticationFailedException(exMsg);
    }
  }

  private AuthenticationFailedException getException(String exStr) {
    return getException(exStr, null);
  }

  private X509Certificate getCertificate(String alias)
      throws NoSuchAlgorithmException, InvalidKeySpecException {
    if (this.aliasCertificateMap.containsKey(alias)) {
      return (X509Certificate)this.aliasCertificateMap.get(alias);
    }
    return null;
  }

  public Principal authenticate(Properties props, DistributedMember member)
      throws AuthenticationFailedException {
    String alias = (String)props.get(PKCSAuthInit.KEYSTORE_ALIAS);
    if (alias == null || alias.length() <= 0) {
      throw new AuthenticationFailedException("No alias received");
    }
    try {
      X509Certificate cert = getCertificate(alias);
      if (cert == null) {
        throw getException("No certificate found for alias:" + alias);
      }
      byte[] signatureBytes = (byte[])props.get(PKCSAuthInit.SIGNATURE_DATA);
      if (signatureBytes == null) {
        throw getException("signature data property ["
            + PKCSAuthInit.SIGNATURE_DATA + "] not provided");
      }
      Signature sig = Signature.getInstance(cert.getSigAlgName());
      sig.initVerify(cert);
      sig.update(alias.getBytes("UTF-8"));

      if (!sig.verify(signatureBytes)) {
        throw getException("verification of client signature failed");
      }
      return new PKCSPrincipal(alias);
    }
    catch (GemFireSecurityException ex) {
      throw ex;
    }
    catch (Exception ex) {
      throw getException(ex.toString(), ex);
    }
  }

  public void close() {
  }

}
