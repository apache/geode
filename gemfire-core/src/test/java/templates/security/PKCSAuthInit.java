/*=========================================================================
* This implementation is provided on an "AS IS" BASIS,  WITHOUT WARRANTIES
* OR CONDITIONS OF ANY KIND, either express or implied."
*==========================================================================
*/

package templates.security;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.security.AuthInitialize;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.GemFireSecurityException;

import java.io.FileInputStream;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.util.Properties;

/**
 * An {@link AuthInitialize} implementation that obtains the digital signature
 * for use with PKCS scheme on server from the given set of properties.
 * 
 * To use this class the <c>security-client-auth-init</c> property should be
 * set to the fully qualified name the static <code>create</code> function
 * viz. <code>templates.security.PKCSAuthInit.create</code>
 * 
 * @author Kumar Neeraj
 * @since 5.5
 */
public class PKCSAuthInit implements AuthInitialize {

  public static final String KEYSTORE_FILE_PATH = "security-keystorepath";

  public static final String KEYSTORE_ALIAS = "security-alias";

  public static final String KEYSTORE_PASSWORD = "security-keystorepass";

  public static final String SIGNATURE_DATA = "security-signature";

  protected LogWriter securitylog;

  protected LogWriter systemlog;

  public void close() {
  }

  public static AuthInitialize create() {
    return new PKCSAuthInit();
  }

  public PKCSAuthInit() {
  }

  public void init(LogWriter systemLogger, LogWriter securityLogger)
      throws AuthenticationFailedException {
    this.systemlog = systemLogger;
    this.securitylog = securityLogger;
  }

  public Properties getCredentials(Properties props, DistributedMember server,
      boolean isPeer) throws AuthenticationFailedException {
    String keyStorePath = props.getProperty(KEYSTORE_FILE_PATH);
    if (keyStorePath == null) {
      throw new AuthenticationFailedException(
          "PKCSAuthInit: key-store file path property [" + KEYSTORE_FILE_PATH
              + "] not set.");
    }
    String alias = props.getProperty(KEYSTORE_ALIAS);
    if (alias == null) {
      throw new AuthenticationFailedException(
          "PKCSAuthInit: key alias name property [" + KEYSTORE_ALIAS
              + "] not set.");
    }
    String keyStorePass = props.getProperty(KEYSTORE_PASSWORD);

    try {
      KeyStore ks = KeyStore.getInstance("PKCS12");
      char[] passPhrase = (keyStorePass != null ? keyStorePass.toCharArray()
          : null);
      FileInputStream certificatefile = new FileInputStream(keyStorePath);
      try {
        ks.load(certificatefile, passPhrase);
      }
      finally {
        certificatefile.close();
      }

      Key key = ks.getKey(alias, passPhrase);

      if (key instanceof PrivateKey) {

        PrivateKey privKey = (PrivateKey)key;
        X509Certificate cert = (X509Certificate)ks.getCertificate(alias);
        Signature sig = Signature.getInstance(cert.getSigAlgName());

        sig.initSign(privKey);
        sig.update(alias.getBytes("UTF-8"));
        byte[] signatureBytes = sig.sign();

        Properties newprops = new Properties();
        newprops.put(KEYSTORE_ALIAS, alias);
        newprops.put(SIGNATURE_DATA, signatureBytes);
        return newprops;
      }
      else {
        throw new AuthenticationFailedException("PKCSAuthInit: "
            + "Failed to load private key from the given file: " + keyStorePath);
      }
    }
    catch (GemFireSecurityException ex) {
      throw ex;
    }
    catch (Exception ex) {
      throw new AuthenticationFailedException(
          "PKCSAuthInit: Exception while getting credentials: " + ex, ex);
    }
  }
}
