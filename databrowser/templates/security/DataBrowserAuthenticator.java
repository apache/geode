/*=========================================================================
* This implementation is provided on an "AS IS" BASIS,  WITHOUT WARRANTIES 
* OR CONDITIONS OF ANY KIND, either express or implied."
*==========================================================================
*/

package security;

import java.security.Principal;
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.Authenticator;

/**
 * A implementation of the {@link Authenticator} interface that expects a
 * user name and password allowing authentication depending on the format of the
 * user name.
 * @author mjha
 */
public class DataBrowserAuthenticator implements Authenticator {

  public static Authenticator create() {
    return new DataBrowserAuthenticator();
  }

  public DataBrowserAuthenticator() {
  }

  public void init(Properties systemProps, LogWriter systemLogger,
      LogWriter securityLogger) throws AuthenticationFailedException {
  }

  public static boolean testValidName(String userName) {

    return true;
  }

  public Principal authenticate(Properties props, DistributedMember member)
      throws AuthenticationFailedException {
    String userName = props.getProperty(DataBrowserAuthInit.USER_NAME);
    if (userName == null) {
      throw new AuthenticationFailedException(
          "DataBrowserAuthenticator: user name property ["
              + DataBrowserAuthInit.USER_NAME + "] not provided");
    }
    String password = props.getProperty(DataBrowserAuthInit.PASSWORD);
    if (password == null) {
      throw new AuthenticationFailedException(
          "DataBrowserAuthenticator: password property ["
              + DataBrowserAuthInit.PASSWORD + "] not provided");
    }

    if (userName.equals(password) && testValidName(userName)) {
      return new DataBrowserPrincipal(userName);
    }
    else {
      throw new AuthenticationFailedException(
          "DataBrowserAuthenticator: Invalid user name [" + userName
              + "], password supplied.");
    }
  }

  public void close() {
  }

}
