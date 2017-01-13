/*=========================================================================
* This implementation is provided on an "AS IS" BASIS,  WITHOUT WARRANTIES 
* OR CONDITIONS OF ANY KIND, either express or implied."
*==========================================================================
*/

package javaobject;

import java.security.Principal;
import java.util.Properties;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.Authenticator;
import javaobject.UserPasswordAuthInit;
import javaobject.UsernamePrincipal;

/**
 * A dummy implementation of the {@link Authenticator} interface that expects a
 * user name and password allowing authentication depending on the format of the
 * user name.
 * 
 * 
 * @since 5.5
 */
public class DummyAuthenticator implements Authenticator {

  public static Authenticator create() {
    return new DummyAuthenticator();
  }

  public DummyAuthenticator() {
  }

  public void init(Properties systemProps, LogWriter systemLogger,
      LogWriter securityLogger) throws AuthenticationFailedException {
  }

  public static boolean testValidName(String userName) {

    return (userName.startsWith("user") || userName.startsWith("reader")
        || userName.startsWith("writer") || userName.equals("admin")
        || userName.equals("root") || userName.equals("administrator"));
  }

  public Principal authenticate(Properties props, DistributedMember member)
      throws AuthenticationFailedException {

    String userName = props.getProperty(UserPasswordAuthInit.USER_NAME);
    if (userName == null) {
      throw new AuthenticationFailedException(
          "DummyAuthenticator: user name property ["
              + UserPasswordAuthInit.USER_NAME + "] not provided");
    }
    String password = props.getProperty(UserPasswordAuthInit.PASSWORD);
    if (password == null) {
      throw new AuthenticationFailedException(
          "DummyAuthenticator: password property ["
              + UserPasswordAuthInit.PASSWORD + "] not provided");
    }

    if (userName.equals(password) && testValidName(userName)) {
      return new UsernamePrincipal(userName);
    }
    else {
      throw new AuthenticationFailedException(
          "DummyAuthenticator: Invalid user name [" + userName
              + "], password supplied.");
    }
  }

  public void close() {
  }

}
