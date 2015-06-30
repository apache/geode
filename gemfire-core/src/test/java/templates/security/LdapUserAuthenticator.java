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

import java.security.Principal;
import java.util.Properties;
import javax.naming.Context;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

/**
 * @author Kumar Neeraj
 * @since 5.5
 */
public class LdapUserAuthenticator implements Authenticator {

  private String ldapServer = null;

  private String basedn = null;

  private String ldapUrlScheme = null;

  public static final String LDAP_SERVER_NAME = "security-ldap-server";

  public static final String LDAP_BASEDN_NAME = "security-ldap-basedn";

  public static final String LDAP_SSL_NAME = "security-ldap-usessl";

  public static Authenticator create() {
    return new LdapUserAuthenticator();
  }

  public LdapUserAuthenticator() {
  }

  public void init(Properties securityProps, LogWriter systemLogger,
      LogWriter securityLogger) throws AuthenticationFailedException {
    this.ldapServer = securityProps.getProperty(LDAP_SERVER_NAME);
    if (this.ldapServer == null || this.ldapServer.length() == 0) {
      throw new AuthenticationFailedException(
          "LdapUserAuthenticator: LDAP server property [" + LDAP_SERVER_NAME
              + "] not specified");
    }
    this.basedn = securityProps.getProperty(LDAP_BASEDN_NAME);
    if (this.basedn == null || this.basedn.length() == 0) {
      throw new AuthenticationFailedException(
          "LdapUserAuthenticator: LDAP base DN property [" + LDAP_BASEDN_NAME
              + "] not specified");
    }
    String sslStr = securityProps.getProperty(LDAP_SSL_NAME);
    if (sslStr != null && sslStr.toLowerCase().equals("true")) {
      this.ldapUrlScheme = "ldaps://";
    }
    else {
      this.ldapUrlScheme = "ldap://";
    }
  }

  public Principal authenticate(Properties props, DistributedMember member) {

    String userName = props.getProperty(UserPasswordAuthInit.USER_NAME);
    if (userName == null) {
      throw new AuthenticationFailedException(
          "LdapUserAuthenticator: user name property ["
              + UserPasswordAuthInit.USER_NAME + "] not provided");
    }
    String passwd = props.getProperty(UserPasswordAuthInit.PASSWORD);
    if (passwd == null) {
      passwd = "";
    }

    Properties env = new Properties();
    env
        .put(Context.INITIAL_CONTEXT_FACTORY,
            "com.sun.jndi.ldap.LdapCtxFactory");
    env.put(Context.PROVIDER_URL, this.ldapUrlScheme + this.ldapServer + '/'
        + this.basedn);
    String fullentry = "uid=" + userName + "," + this.basedn;
    env.put(Context.SECURITY_PRINCIPAL, fullentry);
    env.put(Context.SECURITY_CREDENTIALS, passwd);
    try {
      DirContext ctx = new InitialDirContext(env);
      ctx.close();
    }
    catch (Exception e) {
      //TODO:hitesh need to add getCause message
      throw new AuthenticationFailedException(
          "LdapUserAuthenticator: Failure with provided username, password "
              + "combination for user name: " + userName);
    }
    return new UsernamePrincipal(userName);
  }

  public void close() {
  }

}
