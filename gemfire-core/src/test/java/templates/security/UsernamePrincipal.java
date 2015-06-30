/*=========================================================================
* This implementation is provided on an "AS IS" BASIS,  WITHOUT WARRANTIES
* OR CONDITIONS OF ANY KIND, either express or implied."
*==========================================================================
*/

package templates.security;

import java.io.Serializable;
import java.security.Principal;

/**
 * An implementation of {@link Principal} class for a simple user name.
 * 
 * @author Kumar Neeraj
 * @since 5.5
 */
public class UsernamePrincipal implements Principal, Serializable {

  private final String userName;

  public UsernamePrincipal(String userName) {
    this.userName = userName;
  }

  public String getName() {
    return this.userName;
  }

  @Override
  public String toString() {
    return this.userName;
  }

}
