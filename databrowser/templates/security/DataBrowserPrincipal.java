/*=========================================================================
* This implementation is provided on an "AS IS" BASIS,  WITHOUT WARRANTIES
* OR CONDITIONS OF ANY KIND, either express or implied."
*==========================================================================
*/

package security;

import java.io.Serializable;
import java.security.Principal;

/**
 * An implementation of {@link Principal} class for a simple user name.
 * 
 * @author mjha
 */
public class DataBrowserPrincipal implements Principal, Serializable {

  private final String userName;

  public DataBrowserPrincipal(String userName) {
    this.userName = userName;
  }

  public String getName() {
    return this.userName;
  }

  public String toString() {
    return this.userName;
  }

}
