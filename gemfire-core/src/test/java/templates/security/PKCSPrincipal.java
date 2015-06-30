/*=========================================================================
* This implementation is provided on an "AS IS" BASIS,  WITHOUT WARRANTIES
* OR CONDITIONS OF ANY KIND, either express or implied."
*==========================================================================
*/

package templates.security;

import java.security.Principal;

/**
 * @author kneeraj
 * 
 */
public class PKCSPrincipal implements Principal {

  private String alias;

  public PKCSPrincipal(String alias) {
    this.alias = alias;
  }

  public String getName() {
    return this.alias;
  }

  @Override
  public String toString() {
    return this.alias;
  }
}
