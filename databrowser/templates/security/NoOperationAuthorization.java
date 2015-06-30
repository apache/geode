/*=========================================================================
* This implementation is provided on an "AS IS" BASIS,  WITHOUT WARRANTIES
* OR CONDITIONS OF ANY KIND, either express or implied."
*==========================================================================
*/

package security;

import java.security.Principal;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.NotAuthorizedException;

/**
 * A implementation of the <code>AccessControl</code> interface that
 * allows authorization depending on the format of the <code>Principal</code>
 * string.
 * @author mjha
 */
public class NoOperationAuthorization implements AccessControl {

  private DistributedMember remoteMember;

  private LogWriter logger;

  public NoOperationAuthorization() {

  }

  public static AccessControl create() {
    return new NoOperationAuthorization();
  }


  public void init(Principal principal, DistributedMember remoteMember,
      Cache cache) throws NotAuthorizedException {


    this.remoteMember = remoteMember;
    this.logger = cache.getSecurityLogger();
  }

  public boolean authorizeOperation(String regionName, OperationContext context) {

    OperationCode opCode = context.getOperationCode();
    this.logger.fine("Invoked authorize operation for [" + opCode
        + "] in region [" + regionName + "] for client: " + remoteMember);
    return false;
  }

  public void close() {
  }

}
