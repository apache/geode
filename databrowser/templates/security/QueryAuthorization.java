/*=========================================================================
* This implementation is provided on an "AS IS" BASIS,  WITHOUT WARRANTIES
* OR CONDITIONS OF ANY KIND, either express or implied."
*==========================================================================
*/

package security;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

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
public class QueryAuthorization implements AccessControl {

  private Set allowedOps;

  private DistributedMember remoteMember;

  private LogWriter logger;

  private static final OperationCode[] READER_OPS = { OperationCode.EXECUTE_CQ, OperationCode.CLOSE_CQ, OperationCode.STOP_CQ, OperationCode.QUERY };
  public QueryAuthorization() {
    this.allowedOps = new HashSet(5);
  }

  public static AccessControl create() {
    return new QueryAuthorization();
  }

  private void addReaderOps() {
    for (int index = 0; index < READER_OPS.length; index++) {
      this.allowedOps.add(READER_OPS[index]);
    }
  }


  public void init(Principal principal, DistributedMember remoteMember,
      Cache cache) throws NotAuthorizedException {
    if (principal != null) {
      String name = principal.getName().toLowerCase();
      if (name != null) {
        if (name.equals("databrowser")) {
          addReaderOps();
        }
      }
    }
    this.remoteMember = remoteMember;
    this.logger = cache.getSecurityLogger();
  }

  public boolean authorizeOperation(String regionName, OperationContext context) {

    OperationCode opCode = context.getOperationCode();
    this.logger.fine("Invoked authorize operation for [" + opCode
        + "] in region [" + regionName + "] for client: " + remoteMember);
    return this.allowedOps.contains(opCode);
  }

  public void close() {
    this.allowedOps.clear();
  }

}
