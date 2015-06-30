/*=========================================================================
* This implementation is provided on an "AS IS" BASIS,  WITHOUT WARRANTIES
* OR CONDITIONS OF ANY KIND, either express or implied."
*==========================================================================
*/

package templates.security;

import java.util.HashSet;

/**
 * This is a sample class for objects which hold information of the authorized
 * function names and authorized value for the optimizeForWrite.
 * 
 * @author Aneesh Karayil
 * @since 6.0
 */
public class FunctionSecurityPrmsHolder {

  private final Boolean isOptimizeForWrite;

  private final HashSet<String> functionIds;

  private final HashSet<String> keySet;

  public FunctionSecurityPrmsHolder(Boolean isOptimizeForWrite,
      HashSet<String> functionIds, HashSet<String> keySet) {
    this.isOptimizeForWrite = isOptimizeForWrite;
    this.functionIds = functionIds;
    this.keySet = keySet;
  }

  public Boolean isOptimizeForWrite() {
    return isOptimizeForWrite;
  }

  public HashSet<String> getFunctionIds() {
    return functionIds;
  }

  public HashSet<String> getKeySet() {
    return keySet;
  }
}
