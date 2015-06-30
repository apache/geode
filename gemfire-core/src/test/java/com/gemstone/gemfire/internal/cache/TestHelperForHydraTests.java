package com.gemstone.gemfire.internal.cache;
/**
 * This is utility class for hydra tests.
 * This class is used to set the values of parameters of internal classes which have local visibility. 
 * @author prafulla
 *
 */

public class TestHelperForHydraTests
{
  
  public static void setIssueCallbacksOfCacheObserver (boolean value){
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = value;
  }

}
