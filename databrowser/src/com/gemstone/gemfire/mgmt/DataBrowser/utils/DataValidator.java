/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.utils;

/**
 * @author mghosh
 *
 */
public class DataValidator {

  /**
   * 
   */
  private DataValidator() {
    // TODO Auto-generated constructor stub
  }

  public static boolean isValidTCPPort( String sP ) {
    boolean fRet = false;
    try {
      int iP = Integer.valueOf( sP ).intValue();
      fRet = DataValidator.isValidTCPPort( iP );
    }
    catch( NumberFormatException xptn ) {
      fRet = false;
    }
    
    return fRet;
  }
  
  public static boolean isValidTCPPort( int iP ) {
    return (( 0 <= iP ) && ( iP <= 65535));
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
