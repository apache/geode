/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

//import java.net.InetAddress;

/**
 * @author mghosh
 *
 */
public class NetHostInf {

  private final String        name_;
  private final int           port_;

  /**
   *
   */
/*
  private NetHostInf() {
    name_ = null;
    port_ = 0;
  }
*/

  public NetHostInf( String n, int p ) {
    name_ = n;
    port_ = p;
  }

  public final String getName() {
    return name_;
  }

  public final int getPort() {
    return port_;
  }

  // -- returns all the InetAddr for the host
  public final ArrayList< InetAddress > getInetAddr() {
    ArrayList< InetAddress > result_ = new ArrayList< InetAddress >();
    InetAddress[] ias =  null;
    try {
      ias = InetAddress.getAllByName( name_ );
      for( InetAddress ia : ias ) {
        result_.add( ia );
      }
    }
    catch( UnknownHostException xptn ) {
      result_ = null;
      // -- log & eat this exception
      LogUtil.error( "NetHostInf.getInetAddr() failed", xptn );
    }

    return result_;
  }
}
