/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.util.ArrayList;
import java.util.HashMap;

import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;



/**
 * @author mghosh
 *
 */
public class CustomMsgDispatcher {

  public interface ICustomMessageListener {
    void handleEvent( String msg, ArrayList< Object >  params, ArrayList< Object >  results );
  }

  static private HashMap< String, ArrayList< ICustomMessageListener > > customMsgHandlers_;


  // -- Init storage for custom message handling
  static {
    CustomMsgDispatcher.customMsgHandlers_ = new HashMap< String, ArrayList< ICustomMessageListener > >();
    for( String s : CustomUIMessages.customMessages_ ) {
      CustomMsgDispatcher.customMsgHandlers_.put( s, new ArrayList< ICustomMessageListener  >() );
    }
  }

  public synchronized boolean addCustomMessageListener( String msg, ICustomMessageListener lstnrNew ) {
    ArrayList< ICustomMessageListener  > lstnrs = CustomMsgDispatcher.customMsgHandlers_.get( msg );
    if( null == lstnrs ) {
      lstnrs = new ArrayList< ICustomMessageListener >();
      customMsgHandlers_.put( msg , lstnrs );
    }
    boolean fRet = true;
    if( false == lstnrs.contains( lstnrNew )) {
      LogUtil.fine( "addCustomMessageListener : lstnrs does not contain " + lstnrNew );
      fRet = lstnrs.add( lstnrNew );
    }
    else {
      LogUtil.fine( "addCustomMessageListener : lstnrs **does** contain " + lstnrNew );
    }
    
    return fRet;
  }
  
  public synchronized void sendCustomMessage( String msg, ArrayList< Object > prms, ArrayList< Object > res  ) {
    if( null != msg ) {
      ArrayList< ICustomMessageListener > listeners = customMsgHandlers_.get( msg );
      ArrayList< Object > resTmp = new ArrayList< Object >();
      for( ICustomMessageListener l : listeners ) {
        l.handleEvent(msg, prms, resTmp );
        res.addAll( resTmp );
        resTmp.clear();
      }
    }
  }
  
  /**
   * 
   */
  public CustomMsgDispatcher() {
    // TODO Auto-generated constructor stub
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
