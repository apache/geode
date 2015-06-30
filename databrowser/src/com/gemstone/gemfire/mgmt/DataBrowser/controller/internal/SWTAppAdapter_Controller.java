/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.controller.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.MessageBox;

import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;
import com.gemstone.gemfire.mgmt.DataBrowser.app.State;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.ConnectionTerminatedEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.IConnectionEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DSSnapShot;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DataBrowserController;
import com.gemstone.gemfire.mgmt.DataBrowser.model.IMemberEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.CustomUIMessages;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.MainAppWindow;

/**
 * @author mghosh
 *
 */
public class SWTAppAdapter_Controller extends DataBrowserController {
  private MemberEventProcessor memberEventProcessor= new MemberEventProcessor();

  /**
   *
   */
  public SWTAppAdapter_Controller() {
    // TODO Auto-generated constructor stub
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.mgmt.DataBrowser.controller.DataBrowserController#memberEventReceived(com.gemstone.gemfire.mgmt.DataBrowser.model.IMemberEvent)
   */
  @Override
  public void memberEventReceived(final IMemberEvent memEvent) {
    final MainAppWindow wnd = DataBrowserApp.getInstance().getMainWindow();
    memberEventProcessor.addEvent(memEvent);
    if(memberEventProcessor.isProcessed()){
      memberEventProcessor.setProcessed(false);
      Display display = wnd.getShell().getDisplay();
      display.asyncExec(memberEventProcessor);
    }
  }

  public void connectionEventReceived(IConnectionEvent event) {
    if(event instanceof ConnectionTerminatedEvent) {
      if(this.hasConnection()) {
        try {
          this.disconnect();
        }
        catch (Exception e) {
          // mjha- do we need to log the error ?
        }

        final MainAppWindow wnd = DataBrowserApp.getInstance().getMainWindow();
        if (null != wnd) {
          Runnable runnable = new Runnable() {
            public void run() {
              State st = DataBrowserApp.getInstance().getState();
              DSSnapShot dsss = st.getCurrDS();

              ArrayList<Object> prms = new ArrayList<Object>();
              prms.add(dsss);
              ArrayList<Object> res = new ArrayList<Object>();
              wnd.sendCustomMessage(CustomUIMessages.DS_DISCONNECTED, prms, res);

              // TODO MGH - see if this can be done by sending a message to main window.
              MessageBox mb = new MessageBox(wnd.getShell(), SWT.OK);
              //TODO: Handle this message for localization.
              mb.setText("Connection Closed unexpectedly");
              mb.setMessage("Connection Closed unexpectedly");
              mb.open();

            }
          };
          Display display = wnd.getShell().getDisplay();
          display.asyncExec(runnable);
        }
      }
    }

  }

  private static class MemberEventProcessor implements Runnable{
    private boolean processed = true;
    private Map<String, IMemberEvent> memberEventMap = new HashMap<String, IMemberEvent>();

    public void run() {
      processed = false;
      ArrayList<Object> res = new ArrayList<Object>();
      ArrayList<Object> prms= new ArrayList<Object>() ;
      synchronized (memberEventMap) {
        Collection<IMemberEvent> values = memberEventMap.values();
        prms.addAll(values);
        memberEventMap.clear();
        setProcessed(true);
      }
      MainAppWindow wnd = DataBrowserApp.getInstance().getMainWindow();
      if(wnd != null)
        wnd.sendCustomMessage(CustomUIMessages.UPDATE_MEMBER_EVENT, prms, res);
    }

    public boolean isProcessed(){
      return processed;
    }

    public void setProcessed(boolean processed) {
      this.processed = processed;
    }

    public void addEvent(IMemberEvent memEvent){
      GemFireMember member = memEvent.getMember();
      if(member != null){
        synchronized (memberEventMap) {
          memberEventMap.put(member.getId(), memEvent);
        }
      }
    }
  }

}
