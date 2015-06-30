package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import org.eclipse.jface.preference.PreferencePage;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;

public class PrefModifyListenerAdapter implements ModifyListener {

  private PreferencePage page;  
  
  public PrefModifyListenerAdapter(PreferencePage page) {
   this.page = page;
  } 
  
  public void modifyText(ModifyEvent e) {
    boolean result = page.isValid();
    page.setValid(result);
  }

}
