/**
 * 
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;

import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.ErrorEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.event.ICQEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * @author mghosh
 * 
 */
public class MessagePanel extends Composite {

  private Text exceptions;

  /**
   * @param parent
   * @param style
   */
  public MessagePanel(Composite parent, int style) {
    super(parent, style);
    
    GridLayout lytGrpDisc = new GridLayout();
    lytGrpDisc.numColumns = 1;
    this.setLayout(lytGrpDisc);
    
    exceptions = new Text(this, SWT.MULTI | SWT.READ_ONLY | SWT.LEFT | SWT.V_SCROLL);
    GridData data = new GridData(SWT.FILL, SWT.FILL, true, true);
    data.heightHint = 80;
    exceptions.setLayoutData(data);
  }

  public void processCQEvet(ICQEvent cqEvent){
    if(cqEvent instanceof ErrorEvent) {
      if(cqEvent.getThrowable() != null) {
         StringWriter sw = new StringWriter();
         PrintWriter pw = new PrintWriter(sw);

         cqEvent.getThrowable().printStackTrace(pw);  
         exceptions.append(sw.toString());
         
         try {
          pw.close();
          sw.close();
        } catch (IOException e) { 
          LogUtil.error("Error while writing cq to message panel", e);
        }
      }
    }
  }
}
