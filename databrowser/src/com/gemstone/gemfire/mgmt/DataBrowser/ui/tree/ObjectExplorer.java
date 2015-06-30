package com.gemstone.gemfire.mgmt.DataBrowser.ui.tree;

import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.jface.viewers.TreeViewerColumn;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

import com.gemstone.gemfire.mgmt.DataBrowser.ui.tree.Messages;

public class ObjectExplorer {
  private ObjectContentProvider cp;
  private Shell exp_window;
  private TreeViewer tree_viewer;

  public ObjectExplorer(Shell parent) {
    exp_window = new Shell(parent, SWT.TITLE | SWT.CLOSE | SWT.RESIZE);
    tree_viewer = new TreeViewer(exp_window, SWT.SINGLE | SWT.FULL_SELECTION);
    cp = new ObjectContentProvider();
    tree_viewer.setContentProvider(cp);
    configure();
  }

  private void configure() {
    exp_window.setText("Object Explorer");
    exp_window.setLayout(new GridLayout(1, false));


    tree_viewer.getTree().setHeaderVisible(true);
    tree_viewer.getTree().setLinesVisible(true);
    tree_viewer.getTree().setLayoutData(new GridData(GridData.FILL_BOTH));

    {
      TreeViewerColumn col1 = new TreeViewerColumn(tree_viewer, SWT.LEFT,0);
      col1.getColumn().setText("Field");
      col1.getColumn().setWidth(150);
      col1.setLabelProvider(new ObjectLabelProvider(0));
    }

    {
      TreeViewerColumn col1 = new TreeViewerColumn(tree_viewer, SWT.LEFT,1);
      col1.getColumn().setText("Value");
      col1.getColumn().setWidth(300);
      col1.setLabelProvider(new ObjectLabelProvider(1));
    }

    {
      TreeViewerColumn col1 = new TreeViewerColumn(tree_viewer, SWT.LEFT,2);
      col1.getColumn().setText("Type");
      col1.getColumn().setWidth(150);
      col1.setLabelProvider(new ObjectLabelProvider(2));
    }



    exp_window.setSize(700, 400);
  }


  public void open() {
    this.exp_window.open();
  }

  public void updateModel(ObjectImage root) {
    cp.setRoot(root);
    tree_viewer.setInput("root");
    tree_viewer.expandAll();
  }

  public void waitForClose() {
    Display display = exp_window.getDisplay();
    // Set up the event loop.
    while (!exp_window.isDisposed()) {
      if (!display.readAndDispatch()) {
        // If no more entries in event queue
        display.sleep();
      }
    }
    display.dispose();
  }
}
