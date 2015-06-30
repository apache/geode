/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.jface.action.ActionContributionItem;
import org.eclipse.jface.action.IContributionItem;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.viewers.ColumnPixelData;
import org.eclipse.jface.viewers.ILabelProvider;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.ITreeContentProvider;
import org.eclipse.jface.viewers.LabelProvider;
import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.jface.viewers.TableLayout;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTError;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.HelpEvent;
import org.eclipse.swt.events.HelpListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

import com.gemstone.gemfire.mgmt.DataBrowser.app.DataBrowserApp;
import com.gemstone.gemfire.mgmt.DataBrowser.app.State;
import com.gemstone.gemfire.mgmt.DataBrowser.controller.DSSnapShot;
import com.gemstone.gemfire.mgmt.DataBrowser.model.IMemberEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.MemberCrashedEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.MemberJoinedEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.MemberLeftEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.MemberUpdatedEvent;
import com.gemstone.gemfire.mgmt.DataBrowser.model.region.GemFireRegion;
import com.gemstone.gemfire.mgmt.DataBrowser.ui.actions.NewCQWindow;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.StringMatcher;

/**
 * @author mghosh
 *
 */
public class DSView extends Composite {

  static State                  appState_          = DataBrowserApp
                                                       .getInstance()
                                                       .getState();
  TreeViewer                    dsExplorerView_;
  Text                          regionPatternCtrl_; 

  // -- DS Explorer settings
  boolean                       fOpenFullyExpanded = false;

  private ArrayList<DSSnapShot> connectedDSs_      = null;

  private PatternFilter         filter_;  
  private MenuManager           tableMenuManager_;
  private NewCQWindow           newCQWindowAction_;
  private DSTreeContentProvider treeContentProv_;
  private MemberTableContentProvider memberContentProv_;
  private TableViewer memberViewer_;
  private Button newCQButton;

  /**
   * @param parent
   * @param style
   */
  public DSView(Composite parent, int style) {
    super(parent, style);
    registerForMessages();
    
    GridLayout gLayout = new GridLayout();
    gLayout.marginHeight = 0;
    gLayout.marginWidth = 0;
    setLayout(gLayout);
    
    SashForm sash = new SashForm(this, SWT.FLAT | SWT.VERTICAL );
    sash.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

    FillLayout lytSash = new FillLayout();
    lytSash.type = SWT.HORIZONTAL;
    lytSash.marginWidth = 0;
    lytSash.marginHeight = 0;
    sash.setLayout(lytSash);
    
    Composite p1 = new Composite(sash, style);
    gLayout = new GridLayout();
    gLayout.numColumns = 1;
    gLayout.marginHeight = 0;
    gLayout.marginWidth = 0;
    p1.setLayout(gLayout);
    createRegionPatternControl(p1);
    createDSExplorerView(p1);
    
    
    Composite p2 = new Composite(sash, style);
    gLayout = new GridLayout();
    gLayout.marginHeight = 0;
    gLayout.marginWidth = 0;
    gLayout.numColumns = 1;
    p2.setLayout(gLayout);
    createMemberList(p2);
    
    sash.pack();
    this.pack();
  }

  
  private void createRegionPatternControl(Composite parent) {
    regionPatternCtrl_ = new Text(parent, SWT.LEFT | SWT.SINGLE | SWT.BORDER);
    
    GridData gData = new GridData(SWT.FILL, SWT.VERTICAL, true, false);
    gData.verticalSpan = 1 ;
    regionPatternCtrl_.setLayoutData(gData);
  }
  
  private void createMemberList(Composite parent) {
    memberViewer_ = new TableViewer(parent);
    
    TableLayout lytTable = new TableLayout();
    
    lytTable.addColumnData(new ColumnPixelData(250, true));
    
    memberViewer_.getTable().setLayout(lytTable);
    memberViewer_.getTable().setHeaderVisible(true);
    memberViewer_.getTable().setLinesVisible(true);
    
    GridData gd = new GridData(SWT.FILL, SWT.FILL, true, true);
    memberViewer_.getTable().setLayoutData(gd);
    
    TableColumn col = new TableColumn(memberViewer_.getTable(), SWT.NONE, 0);
    col.setText("Members");
    
    memberContentProv_ = new MemberTableContentProvider(this);
    
    memberViewer_.setContentProvider(memberContentProv_);
    memberViewer_.setLabelProvider(new MemberTableLabelProvider(this));

    memberViewer_.getTable().addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        TableItem item = (TableItem)e.item;        
        GemFireMember member = (GemFireMember)item.getData();
        if (member != null && member.isNotifyBySubscriptionEnabled()) {
          IContributionItem menuItem = tableMenuManager_.find(newCQWindowAction_.getId());
          if (null == menuItem) {
           tableMenuManager_.add(newCQWindowAction_);
          }
          newCQButton.setEnabled(true);
        } else {
          tableMenuManager_.remove(newCQWindowAction_.getId());
          newCQButton.setEnabled(false);
        }
      }
    });
    
    tableMenuManager_ = new MenuManager();
    memberViewer_.getTable().setMenu(tableMenuManager_.createContextMenu(memberViewer_.getTable()));
    newCQWindowAction_ = new NewCQWindow();

    ActionContributionItem cqActionItem = new ActionContributionItem(newCQWindowAction_);
    cqActionItem.fill(parent);
    newCQButton = (Button) cqActionItem.getWidget();
    newCQButton.setText("New Continuous Query");
    newCQButton.setEnabled(false);
  }

  private void installFilter() {
    regionPatternCtrl_.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        String text = ((Text) e.widget).getText();
        setMatcherString(text, true);
      }
    });
  }

  protected void setMatcherString(String pattern, boolean update) {
    if (pattern.length() == 0) {
      filter_.setPattern(null);
    } else {
      filter_.setPattern(pattern);
    }

    if (update)
      stringMatcherUpdated();
  }

  protected void stringMatcherUpdated() {
    // refresh viewer to re-filter
    dsExplorerView_.getControl().setRedraw(false);
    dsExplorerView_.refresh();
    dsExplorerView_.expandAll();
    selectFirstMatch();
    dsExplorerView_.getControl().setRedraw(true);
  }
  
  protected Object selectFirstMatch() {
    Tree tree = dsExplorerView_.getTree();
    Object element = findElement(tree.getItems());
    if (element != null) {
      dsExplorerView_.setSelection(new StructuredSelection(element), true);
    } else
      dsExplorerView_.setSelection(StructuredSelection.EMPTY);
    
    return element;
  }

  private GemFireRegionRepresentation findElement(TreeItem[] items) {
    ILabelProvider labelProvider = (ILabelProvider) dsExplorerView_
        .getLabelProvider();
    for (int i = 0; i < items.length; i++) {
      GemFireRegionRepresentation element = (GemFireRegionRepresentation)items[i].getData();
      StringMatcher matcher = filter_.getPattern();
      if (matcher == null)
        return element;

      if (element != null) {
        String label = labelProvider.getText(element);
        if (matcher.match(label))
          return element;
      }
    }
    return null;
  }


  // -----------------------------------------------------------------
  //
  // DSTree view stuff
  //
  // -----------------------------------------------------------------
  private void createDSExplorerView(Composite parent) {
    dsExplorerView_ = new TreeViewer(parent, SWT.FLAT | SWT.VIRTUAL
        | SWT.BORDER);
    GridData gData = new GridData(SWT.FILL, SWT.FILL, true, true);
    gData.verticalSpan = 10 ;
    dsExplorerView_.getTree().setLayoutData(gData);
    // this.treeViewer_.getTree().setHeaderVisible( true );
    this.treeContentProv_ = new DSTreeContentProvider(dsExplorerView_);
    dsExplorerView_.setContentProvider(treeContentProv_);
    dsExplorerView_.setLabelProvider(new DSTreeLabelProvider(this));
    dsExplorerView_.setInput(viewInput);

    // -- setup listeners
    dsExplorerView_.addHelpListener(new DSViewHelpLstnr(this));
    this.filter_ = new PatternFilter();
    this.filter_.setIncludeLeadingWildcard(true);
    this.filter_.setUseCache(true);
    dsExplorerView_.addFilter(this.filter_);

    // -- toggle this on user prefs
    if (true == fOpenFullyExpanded)
      dsExplorerView_.expandAll();

    installFilter();
    
    dsExplorerView_.getTree().addSelectionListener(new TreeSelectionAdapter());
  }

  public Control getTree() {
    return dsExplorerView_.getTree();
  }

  static private class DSTreeContentProvider implements ITreeContentProvider {

    private TreeViewer viewer_;

    public DSTreeContentProvider(TreeViewer vwr) {
      viewer_ = vwr;
    }

    public Object[] getChildren(Object element) {
      Object[] o = new Object[0];
      if (element instanceof DSViewInput) {
        DSViewInput input = (DSViewInput) element;
        GemFireRegionRepresentation[] regions = input.getRegions();
        o = regions;
      } else if (element instanceof GemFireRegionRepresentation) {
        GemFireRegionRepresentation reg = (GemFireRegionRepresentation)element;
        o = reg.getSubRegions();
      }

      return o;
    }

    public Object[] getElements(Object element) {
      Object[] oRet = new Object[0];
      if (element instanceof DSViewInput) {
        DSViewInput input = (DSViewInput) element;
        GemFireRegionRepresentation[] regions = input.getRegions();
        oRet = regions;
      
      }else if (element instanceof GemFireRegionRepresentation) {
        GemFireRegionRepresentation reg = (GemFireRegionRepresentation)element;
        oRet = reg.getSubRegions();
      }
      
      return oRet;
    }

    public boolean hasChildren(Object element) {
      boolean fRet = false;
      if (element instanceof DSViewInput) {
        DSViewInput input = (DSViewInput) element;
        fRet = (0 != input.getRegions().length) ? true : false;
      } else if (element instanceof GemFireRegionRepresentation) {
        GemFireRegionRepresentation reg = (GemFireRegionRepresentation)element;
        return ( reg.getSubRegions().length > 0);        
      }

      return fRet;
    }

    public Object getParent(Object element) {
      Object oRet = null;
      /*
       * if (element instanceof GemFireMember) { oRet = null; } else if (element
       * instanceof Region) { oRet = ((Region) element).getDsMember(); }
       */

      return oRet;
    }

    public void dispose() {
    }

    public void inputChanged(Viewer viewer, Object old_input, Object new_input) {
    }
  }

  static private class DSTreeLabelProvider extends LabelProvider {
    static String  imgPathMember = "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/DSMember.ico";
    static String  imgPathRegion = "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/Region.ico";

    private DSView dsvParent_    = null;

    DSTreeLabelProvider(DSView dsv) {
      dsvParent_ = dsv;
    }

    @Override
    public String getText(Object element) {
      /*
       * if (element instanceof Region) { return ((Region) element).getName(); }
       */
      String sLabel = "";
      if (element instanceof GemFireRegionRepresentation) {
        sLabel = ((GemFireRegionRepresentation)element).getName();
      } else if (element instanceof GemFireMember) {
        sLabel = ((GemFireMember) element).getRepresentationName();
      } else if (element instanceof DSSnapShot) {
        sLabel = "snapshot";
      }

      return sLabel;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.eclipse.jface.viewers.LabelProvider#getImage(java.lang.Object)
     */
    @Override
    public Image getImage(Object element) {
      Display dsply = dsvParent_.getDisplay();
      String path = null;
      // if (element instanceof Region) {
      if (element instanceof String) {
        path = DSTreeLabelProvider.imgPathRegion;
      } else if (element instanceof GemFireMember) {
        path = DSTreeLabelProvider.imgPathMember;
      }

      Image img = null;
      if (null != path) {
        InputStream isImage = null;
        try {
          isImage = getClass().getResourceAsStream(path);

          if (null != isImage) {
            img = new Image(dsply, isImage);
          }
        } catch (SWTException xptn) {
          // MGH - we simply show no icon
          // handler for org.eclipse.swt.graphics.Image ctor
          // we continue an try to add the other nodes
          LogUtil
              .error(
                  "Unable to create icon for item in DSTreeLabelProvider " + getText(element) + ". No icon will be rendered.", xptn);
          img = null;
        } catch (SWTError err) {
          // MGH - we simply show no icon
          LogUtil
              .error(
                  "Unable to create icon for item in DSTreeLabelProvider " + getText(element) + ". No icon will be rendered.", err);
          img = null;
        } finally {
          if (null != isImage) {
            try {
              isImage.close();
            } catch (IOException xptn) {
              LogUtil
                  .error(
                      "Error closing image resource input stream when creating icon for in DSTreeLabelProvider " + getText(element) + ". Ignoring error.", xptn);
            }
          }
        }
      }

      return img;
    }

  } // DSTreeLabelProvider

  // -----------------------------------------------------------------
  //
  // Common resizer handler
  //
  // -----------------------------------------------------------------
  private static class DSViewHelpLstnr implements HelpListener {
    DSView dsvParent_ = null;

    DSViewHelpLstnr(DSView prnt) {
      dsvParent_ = prnt;
    }

    public void helpRequested(HelpEvent e) {

    }
  }

  private void registerForMessages() {
    final DataBrowserApp app = DataBrowserApp.getInstance();
    if (null != app) {
      final MainAppWindow wnd = app.getMainWindow();

      if (null != wnd) {
        // TODO MGH - Perhaps we should log failure to register and continue!
        wnd.addCustomMessageListener(CustomUIMessages.DS_CONNECTED, viewInput);
        wnd.addCustomMessageListener(CustomUIMessages.UPDATE_MEMBER_EVENT,
            viewInput);
        wnd.addCustomMessageListener(
            CustomUIMessages.QRY_MEMBER_SELECTED_FOR_QUERY_EXEC,
            hndlrMemSelCustomMsgs);
      }
    }
  }

  private DSViewInput                     viewInput             = new DSViewInput();
  private Member_Selection_custom_handler hndlrMemSelCustomMsgs = new Member_Selection_custom_handler();


  private class Member_Selection_custom_handler implements
      CustomMsgDispatcher.ICustomMessageListener {

    Member_Selection_custom_handler() {
    }

    /*
     * (non-Javadoc)
     *
     * @seecom.gemstone.gemfire.mgmt.DataBrowser.ui.CustomMsgDispatcher.
     * ICustomMessageListener#handleEvent(java.lang.String, java.util.ArrayList,
     * java.util.ArrayList)
     */
    public void handleEvent(String msg, ArrayList<Object> params,
        ArrayList<Object> results) {

      if (CustomUIMessages.QRY_MEMBER_SELECTED_FOR_QUERY_EXEC.equals(msg)) {
        IStructuredSelection selection = (IStructuredSelection)DSView.this.memberViewer_.getSelection();
        if(!selection.isEmpty()) {
          results.add(selection.getFirstElement());
          return;
        } 
        
        //If only one member is available, then by default use it. No need for user intervention.
        GemFireRegionRepresentation reg = (GemFireRegionRepresentation)memberViewer_.getInput();
        if(reg != null) {
          GemFireMember[] members = viewInput.getMembers(reg);          
          if(members.length >= 1) {
            results.add(members[0]);
          }
        }
      } // CustomUIMessages.QRY_MEMBER_SELECTED_FOR_QUERY_EXEC
    }

  }

  private class DSViewInput implements
      CustomMsgDispatcher.ICustomMessageListener {

    private DSSnapShot                      snapshot_;

    private Map<GemFireRegionRepresentation, Set<GemFireMember>> regionMemberContainer_;
    private Set<GemFireRegionRepresentation> rootRegions_;

    public DSViewInput() {
      regionMemberContainer_ = new HashMap<GemFireRegionRepresentation, Set<GemFireMember>>();
      rootRegions_ = new HashSet<GemFireRegionRepresentation>();
    }

    public DSSnapShot getDSSnapShot() {
      return snapshot_;
    }

    public GemFireRegionRepresentation[] getRegions() {
      return rootRegions_.toArray(new GemFireRegionRepresentation[0]);
    }

    public GemFireMember[] getMembers(GemFireRegionRepresentation region) {
      Set<GemFireMember> set = regionMemberContainer_.get(region);
      if(set == null)
        return new GemFireMember[0];

      GemFireMember[] members = new GemFireMember[set.size()];

      return set.toArray(members);
    }

    public void addMember(GemFireMember[] members) {
      for (int j = 0; j < members.length; j++) {
        GemFireMember member = members[j];
        GemFireRegion[] rootRegions = member.getRootRegions();
        
        for( GemFireRegion reg : rootRegions) {
          GemFireRegionRepresentation temp = addRegionToTree(member, reg, null);
          rootRegions_.add(temp);
        }
      }
      
      GemFireRegionRepresentation rep = (GemFireRegionRepresentation)selectFirstMatch();
      DSView.this.memberViewer_.setInput(rep);
    }
    
    private GemFireRegionRepresentation addRegionToTree(GemFireMember member,
        GemFireRegion reg, GemFireRegionRepresentation parent) {
      GemFireRegionRepresentation root = updateRegionMemberContainer(member,
          reg);

      if (null != parent)
        dsExplorerView_.add(parent, root);
      else
        dsExplorerView_.add(this, root);

      GemFireRegion[] subRegions = reg.getSubRegions();

      for (GemFireRegion sub : subRegions) {
        GemFireRegionRepresentation temp = addRegionToTree(member, sub, root);
        root.addSubRegion(temp);
      }

      return root;
    }
    
    private GemFireRegionRepresentation updateRegionMemberContainer(GemFireMember member, GemFireRegion reg) {
      String fullPath = reg.getFullPath();
      String name = reg.getName();
      GemFireRegionRepresentation rep = new GemFireRegionRepresentation(fullPath, name);
      Set<GemFireMember> memSet = regionMemberContainer_.get(rep);
      if (memSet == null) {
        memSet = new HashSet<GemFireMember>();
        regionMemberContainer_.put(rep, memSet);
        memSet.add(member);        
      } else {
        if (!memSet.contains(member)) {
         memSet.add(member);
        }
      }

      return rep;
    }

    public void removeMember(GemFireMember[] members) {
      for (int j = 0; j < members.length; j++) {
        GemFireMember member = members[j];
        GemFireRegion[] allRegions = member.getAllRegions();
        
        for(GemFireRegion reg : allRegions) {
          GemFireRegionRepresentation rep = new GemFireRegionRepresentation(reg.getFullPath(),reg.getName());
          Set<GemFireMember> memberSet = regionMemberContainer_.get(rep);
          if(memberSet != null) {
            memberSet.remove(member);
            
            if(memberSet.isEmpty()) {
             regionMemberContainer_.remove(rep); 
             rootRegions_.remove(rep);
             dsExplorerView_.remove(this, new Object[]{rep});

             //Send a Message to the Member Table to clear its contents.
             DSView.this.memberViewer_.setInput(rep.getFullPath());
            }
          }
        }
      }
    }

    private void clear() {
      regionMemberContainer_.clear();
      regionMemberContainer_ = new HashMap<GemFireRegionRepresentation, Set<GemFireMember>>();
      rootRegions_.clear();
      rootRegions_ = new HashSet<GemFireRegionRepresentation>();
      Object input = dsExplorerView_.getInput();
      dsExplorerView_.setInput(input);
      memberViewer_.setInput(new Object()); //We just want to clear the table.      
      tableMenuManager_.remove(newCQWindowAction_.getId());
      newCQButton.setEnabled(false);
    }

    private void dsConnected(DSSnapShot dss) {
      snapshot_ = dss;
      GemFireMember[] members = snapshot_.getMembers();
      addMember(members);
      dsExplorerView_.expandAll();
      GemFireRegionRepresentation rep = (GemFireRegionRepresentation)selectFirstMatch();
      DSView.this.memberViewer_.setInput(rep);      
      tableMenuManager_.add(newCQWindowAction_);
    }

    public void handleEvent(String msg, ArrayList<Object> params,
        ArrayList<Object> results) {
      if (CustomUIMessages.UPDATE_MEMBER_EVENT.equals(msg)) {
        if (true == params.isEmpty()) {
          return;
        }
        for (int i = 0; i < params.size(); i++) {
          Object oParam = params.get(i);
          if (oParam instanceof IMemberEvent) {
            IMemberEvent event = (IMemberEvent)oParam;
            if (event instanceof MemberUpdatedEvent
                || event instanceof MemberJoinedEvent) {
              addMember(event.getMembers());
            }
            else if (event instanceof MemberLeftEvent
                || event instanceof MemberCrashedEvent) {
              removeMember(event.getMembers());
            }
          }
        }
        dsExplorerView_.expandAll();
      } // CustomUIMessages.UPDATE_MEMBER_EVENT
      else if (CustomUIMessages.DS_CONNECTED.equals(msg)) {
        clear();
        if (!params.isEmpty()) {
          Object oParam = params.get(0);
          if (oParam instanceof DSSnapShot) {
            DSSnapShot dss = (DSSnapShot) oParam;
            dsConnected(dss);
          }
        }
      }
    }
  }
  
  private class TreeSelectionAdapter extends SelectionAdapter {
    
    @Override
    public void widgetSelected(SelectionEvent e) {
      TreeItem item = (TreeItem)e.item;
      GemFireRegionRepresentation region = (GemFireRegionRepresentation)item.getData();
        
      DSView.this.memberViewer_.setInput(region);
    }
  }
  
  private static class MemberTableContentProvider implements IStructuredContentProvider {
    private DSView view;

    MemberTableContentProvider(DSView view) {
      this.view = view;
    }
    
    public Object[] getElements(Object inputElement) {
      Object[] result = new Object[0];
      
      if(inputElement instanceof GemFireRegionRepresentation) {
        GemFireRegionRepresentation temp = (GemFireRegionRepresentation)inputElement;
        GemFireMember[] members = view.viewInput.getMembers(temp);
        result = members;
      }
      
      return result;
    }

    public void dispose() {
    }

    public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {
    }
  }
  
  static private class MemberTableLabelProvider extends LabelProvider {
    private static final String imgPath    = "/com/gemstone/gemfire/mgmt/DataBrowser/resources/icons/QueryResultsTablIcon.ico";
    DSView                      dsvParent_ = null;

    public MemberTableLabelProvider(DSView prnt) {
      super();
      dsvParent_ = prnt;
    }

    @Override
    public String getText(Object element) {
      String sLabel = "";

      if (element instanceof GemFireMember) {
        sLabel = ((GemFireMember)element).getRepresentationName();
      }
      return sLabel;
    }

    @Override
    public Image getImage(Object element) {
      return null;
    }

  } 

  static class GemFireRegionRepresentation {
    private String fullPath;
    private String name;
    private List<GemFireRegionRepresentation> subregions;
    
    public GemFireRegionRepresentation(String fullPath, String name) {
      super();
      this.fullPath = fullPath;
      this.name = name;
      this.subregions = new ArrayList<GemFireRegionRepresentation>();
    }
    
    public String getFullPath() {
      return fullPath;
    }
    
    public String getName() {
      return name;
    }
    
    public void addSubRegion(GemFireRegionRepresentation sub) {
      this.subregions.add(sub);
    }
    
    public GemFireRegionRepresentation[] getSubRegions() {
      return this.subregions.toArray(new GemFireRegionRepresentation[0]);
    }
    
    @Override
    public boolean equals(Object obj) {
      if(!(obj instanceof GemFireRegionRepresentation))
        return false;
      
      GemFireRegionRepresentation rep = (GemFireRegionRepresentation)obj;
      return getFullPath().equals(rep.getFullPath());
    }
    
    @Override
    public int hashCode() {
      return getFullPath().hashCode();
    }
    
    @Override
    public String toString() {
      return getFullPath();
    }
  }
}
