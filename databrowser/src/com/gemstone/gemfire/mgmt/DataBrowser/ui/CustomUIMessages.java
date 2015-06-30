/**
 *
 */
package com.gemstone.gemfire.mgmt.DataBrowser.ui;

import java.util.ArrayList;

/**
 * @author mghosh
 *
 */
public abstract class CustomUIMessages {
  // -------------------------------------------------------------
  // -- Query stuff
  // -------------------------------------------------------------
  // -- Sent to the pane containg the query string
  //    Param -
  //            an int value that can be one of the following
  //                    0 -> Get selection
  //                    1 -> Get last line
  //                    2 -> Get all the contents of the pane.
  //    Res -
  //            a string containing the results or a null if there is nothing to return (no selection or no content)
  public static final String QRY_MSG_GET_QUERY_STR_FOR_EXEC = "Get Query String For Execution";
  public static final String QRY_MSG_ADD_QUERY_SINGLE_RESULT = "Add Query Single Result";

  // -- Show/hide the tabs showing the results of query execution
  public static final String QRY_MSG_SHOW_QUERY_TRACE_PANE = "Hide Query Trace Pane";
  public static final String QRY_MSG_SHOW_QUERY_STATISTICS_PANE = "Hide Query Log Pane";
  public static final String QRY_MSG_SHOW_QUERY_MESSAGES_PANE = "Hide Query Messages Pane";
  public static final String QRY_MSG_SHOW_QUERY_EXECPLAN_PANE = "Hide Query ExecutionPlan Pane";

  public static final String QRY_MEMBER_SELECTED_FOR_QUERY_EXEC = "Get Selected Memebr For Query Execution";
  public static final String DISPOSE_QUERY_RESULTS = "Dispose Query Results";

  // for CQ
  public static final String QRY_MSG_GET_CQ_QUERY_STR_FOR_EXEC = "Get CQ Query String For Execution";
  public static final String QRY_MSG_SET_CQ_QUERY_FOR_DISP     = "Set CQ Query For For Result Display";
  public static final String QRY_MSG_PROCESS_CQ_QUERY_EVT = "Process CQ Query New Type added Event";

  // -------------------------------------------------------------
  // -- Tree stuff
  // -------------------------------------------------------------
  public static final String DSTREE_MSG_UPDATE_SNAPSHOT = "Update DS Snapshot";
  public static final String UPDATE_MEMBER_EVENT = "Update Gemfire Member";

  // -------------------------------------------------------------
  // -- Overall activity events stuff
  // -------------------------------------------------------------
  // Single parameter for each of the following event - the DSSnapShot
  public static final String DS_CONNECTED = "Connected to DS";
  public static final String DS_DISCONNECTED = "Disconnected from DS";

  protected final static ArrayList< String > customMessages_ = new ArrayList< String >();

  static {
    CustomUIMessages.customMessages_.add( CustomUIMessages.QRY_MSG_GET_QUERY_STR_FOR_EXEC );

    CustomUIMessages.customMessages_.add( CustomUIMessages.QRY_MSG_SHOW_QUERY_TRACE_PANE );
    CustomUIMessages.customMessages_.add( CustomUIMessages.QRY_MSG_SHOW_QUERY_STATISTICS_PANE );
    CustomUIMessages.customMessages_.add( CustomUIMessages.QRY_MSG_SHOW_QUERY_MESSAGES_PANE );
    CustomUIMessages.customMessages_.add( CustomUIMessages.QRY_MSG_SHOW_QUERY_EXECPLAN_PANE );

    CustomUIMessages.customMessages_.add( CustomUIMessages.DSTREE_MSG_UPDATE_SNAPSHOT );

    CustomUIMessages.customMessages_.add( CustomUIMessages.DS_CONNECTED );
    CustomUIMessages.customMessages_.add( CustomUIMessages.DS_DISCONNECTED );
  }

  /**
   *
   */
  public CustomUIMessages() {
    // TODO Auto-generated constructor stub
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
