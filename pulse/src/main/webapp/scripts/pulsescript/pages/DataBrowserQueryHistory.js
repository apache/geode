/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

// DataBrowserQueryHistory.js
// updateQueryHistory()
function updateQueryHistory(action,queryId) {
  
  requestData = {
    action:action,
    queryId:queryId
  };

  $.getJSON("pulse/dataBrowserQueryHistory", requestData, function(data) {
    
    var queries = new Array();
    if(data.queryHistory != undefined && data.queryHistory != null){
      queries = data.queryHistory;
    }
    var refHistoryConatiner = $("#detailsHistoryList");
    var queryListHTML = "";
    if(queries.length == 0){
      // no queries found
      queryListHTML = "No Query Found";
    }else{
      queries.sort(dynamicSort("queryId", "desc"));
      for(var i=0; i<queries.length && i<20; i++){
        // add query item
        queryListHTML += "" +
          "<div class=\"container\">" +
            "<div class=\"wrap\">" +
              "<div class=\"read-more\">" +
                "<a href=\"#\" class=\"remore_plus\">&nbsp;</a>" +
              "</div>" +
              "<div class=\"remove\">" +
                "<a href=\"#\" onclick=\"updateQueryHistory('delete','"+ queries[i].queryId +"');\">&nbsp;</a>" +
              "</div>" +
              "<div class=\"wrapHistoryContent\"  ondblclick=\"queryHistoryItemClicked(this);\">" + queries[i].queryText +
              "</div>" +
              "<div class=\"dateTimeHistory\">" + queries[i].queryDateTime +
              "</div>" +
            "</div>" +
          "</div>";
      }
    }
    
    refHistoryConatiner.html(queryListHTML);
    //$('.queryHistoryScroll-pane').jScrollPane();/*Custome scroll*/    

    // Set eventsAdded = false as list is refreshed and slide events 
    // (for expanding and collapsing) are removed
    eventsAdded = false;
    
  }).error(resErrHandler);
   
}

// This function displays error if occurred 
function resErrHandler(data){
  // Check for unauthorized access
  if (data.status == 401) {
    // redirect user on Login Page
    window.location.href = "Login.html?error=UNAUTH_ACCESS";
  }else{
    console.log(data);
  }
};

// This function is called when any query from history list is double clicked 
function queryHistoryItemClicked(divElement){
  // Set selected query text into Query Editor
  $('#dataBrowserQueryText').val(unescapeHTML(divElement.innerHTML));
  //Enable Execute button
  onQueryTextChange();
  // fire a click event on document to hide history panel
  $(document).click();
  
  
  
}
