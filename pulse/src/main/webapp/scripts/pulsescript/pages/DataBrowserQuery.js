/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

// DataBrowserQuery.js

var responseResult = null;

// This function activates Execute button
function enableExecuteBtn(flagEnable) {
  if (flagEnable) {
    $('#btnExecuteQuery').prop("disabled", false);
    if ($('#btnExecuteQuery').hasClass("grey")) {
      $('#btnExecuteQuery').addClass("blue");
      $('#btnExecuteQuery').removeClass("grey");
    }
  } else {
    $('#btnExecuteQuery').prop("disabled", true);
    if ($('#btnExecuteQuery').hasClass("blue")) {
      $('#btnExecuteQuery').addClass("grey");
      $('#btnExecuteQuery').removeClass("blue");
    }
  }
}

// This function is called when query editor content changes
function onQueryTextChange() {
  var queryString = $('#dataBrowserQueryText').val();
  if (!isEmpty(queryString)) {
    enableExecuteBtn(true);
  } else {
    enableExecuteBtn(false);
  }
}

// This function clears query editor 
function clearDBQuery(){
  $('#dataBrowserQueryText').val('');
  onQueryTextChange();
}

// This function executes query written in query editor and displays results
function executeDBQuery(){
  // Disable Execute button
  enableExecuteBtn(false);

  var queryText = $('#dataBrowserQueryText').val();
  var selectedMemberNames = "";
  
  if(isEmpty(queryText)){
    alert("Please enter query to execute..");
    return;
  }
  
  // Determine selected members query to be execute on 
  if($("#membersList").html() != ""){
    var selectedMembers = $( "input[type=checkbox][name=Member]:checked" );
    for(var i=0; i< selectedMembers.length; i++){
      if(selectedMemberNames == ""){
        selectedMemberNames = selectedMembers[i].value;
      }else{
        selectedMemberNames += ","+selectedMembers[i].value;
      }
    }
  }
  
  // TODO 
  // As of now, member names are creating problem due to ':' character in 
  // member names. hence query based upon member names are not supported.
  // following line of code should be removed when solution to this problem is 
  // in place.
  //selectedMemberNames = "";
  
  requestData = {
        query:queryText,
        members:selectedMemberNames,
        limit:0
      };

  // Display Loading/Busy symbol
  $("#loadingSymbol").show();

  // Delete previous query result, if exists.
  if(!isEmpty($('#memberAccordion').html())){
    deleteExistingResultGrids();
    $('#memberAccordion').html('');
  }

  $.getJSON("pulse/dataBrowserQuery", requestData, function(data) {
    
    if(data != undefined && data != null){
      if(data.error != undefined && data.error != null){
        
        // Delete previous query result, if exists.
        /*if(!isEmpty($('#memberAccordion').html())){
          deleteExistingResultGrids();
          $('#memberAccordion').html('');
        }*/
        
        var errorText = data.error;
        errorText = encodeMemberName(errorText);
        
        $('#memberAccordion').append("<span>"+errorText+"</span>");

      }else if(data.message != undefined && data.message != null){

        // Delete previous query result, if exists.
        /*if(!isEmpty($('#memberAccordion').html())){
          deleteExistingResultGrids();
          $('#memberAccordion').html('');
        }*/

        var messageText = data.message;
        messageText = encodeMemberName(messageText);

        $('#memberAccordion').append("<span>"+messageText+"</span>");

      }else{
        //console.log("data.result.length"+data.result.length);
        // Convert received data into expected format
        responseResult = convertRawResponseToExpectedFormat(data);
        
        // Delete previous query result, if exists.
        /*if(!isEmpty($('#memberAccordion').html())){
          deleteExistingResultGrids();
          $('#memberAccordion').html('');
        }*/
        
        // Create html for newly executed query
        createHtmlForQueryResults();
        
        // Apply accordion Behavior 
        accordion();
        accordionNested();
        /*custom scroll bar*/
        $('.ScrollPaneBlock').jScrollPane();
        
        // Populate result grids
        if(!isEmpty($('#memberAccordion').html())){
          populateResultGrids();
        }
        // Update queries list in history panel
        updateQueryHistory("view","");
        //$('.queryHistoryScroll-pane').jScrollPane();/*Custome scroll*/
        
      }
    }
    
    // Hide Loading/Busy symbol
    $("#loadingSymbol").hide();

    // enable Execute button
    enableExecuteBtn(true);

  }).error(resErrHandler);
  
  return;
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

// This function creates complete result panel html
function createHtmlForQueryResults(){
  var memberResults = responseResult.result;

  if(memberResults.length > 0){

    if(memberResults[0].member != undefined || memberResults[0].member != null){
      //console.log("member wise results found..");      
      for(var i=0; i<memberResults.length; i++){
        //console.log(memberResults[i].member);
        $('#memberAccordion').append(createHtmlForMember(memberResults[i]));
      }
    }else{
      //console.log("cluster level results found..");
      var accordionContentHtml = "";
      accordionContentHtml = createClusterAccordionContentHtml(memberResults);
      var resultHtml = "<div class=\"accordion-content2\">"+ accordionContentHtml +"</div>";
      $('#memberAccordion').append(resultHtml);
    }
  }else{
    $('#memberAccordion').append("<span> No Results Found...</span>");
  }
}

// This function create/generate single member's html
function createHtmlForMember(memberData){
  
  var accordionContentHtml = "";
  accordionContentHtml = createMemberAccordionContentHtml(memberData);
  // Convert angular brackets ('<', '>') using HTML character codes 
  var memberTitle = memberData.member[1].replace(/\<+/g, '&#60').replace(/\>+/g, '&#62');
  var memberHtml = 
            "<div class=\"listing\">" +
              "<div class=\"heading\">" +
                "<span class=\"title\">"+ memberTitle + "</span>" +
              "</div>" +
              "<div class=\"accordion-content\">"+ accordionContentHtml +
              "</div>" +
            "</div>";
  
  return memberHtml;
}

// This function creates/generates member's accordion content html
function createMemberAccordionContentHtml(memberData){
  
  var memName = encodeMemberName(memberData.member[1]);
  var requiredHeight = determineAccordionContentHeight(memberData.result);
  var memberAccordionContentHtml = "";
  memberAccordionContentHtml += 
                "<div class=\"ScrollPaneBlock\" style=\"height: "+requiredHeight+"px;\">" +
                  "<div id=\"memberDetails_"+memName+"\" class=\"accordionNested\">";
  
  var memberResults = memberData.result;
  for(var i=0; i<memberResults.length; i++){
    var htmlTableId = generateTableIdForObjectsTable(memberData.member[1], memberResults[i].objectType);
    var ScrollPaneBlockHeight = 40;
    var resultsLength = memberResults[i].objectResults.length;
    if(resultsLength > 0 && resultsLength < 6){
      ScrollPaneBlockHeight += resultsLength * 30;
    }else{
      ScrollPaneBlockHeight = 200;
    }
    memberAccordionContentHtml += 
                    "<div class=\"n-listing\">" +
                      "<div class=\"n-heading\">" +
                        "<span class=\"n-title\">"+memberResults[i].objectType+"</span>" +
                      "</div>" +
                      "<div class=\"n-accordion-content\">" +
                        "<div class=\"widthfull-100per\">" +
                            "<table id=\""+ htmlTableId +"\"></table>" +
                        "</div>" +
                      "</div>" +
                    "</div>";
  }
  
  memberAccordionContentHtml += 
                  "</div>" +
                "</div>";
  
  return memberAccordionContentHtml ;
  
}

//This function creates/generates cluster's accordion content html
function createClusterAccordionContentHtml(clusterResults){
  var requiredHeight = determineAccordionContentHeight(clusterResults);
  var memberAccordionContentHtml = "";
  memberAccordionContentHtml += 
             "<div class=\"ScrollPaneBlock\" style=\"height: "+requiredHeight+"px;\">" +
               "<div id=\"clusterDetails\" class=\"accordionNested\">";

  for(var i=0; i<clusterResults.length; i++){
    var htmlTableId = generateTableIdForObjectsTable("", clusterResults[i].objectType);
    var ScrollPaneBlockHeight = 40;
    var resultsLength = clusterResults[i].objectResults.length;
    if(resultsLength > 0 && resultsLength < 6){
      ScrollPaneBlockHeight += resultsLength * 30;
    }else{
      ScrollPaneBlockHeight = 200;
    }
    memberAccordionContentHtml += 
                 "<div class=\"n-listing\">" +
                   "<div class=\"n-heading\">" +
                     "<span class=\"n-title\">"+clusterResults[i].objectType+"</span>" +
                   "</div>" +
                   "<div class=\"n-accordion-content\">" +
                     "<div class=\"widthfull-100per\">" +
                         "<table id=\""+ htmlTableId +"\"></table>" +
                     "</div>" +
                   "</div>" +
                 "</div>";
  }

  memberAccordionContentHtml += 
               "</div>" +
             "</div>";

  return memberAccordionContentHtml ;

}

// Function to find/determine the required height of accordion container
function determineAccordionContentHeight(result){
  var numObjects = result.length;
  // determine height of single grid/table
  var height = 0;
  for(var i=0; i<numObjects; i++){
    var numObjectEntries = result[i].objectResults.length;
    if(numObjectEntries > 0 
        && numObjectEntries < 6){
      if(height < 210){
        height = 60 + numObjectEntries * 30;
      }
    }else{
      height = 210;
    }
  }

  // Add additional height required for accordin headers
  height += numObjects * 30;
  return height;

}

// Function to generates id of html table in which results grid is contained
function generateTableIdForObjectsTable(memberName, objectName){
  // Remove all dots (.) in the string and also replace "[]" by "BxBr"
  var objName = objectName.replace(/\.+/g, '').replace("[]", 'BxBr');
  var memName = encodeMemberName(memberName);
  // form html table id
  var tableId = "table-"+memName+"-"+objName;
  
  return tableId;
}

// Function to generates encoded member name
function encodeMemberName(memberName){
  var memName = memberName.replace(/\.+/g, '');
  memName = memName.replace(/\(+/g, '-');
  memName = memName.replace(/\)+/g, '-');
  memName = memName.replace(/\<+/g, '-');
  memName = memName.replace(/\>+/g, '-');
  memName = memName.replace(/\:+/g, '-');
  
  return memName;
}

// This function populates query results into jqgrid tables 
function populateResultGrids(){
  
  var memberResults = responseResult.result;

  if(memberResults.length == 0){
    return;
  }

  if(memberResults[0].member != undefined || memberResults[0].member != null){
    for(var i=0; i<memberResults.length; i++){
      
      var memberResult = memberResults[i].result;
      var memberName = memberResults[i].member[1];
      for(var j=0; j<memberResult.length; j++){
        var htmlTableId = generateTableIdForObjectsTable(memberName, memberResult[j].objectType);
        
        if($('#'+htmlTableId)){
          createResultGrid(memberName, memberResult[j]);
        }else{
          console.log("Not Found : "+htmlTableId);
        }
      }
    }
  }else{
      
      var memberResult = memberResults;
      var memberName = "";
      for(var j=0; j<memberResult.length; j++){
        var htmlTableId = generateTableIdForObjectsTable(memberName, memberResult[j].objectType);
        
        if($('#'+htmlTableId)){
          createResultGrid(memberName, memberResult[j]);
        }else{
          console.log("Not Found : "+htmlTableId);
        }
      }
  }
}

// Function to delete previous queries results from UI
function deleteExistingResultGrids(){
  if(responseResult == undefined || responseResult == null){
    return;
  }
  
  var memberResults = responseResult.result;

  if(memberResults.length == 0){
    return;
  }

  if(memberResults[0].member != undefined || memberResults[0].member != null){
    for(var i=0; i<memberResults.length; i++){
      
      var memberResult = memberResults[i].result;
      var memberName = memberResults[i].member[1];
      for(var j=0; j<memberResult.length; j++){
        var htmlTableId = generateTableIdForObjectsTable(memberName, memberResult[j].objectType);
        
        if($('#'+htmlTableId)){
          $('#'+htmlTableId).jqGrid('GridDestroy');
        }else{
          console.log("Not Found : "+htmlTableId);
        }
      }
    }
  }else{
    var memberResult = memberResults;
    var memberName = "";
    for(var j=0; j<memberResult.length; j++){
      var htmlTableId = generateTableIdForObjectsTable(memberName, memberResult[j].objectType);
      
      if($('#'+htmlTableId)){
        $('#'+htmlTableId).jqGrid('GridDestroy');
      }else{
        console.log("Not Found : "+htmlTableId);
      }
    }
  }
}

// Function to create single results grid
function createResultGrid(member, memberResultObject){
  var objectType = memberResultObject.objectType;
  // make a copy of memberResultObject.objectResults
  var objectResults = jQuery.extend(true, [], memberResultObject.objectResults);
  var htmlTableId = generateTableIdForObjectsTable(member, objectType);
  
  // TODO Following code section should be removed once to string representation 
  // is finalized in same scenario 
  // If results set is java primitive collection of HashMap then following 
  // custom representation is needed
  /*var customObjectResults = new Array();
  if(objectType.indexOf("java.util.HashMap") != -1){
    // generate customized objectResults
    for(var cnt=0; cnt<objectResults.length; cnt++){
      var uid = objectResults[cnt].uid;
      delete objectResults[cnt].uid;
      var newEntry = {"uid":uid, "result":["java.util.HashMap",objectResults[cnt]]};
      customObjectResults.push(newEntry);
    }
    // replace existing objectResults by new customObjectResults
    objectResults = customObjectResults;
  }*/
  
  // Determine table columns
  var columnName = new Array();
  var columnModel = new Array();
  for(var cnt=0; cnt<objectResults.length; cnt++){
    for(key in objectResults[cnt]){
      if(-1 == columnName.indexOf(key)){
        // add column and column model only if not present
        columnName.push(key);
        columnModel.push({name:key, index:key, width:150});
      }
    }
  }
  
  // set custom column widths if less columns
  if(columnModel.length == 2){
    for(var c=0; c<columnModel.length; c++){
      columnModel[c].width = 660;
    }
  }else if(columnModel.length == 3){
    for(var c=0; c<columnModel.length; c++){
      columnModel[c].width = 330;
    }
  }else if(columnModel.length == 4){
    for(var c=0; c<columnModel.length; c++){
      columnModel[c].width = 220;
    }
  }
  
  // Delete jqgrid if already exists
  $("#"+htmlTableId).jqGrid('GridDestroy');
  
  // Determine Grid's Height
  var gridheight = 30;
  if(objectResults.length > 0 && objectResults.length < 6){
    gridheight += objectResults.length * 26;
  }else{
    gridheight = 160;
  }
  
  // Create jqgrid 
  jQuery("#"+htmlTableId).jqGrid({
                  datatype: "local",
                  width : 668,
                  height: gridheight,
                  shrinkToFit : false,
                  colNames:columnName,
                  colModel:columnModel,
                  ondblClickRow:displayPopUpObjectExplorer,
                  gridComplete : function() {
                    $(".jqgrow").css({
                      cursor : 'default'
                    });

                    var gridList = $('#gview_'+htmlTableId);
                    var gridListChild = gridList.children('.ui-jqgrid-bdiv');

                    gridListChild.unbind('jsp-scroll-x');
                    gridListChild.bind('jsp-scroll-x', function(event,
                        scrollPositionX, isAtLeft, isAtRight) {
                      var mRList = $('#gview_'+htmlTableId);
                      var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                          '.ui-jqgrid-hbox');
                      mRLC.css("position", "relative");
                      mRLC.css('right', scrollPositionX);
                    });
                  },
                  resizeStop : function(width, index) {

                    var gridList = $('#gview_'+htmlTableId);
                    var gridListChild = gridList.children('.ui-jqgrid-bdiv');
                    var api = gridListChild.data('jsp');
                    api.reinitialise();

                    gridList = $('#gview_'+htmlTableId);
                    gridListChild = gridList.children('.ui-jqgrid-bdiv');
                    gridListChild.unbind('jsp-scroll-x');
                    gridListChild.bind('jsp-scroll-x', function(event,
                        scrollPositionX, isAtLeft, isAtRight) {
                      var mRList = $('#gview_'+htmlTableId);
                      var mRLC = mRList.children('.ui-jqgrid-hdiv').children(
                          '.ui-jqgrid-hbox');
                      mRLC.css("position", "relative");
                      mRLC.css('right', scrollPositionX);
                    });
                  }
          });
  
  // Hide "uid" column as it contains program generated unique id of row
  $("#"+htmlTableId).jqGrid('hideCol',columnName[columnName.indexOf("uid")]);
  
  // Remove type details from values
  for(var i=0; i<objectResults.length; i++){
    for(key in objectResults[i]){
      var arrVals = objectResults[i][key];
      if(arrVals != null && arrVals != undefined){
        if($.isArray(arrVals)){
          if(arrVals.length == 2){
            if(typeof (arrVals[1]) == "object"){ // if object, display its type as value
              objectResults[i][key] = arrVals[0];
            }else{ // else display its value
              objectResults[i][key] = arrVals[1];
            }
          }else{ // if ArrayList object, display its type as value
            objectResults[i][key] = arrVals[0];
          }
        }
      } 
    }
  }

  // Clear grid before populating rows into it
  $("#"+htmlTableId).jqGrid('clearGridData');
  $("#"+htmlTableId).jqGrid('addRowData',columnName[columnName.indexOf("uid")],objectResults);
}

// Function that displays object explorer in pop up
function displayPopUpObjectExplorer(rowId, rowIndex, columnIndex, event){
  //console.log("Row clicked: "+rowId+" "+rowIndex+" "+columnIndex+" "+event);
  
  var rowIdSplits = rowId.split('*');
  var selMember = null
  var selMemObject = null;
  var selMemObjectRow = null;
  var selectedRowData = null;
  
  if(rowIdSplits.length == 2){
    selMemObject = rowIdSplits[0];
    selMemObjectRow = rowIdSplits[1];
  }else if(rowIdSplits.length == 3){
    selMember = rowIdSplits[0];
    selMemObject = rowIdSplits[1];
    selMemObjectRow = rowIdSplits[2];
  }else{
    console.log("Incomplete inputs ");
    return;
  }
  
  var resResults = responseResult.result;
  var memberResults = null;
  var memberObjectResults = null;
  
  if(rowIdSplits.length == 2){
    memberResults = resResults;
  }else if(rowIdSplits.length == 3){
    for(var i=0; i< resResults.length; i++){
      if(resResults[i].member[1] == selMember){
        memberResults = resResults[i].result;
        break;
      }
    }
  }
  
  for(var i=0; i< memberResults.length; i++){
    // replace "[]" by "BxBr"
    var objType = memberResults[i].objectType.replace("[]", 'BxBr');
    if(objType == selMemObject){
      memberObjectResults = memberResults[i].objectResults;
      break;
    }
  }
  
  for(var i=0; i< memberObjectResults.length; i++){
    /*if(memberObjectResults[i].ID[1] == selMemObjectRow){
      selectedRowData = memberObjectResults[i];
      break;
    }*/
    if(memberObjectResults[i].uid == rowId){
      selectedRowData = memberObjectResults[i];
      break;
    }
  }
  
  var popUpData = formDataForPopUpGrid(selectedRowData);
  
  // clear pop grid data
  gridElement.jqGrid('clearGridData');
  // add pop grid new data
  gridElement[0].addJSONData({
    total : 1,
    page : 1,
    records : popUpData.length,
    rows : popUpData
  });
  
  // Display pop up grid
  $('#gridPopup').show();
  $('.ui-jqgrid-bdiv').jScrollPane();

  // Attaching handler to each row in object explorer to update vertical
  // scrollbar when user expands/collapse tree grid row
  $('div.tree-plus').click(function() {
    $('.ui-jqgrid-bdiv').jScrollPane();
  });
                  
  // Get the screen height and width
  var maskHeight = $(document).height();
  var maskWidth = $(window).width();

  //Set heigth and width to mask to fill up the whole screen
  $('#mask').css({'width':maskWidth,'height':maskHeight});
  
  //transition effect             
  $('#mask').fadeIn(1000);        
  $('#mask').fadeTo("fast",0.8);  

  //Get the window height and width
  var winH = $(window).height();
  var winW = $(window).width();

  //Set the popup window to center
  $('#gridPopup').css('top',  winH/2-$('#gridPopup').height()/2);
  $('#gridPopup').css('left', winW/2-$('#gridPopup').width()/2);

}

// Creates pop grid
function createPopUpObjectExplorer(){
  gridElement = $("#popUpExplorer");
  gridElement.jqGrid({
      datatype: "local",
      colNames:["Id","Key","Value","Type"],
      colModel:[
          {name:'id', index:'id', width:1, hidden:true, key:true},
          {name:'key', index:'key', width:220},
          {name:'value', index:'value', width:220},
          {name:'type', index:'type', width:230}
      ],
      cmTemplate: {sortable:false},
      height: '220',
      gridview: true,
      rowNum: 10000,
      sortname: 'id',
      treeGrid: true,
      treeGridModel: 'adjacency',
      treedatatype: "local",
      ExpandColumn: 'key',
  });
  
  gridElement.jqGrid('clearGridData');
  
}

// Function to prepare data for pop up object explorer
function formDataForPopUpGrid(data){
  
  var newArray = [];
  var idCounter = 1;

  function traverse(object, level, parentId) {
      for (var prop in object) {
          if (typeof(object[prop]) == "object") {
            if($.isArray(object[prop])){
              
              var arrObj = object[prop];
              if(arrObj.length == 2){
                
                if (typeof(arrObj[1]) == "object") {
                  //newArray.push({'key':prop, 'value':arrObj[1], 'type':arrObj[0]});
                  newArray.push({
                    'id':idCounter++,
                    'key':prop, 
                    'type':arrObj[0],
                    'value':"", // ignore value for composite data
                    'level':level,
                    'parent':parentId,
                    'isLeaf':false,
                    'expanded':false,
                    'loaded':true
                    });
                
                  //going on step down in the object tree!!
                  traverse(arrObj[1], level+1, idCounter-1);
                }else{
                  //newArray.push({'key':prop, 'value':arrObj[1], 'type':arrObj[0]});
                  newArray.push({
                    'id':idCounter++,
                    'key':prop, 
                    'type':arrObj[0],
                    'value':arrObj[1],
                    'level':level,
                    'parent':parentId,
                    'isLeaf':false,
                    'expanded':false,
                    'loaded':true
                    });
                }
              }else{
                //newArray.push({'key':prop, 'value':object[prop], 'type':typeof(object[prop])});
                newArray.push({
                  'id':idCounter++,
                  'key':prop, 
                  'type':typeof(object[prop]),
                  'value':object[prop],
                  'level':level,
                  'parent':parentId,
                  'isLeaf':false,
                  'expanded':false,
                  'loaded':true
                  });
                
                // todo : multiple value traversing in array
              }
            }else{
              newArray.push({
                'id':idCounter++,
                'key':prop, 
                'type':"object",
                'value':"", // ignore value for composite data
                'level':level,
                'parent':parentId,
                'isLeaf':false,
                'expanded':false,
                'loaded':true
                });
              //going on step down in the object tree!!
              traverse(object[prop], level+1, idCounter-1);
            }
              
          }else{
            //console.log(prop + " : "+object[prop]);
            if(prop != "uid"){
              newArray.push({
                  'id':idCounter++,
                  'key':prop, 
                  'type':typeof(object[prop]),
                  'value':object[prop],
                  'level':level,
                  'parent':parentId,
                  'isLeaf':false,
                  'expanded':false,
                  'loaded':true
                  });
            }
          }
      }
  }

  // start traversing into data object 
  traverse(data, 0, 0);
  
  return newArray;
}

//Function for converting raw response into expected format
function convertRawResponseToExpectedFormat(rawResponeData){
  
  if(rawResponeData == null || rawResponeData == undefined){
    return;
  }
  
  var finalResponseData = {};
  var finalResponseResults = new Array();
  
  if(rawResponeData.result != null || rawResponeData.result != undefined){
    var rawResponeDataResult = rawResponeData.result;
    
    for(var i=0; i<rawResponeDataResult.length; i++){
      if(rawResponeDataResult[i] != null){
        if(typeof(rawResponeDataResult[i]) == "object"){
          if($.isArray(rawResponeDataResult[i])){
            // Cluster level result set
            finalResponseResults = convertToExpectedObjectsFormat(rawResponeDataResult, "");
            break;
            
          }else if(rawResponeDataResult[i].member != null && rawResponeDataResult[i].member != undefined){
            
            var responseForMember = {};
            responseForMember.member = rawResponeDataResult[i].member[0];
            responseForMember.result = convertToExpectedObjectsFormat(rawResponeDataResult[i].result, rawResponeDataResult[i].member[0][1]);
            
            // add into final results
            finalResponseResults.push(responseForMember);
          }
        }
      }
    }
  }
  
  finalResponseData.result = finalResponseResults;
  //responseResult = finalResponseData;
  return finalResponseData;
}

// Function for converting raw response into expected object wise results format
function convertToExpectedObjectsFormat(rawResponseResult, prefixForId){
  
  var expResponseResult = new Array();
  
  if(rawResponseResult != null && rawResponseResult != undefined ){
    
    for(var i=0; i< rawResponseResult.length; i++){
      if(rawResponseResult[i] != null){
        
        if(expResponseResult.length > 0){
          // search expected object type in expResponseResult
          var flagObjectFound = false;
          for(var j=0 ; j < expResponseResult.length ; j++){
            if(expResponseResult[j].objectType == rawResponseResult[i][0]){
              // required object found
              flagObjectFound = true;
              var objectResults = expResponseResult[j].objectResults;
              var type = rawResponseResult[i][0];
              var entry = rawResponseResult[i][1];

              // if entry is not object then convert it into object
              if(typeof(entry) != "object" ){
                var entryObj = {};
                entryObj[type] = rawResponseResult[i][1];
                entry = entryObj;
              }

              // add unique id for new entry
              entry.uid = generateEntryUID(prefixForId, expResponseResult[j].objectType, objectResults.length);
              
              objectResults.push(entry);
              break;
            }
          }
          
          if(!flagObjectFound){  // required object not found in expResponseResult 
            
            var objectResults = new Array();
            var type = rawResponseResult[i][0];
            var entry = rawResponseResult[i][1];

            // if entry is not object then convert it into object
            if(typeof(entry) != "object" ){
              var entryObj = {};
              entryObj[type] = rawResponseResult[i][1];
              entry = entryObj;
            }

            // add unique id for new entry
            entry.uid = generateEntryUID(prefixForId, type, objectResults.length);
            
            objectResults.push(entry);
            
            var newResultObject = {};
            newResultObject.objectType = type;
            newResultObject.objectResults = objectResults;
            
            expResponseResult.push(newResultObject);
          }
          
        }else{  // expResponseResult is empty
          
          var objectResults = new Array();
          var type = rawResponseResult[i][0];
          var entry = rawResponseResult[i][1];

          // if entry is not object then convert it into object
          if(typeof(entry) != "object" ){
            var entryObj = {};
            entryObj[type] = rawResponseResult[i][1];
            entry = entryObj;
          }

          // add unique id for new entry
          entry.uid = generateEntryUID(prefixForId, type, objectResults.length);
          
          objectResults.push(entry);
          
          var newResultObject = {};
          newResultObject.objectType = type;
          newResultObject.objectResults = objectResults;
          
          expResponseResult.push(newResultObject);
        }
        
      }
    }
  }
  
  return expResponseResult;
}

// Function to generate unique idetifier for entry
function generateEntryUID(prefixForId, type, len) {

  var uid = "";

  if(type.indexOf("[]") != -1){
    type = type.replace("[]","BxBr");
  }

  if (prefixForId != "") {
    uid = prefixForId + "*" + type + "*" + len;
  } else {
    uid = type + "*" + len;
  }

  return uid;
}

// Function that posts the query result response received to backend in order
// export it into file
function exportResult() {
  if(responseResult != undefined && responseResult !== null){
    $.generateFile({
      filename : 'export.json',
      content : JSON.stringify(responseResult),
      script : 'pulse/dataBrowserExport'
    });
  }
}
