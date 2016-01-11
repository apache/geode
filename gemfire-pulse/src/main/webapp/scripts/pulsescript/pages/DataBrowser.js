/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

// DataBrowser.js

var clusterRegionsList = new Array();
var eventsAdded = false;

var labelType, useGradients, nativeTextSupport, animate;

(function() {
  var ua = navigator.userAgent,
      iStuff = ua.match(/iPhone/i) || ua.match(/iPad/i),
      typeOfCanvas = typeof HTMLCanvasElement,
      nativeCanvasSupport = (typeOfCanvas == 'object' || typeOfCanvas == 'function'),
      textSupport = nativeCanvasSupport 
        && (typeof document.createElement('canvas').getContext('2d').fillText == 'function');
  //I'm setting this based on the fact that ExCanvas provides text support for IE
  //and that as of today iPhone/iPad current text support is lame
  labelType = (!nativeCanvasSupport || (textSupport && !iStuff))? 'Native' : 'HTML';
  nativeTextSupport = labelType == 'Native';
  useGradients = nativeCanvasSupport;
  animate = !(iStuff || !nativeCanvasSupport);
})();


$(document).ready(function() {
  
  // Load Notification HTML  
  generateNotificationsPanel();

  if (CONST_BACKEND_PRODUCT_SQLFIRE == productname.toLowerCase()) {
    alterHtmlContainer(CONST_BACKEND_PRODUCT_SQLFIRE);
  } else {
    alterHtmlContainer(CONST_BACKEND_PRODUCT_GEMFIRE);
  }

  scanPageForWidgets();
  $.ajaxSetup({ cache: false });
  
  var requestData = {};
  getRequestParams();
  $.getJSON("pulse/authenticateUser", requestData, function(data) {
    
    // return isUserLoggedIn
    if(!data.isUserLoggedIn){
      // redirect user on Login Page 
      window.location.href = "Login.html";
    }else{
      getPulseVersion();
      
      // System Alerts
      //getSystemAlerts();
      
      // keep "Execute" button disabled if editor is empty else make it enabled
      onQueryTextChange();
      
      // Initialize Cluster's regions tree
      initClusterRegions();
      
      // Display queries list in history panel
      updateQueryHistory("view","");
      //$('.queryHistoryScroll-pane').jScrollPane();/*Custome scroll*/
      /*custom scroll bar*/
      //$('.ScrollPaneBlock').jScrollPane();
      
      /*splitter*/
      $('#widget').width('1002').height('100%').split({orientation:'vertical', limit:250, position: '250'});
      $('#rightBlock').width('748')/*.height('100%')*/;
      $('#leftBlock').width('250')/*.height('100%')*/.split({orientation:'horizontal', limit:310, position: '380'});
       //$('#leftBlock').width('29.7%').height('100%');
      $('.ScrollPaneBlock').jScrollPane();
      
      /*History Overlay Toggle*/
      /*toggle*/

      $("#historyIcon").click(queryHistoryIconOnClick);

      $(document).click(function(event) {
              var a = $(event.target).andSelf().parents("#detailsHistory");
              if (a.length == 0 && $("#detailsHistory").is(":visible")) {
                      $("#detailsHistory").toggle();
                      $("#historyIcon").toggleClass('historyClicked-On').toggleClass('historyClicked-Off');
              }
      });
      
      // create pop up object explorer
      createPopUpObjectExplorer();
      
    }

  }).error(repsonseErrorHandler);
  
});

// Handler for query history button click
var queryHistoryIconOnClick = function(event) {
  $("#detailsHistory").toggle();
  $("#historyIcon").toggleClass('historyClicked-Off').toggleClass('historyClicked-On');
  event.stopPropagation();

  // Add slide event if not added
  if (!eventsAdded) {
    addSlideEvents();
    eventsAdded = true;
    $('.queryHistoryScroll-pane').jScrollPane();
  }
};

/*
 * Function to show and hide html elements/components based upon whether product
 * is sqlfire or gemfire 
 */
function alterHtmlContainer(prodname){
  if(CONST_BACKEND_PRODUCT_SQLFIRE == prodname.toLowerCase()){
    // Show HTML for following
    $('#subTabQueryStatistics').show();
  }else{
    // Hide HTML for following
    $('#subTabQueryStatistics').hide();
  }
  
}


// Regions Tree settings
var regionTreeConfigs = {
              check: { 
                enable: true 
                },
              data: { 
                simpleData: { 
                  enable: true 
                  }
                },
              callback: {
                //onClick: onClickHandler,
                onCheck: zTreeOnCheck //onRegionCheck
                }
              };

var regionsTreeElementName = "treeDemo";      // Regions tree html element name
var referenceToRegionsTreeElement; // Reference to Regions tree html element
//var dataBroswerRegions = new Array();    // Var to store cluster Regions
function initClusterRegions(){
  
  // JQuery Reference to Regions tree html element
  referenceToRegionsTreeElement = $("#"+regionsTreeElementName);
  
  // Initialize zTree with empty data
  $.fn.zTree.init(referenceToRegionsTreeElement, regionTreeConfigs, []);
  setCheck(regionsTreeElementName);
  $("#py").bind("change", setCheck);
  $("#sy").bind("change", setCheck);
  $("#pn").bind("change", setCheck);
  $("#sn").bind("change", setCheck);
  
  // update Regions tree
  updateDataBrowserRegionsData();
}

function updateDataBrowserRegionsData(){
  requestData = {};
  $.getJSON("pulse/dataBrowserRegions", requestData, function(data) {
    
    // set cluster name on tab link to cluster page
    if(data.clusterName != undefined && data.clusterName != null){
      $("#clusterNameLink").html(data.clusterName);
    }
    
    if(data.connectedFlag){
      // update clusterRegionsList
      clusterRegionsList = data.clusterRegions;
      createRegionsTree(clusterRegionsList);
    }
    
  }).error(repsonseErrorHandler);
  

  //setting refresh time interval to 10 sec
  //setTimeout("updateDataBrowserRegionsData()", 10000);
}

function createRegionsTree(regionsList){

  // Formulate region tree data
  var dataBroswerRegionsData = formRegionTreeData(regionsList);
  
  // destroy existing zTree
  $.fn.zTree.destroy();
  
  // recreate new zTree
  $.fn.zTree.init(referenceToRegionsTreeElement, regionTreeConfigs, dataBroswerRegionsData);
  setCheck(regionsTreeElementName);
  $("#py").bind("change", setCheck);
  $("#sy").bind("change", setCheck);
  $("#pn").bind("change", setCheck);
  $("#sn").bind("change", setCheck); 
  
}

// Display Colocated regions
function showColocatedRegions(){
  if($("#linkColocatedRegions").hasClass("disabledLink")){
    return false;
  }
  
  // Clear members list
  $("#membersList").html("");
  
  var selectedRegionFullPath = $("#selectedRegion").val();
  var regionsmembers = new Array();
  var colocatedRegions = new Array();
  
  // Determine selected region's members
  for(var i=0; i<clusterRegionsList.length; i++){
    if(selectedRegionFullPath == clusterRegionsList[i].fullPath){
      regionsmembers = clusterRegionsList[i].members;
      break;
    }
  }
  
  // Determine Co-located regions
  for(var i=0; i<regionsmembers.length; i++){
    var searchObject = JSON.stringify(regionsmembers[i]);
    for(var j=0; j<clusterRegionsList.length; j++){
      var members = JSON.stringify(clusterRegionsList[j].members);
      if(members.indexOf(searchObject) > -1){
        if($.inArray(clusterRegionsList[j], colocatedRegions) == -1){
          colocatedRegions.push(clusterRegionsList[j]);
        }
      }
    }
  }
  
  //console.log(colocatedRegions);
  // Generate colocated regions Tree
  createRegionsTree(colocatedRegions);
  var treeObj = $.fn.zTree.getZTreeObj(regionsTreeElementName);
  treeObj.expandAll(true);
  
  // Disable colocated link and enable show all regions link
  if($("#linkColocatedRegions").hasClass("enabledLink")){
    $("#linkColocatedRegions").addClass("disabledLink");
    $("#linkColocatedRegions").removeClass("enabledLink");

    $("#linkAllRegions").addClass("enabledLink");
    $("#linkAllRegions").removeClass("disabledLink");
  }

}

// Function to display all regions in the cluster
function showAllRegions(){
  
  if($("#linkAllRegions").hasClass("enabledLink")){

    // Generate colocated regions Tree
    createRegionsTree(clusterRegionsList);
    var treeObj = $.fn.zTree.getZTreeObj(regionsTreeElementName);
    treeObj.expandAll(false);
    
    $("#linkAllRegions").addClass("disabledLink");
    $("#linkAllRegions").removeClass("enabledLink");
    
    $("#linkColocatedRegions").addClass("disabledLink");
    $("#linkColocatedRegions").removeClass("enabledLink");
    
  }
  
}

// This function applies filter on regions tree
function applyFilterOnRegions() {

  // filterText
  var filterText = "";
  filterText = $('#filterTextRegion').val().toLowerCase();
  
  var filteredRegionsListNew = new Array();

  if (!isEmpty(filterText)) {
    // Determine filtered regions
    for(var i=0; i<clusterRegionsList.length; i++){
      if(clusterRegionsList[i].name.toLowerCase().indexOf(filterText) != -1){
        filteredRegionsListNew.push(clusterRegionsList[i]);
        
        var regionFullPath = clusterRegionsList[i].fullPath;
        regionFullPath = regionFullPath.substring(1);
        
        // add if parent of region is not present in filtered list
        while(regionFullPath.lastIndexOf('/') > 0 ){
          var parentFullPath = regionFullPath.substring(0, regionFullPath.lastIndexOf('/'));
          for(var j=0; j<clusterRegionsList.length; j++){
            if('/'+parentFullPath == clusterRegionsList[j].fullPath){
              if($.inArray(clusterRegionsList[j], filteredRegionsListNew) == -1){
                filteredRegionsListNew.push(clusterRegionsList[j]);
              }
              break;
            }
          }
          regionFullPath = parentFullPath;
        }
      }
    }
    
    // Generate filtered regions Tree
    createRegionsTree(filteredRegionsListNew);
    var treeObj = $.fn.zTree.getZTreeObj(regionsTreeElementName);
    treeObj.expandAll(true);
    
  } else {
    
    // Generate filtered regions Tree
    createRegionsTree(clusterRegionsList);
    var treeObj = $.fn.zTree.getZTreeObj(regionsTreeElementName);
    treeObj.expandAll(false);
    
  }
}

function onClickHandler(event, treeId, treeNode, clickFlag) {
  console.log("[ onClick ]&nbsp;&nbsp;clickFlag = " + clickFlag + 
      " (" + (clickFlag===1 ? "single selected": (clickFlag===0 ? "<b>cancel selected</b>" : "<b>multi selected</b>")) + ")");
  var treeObj = $.fn.zTree.getZTreeObj(regionsTreeElementName);
  
  // set check mark of node clicked
  // Syntax: treeObj.checkNode(treeNode, boolean, boolean, boolean)
  treeObj.checkNode(treeNode);  
}

function zTreeOnCheck(event, treeId, treeNode) {
  console.log(treeNode.tId + ", " + treeNode.name + "," + treeNode.checked);
  
  var treeObj = $.fn.zTree.getZTreeObj(regionsTreeElementName);
  var selectedNodes = treeObj.getCheckedNodes(true);
  var selectedRegions = new Array();
  
  // If only one region is selected then activate colocated regions link else 
  // deactivate link
  if(selectedNodes.length == 1){
    $("#linkColocatedRegions").addClass("enabledLink");
    $("#linkColocatedRegions").removeClass("disabledLink");
    // set selected region full path in html hidden input element
    $("#selectedRegion").val(getRegionFullPathFromTreeNode(selectedNodes[0],""));
  }else{
    // else 
    if($("#linkColocatedRegions").hasClass("enabledLink")){
      $("#linkColocatedRegions").addClass("disabledLink");
      $("#linkColocatedRegions").removeClass("enabledLink");
    }
  }
  
  // Display Members of selected regions
  for(var i=0; i<selectedNodes.length; i++){
    var selectedNode = selectedNodes[i];
    //console.log(getRegionFullPathFromTreeNode(selectedNode,""));
    // add to array
    selectedRegions.push(getRegionFullPathFromTreeNode(selectedNode,""));
  }
  console.log(selectedRegions);
  
  // make intersection of selected regions members
  var commonMembers = new Array();
  if(selectedRegions.length == 1){
    for(var i=0; i<clusterRegionsList.length; i++){
      if(clusterRegionsList[i].fullPath == selectedRegions[0]){
        commonMembers = clusterRegionsList[i].members;
        break;
      }
    }
  }else if(selectedRegions.length > 1){
    
    for(var i=0; i<clusterRegionsList.length; i++){
      if($.inArray(clusterRegionsList[i].fullPath, selectedRegions) != -1){
        
        if(commonMembers.length == 0){
          commonMembers = clusterRegionsList[i].members;
        }else{
          commonMembers = arrayIntersection(commonMembers, clusterRegionsList[i].members);
        }
        
      }
    }
    
  }
  
  // create members html
  var membersHTML = "";
  for(var j=0; j<commonMembers.length; j++){
    membersHTML += "<li>" +
      "<label for=\"Member"+j+"\">"+encodeMemberName(commonMembers[j].name)+"</label>" +
      "<input type=\"checkbox\" name=\"Member\" id=\"Member"+j+"\" class=\"styled\" value=\""+commonMembers[j].id+"\">" +
      "</li>";
  }
  $("#membersList").html(membersHTML);
  // Apply Check Box styles on members
  Custom.init();
  
  $('.ScrollPaneBlock').jScrollPane();
  
};

// Function to get full path of treeNode
function getRegionFullPathFromTreeNode(treeNode, path){
  
  if(treeNode.getParentNode() != null){
    path = getRegionFullPathFromTreeNode(treeNode.getParentNode(), path) + '/'+treeNode.name;
  }else{
    path += '/'+treeNode.name;
  }
  return path;
}

// function which returns common elements of two arrays
function arrayIntersection(A, B) {
  var result = new Array();
  for ( var i = 0; i < A.length; i++) {
    for ( var j = 0; j < B.length; j++) {
      if (A[i].id == B[j].id && $.inArray(A[i], result) == -1) {
        result.push(A[i]);
      }
    }
  }
  return result;
}



/*function displayMembersByRegionsSelected(regionName){
  console.log("regionName clicked");
  var membersHTML = "";
  for(var i=0; i<clusterRegionsList.length; i++){
    if(clusterRegionsList[i].name.endsWith(regionName)){
      var regionMembers = clusterRegionsList[i].members;
      for(var j=0; j<regionMembers.length; j++){
        membersHTML += "<li>" +
          "<label for=\"Member"+j+"\">"+regionMembers[j]+"</label>" +
          "<input type=\"checkbox\" name=\"Member"+j+"\" id=\"Member"+j+"\" class=\"styled\">" +
          "</li>";
      }  
    }
  }
  $("#membersList").html(membersHTML);
  // Apply Check Box styles on members
  Custom.init();
}*/

// Regions Tree event handlers 
var code;
function setCheck(treeElementName) {
        var zTree = $.fn.zTree.getZTreeObj(treeElementName),
        py = $("#py").attr("checked")? "p":"",
        sy = $("#sy").attr("checked")? "s":"",
        pn = $("#pn").attr("checked")? "p":"",
        sn = $("#sn").attr("checked")? "s":"",
        type = { "Y":py + sy, "N":pn + sn};
        zTree.setting.check.chkboxType = type;
        showCode('setting.check.chkboxType = { "Y" : "' + type.Y + '", "N" : "' + type.N + '" };');
}

function showCode(str) {
        if (!code) code = $("#code");
        code.empty();
        code.append("<li>"+str+"</li>");
}

$(function() 
        {
        $('span.level0').click(function(){
        $(this).parent('li').toggleClass('active');
        });
});

$(function() 
        {
        $('a.level0').dblclick(function(){
        $(this).parent('li').toggleClass('active');
        });
});
//Regions Tree event handlers - End

// this function prepares data for region tree
function formRegionTreeData(clusterRegions){
  
  if(clusterRegions.length == 0){
    return new Array();
  }
  
  var zTreeNodes = new Array();
  
  //console.log("clusterRegions B4 Sorting"); console.log(clusterRegions);
  // Sort regions based on full path
  clusterRegions.sort(dynamicSort("fullPath", "asc"));
  //console.log("clusterRegions After Sorting"); console.log(clusterRegions);
  
  for(var i=0; i< clusterRegions.length ; i++){
    var obj = {};
    obj["name"] = clusterRegions[i].name.substring(clusterRegions[i].name.indexOf("/") + 1);
    obj["isPartition"] = clusterRegions[i].isPartition;
    obj["open"] = false;
    obj["isParent"] = true;
    //obj["click"] = "displayMembersByRegionsSelected('"+clusterRegions[i].name.substring(clusterRegions[i].name.indexOf("/") + 1)+"');",
    obj["children"] = new Array(); 
    
    //console.log("object formed:");
    //console.log(obj);
    
    var regionFullPath = clusterRegions[i].fullPath;
    if(regionFullPath.indexOf("/") == 0){
      // remove first slash from full path
      regionFullPath = regionFullPath.substring(1); 
    }
    
    var indexOfSlash = regionFullPath.indexOf("/");
    
    if(indexOfSlash == -1){
      // Region is not subregion
      zTreeNodes.push(obj);
    }else{
      // Region is subregion
      
      var parentRegions = regionFullPath.split("/");
      //console.log("parentRegions.length : "+parentRegions.length);
      
      var j=0;
      var refToParent;
      // get reference to first parent
      for(var k=0; k < zTreeNodes.length; k++){
        if(zTreeNodes[k].name == parentRegions[j]){
          refToParent = zTreeNodes[k]; 
        }
      }
      
      // traverse in parents trail till last parent
      do{
        for(var k=0; k < refToParent.children.length; k++){
          if(refToParent.children[k].name == parentRegions[j]){
            refToParent = refToParent.children[k]; 
          }
        }
        j++;
      }while(j<parentRegions.length - 1);
        
        //console.log("refToParent : "); console.log(refToParent);
        
        // get list children of parent and add child/sub-region into it 
        var childrenList = refToParent["children"];
        //console.log("childrenList B4: "); console.log(childrenList);
        childrenList.push(obj);
        //console.log("childrenList After: "); console.log(childrenList);
        
    }
    
    //console.log("final data zTreeNodes : "); console.log(zTreeNodes);
    //dataBroswerRegions = zTreeNodes;
  }
  
  return zTreeNodes;
}

// Sort Array of objects based upon object property 
function dynamicSort(property, passedSortOrder) {
  var sortOrder = 1;
  if("desc" == passedSortOrder.toLowerCase()){
    sortOrder = -1;
  }else{
    sortOrder = 1;
  }
  
  if(property[0] === "-") {
      sortOrder = sortOrder == -1 ? 1 : -1;
      property = property.substr(1, property.length - 1);
  }
  return function (a,b) {
      var result = (a[property] < b[property]) ? -1 : (a[property] > b[property]) ? 1 : 0;
      return result * sortOrder;
  };
}

/*
 * Function to get basic details on Data Browser Page
 */
function getClusterBasicDetails(){
  $.getJSON("pulse/dataBrowserBasicDetails", function(data) { 

    $('#userName').html(data.userName);
    
    
  }).error(repsonseErrorHandler);
  setTimeout("getClusterBasic()", 5000); 
}

/*Slide Height History Panel*/
/*Slide Height*/

function addSlideEvents() {
  var slideHeight = 67; // px
  var elements = $('.wrap');

  elements.each(function() {
    var defHeight = $(this).height();
    if (defHeight >= slideHeight) {
      $(this).css('height', slideHeight + 'px');
      /* read more block */
      $(this).find('.read-more a').click(function() {
        var curHeight = $(this).parent().parent().height();

        if (curHeight == slideHeight) {
          $(this).parent().parent().animate({
            height : defHeight
          },{
            complete:function(){
              $('.queryHistoryScroll-pane').jScrollPane();
            }
          });
          $(this).toggleClass('remore_plus').toggleClass('remore_minus');
          /* apply the selected style to the container */
          $(this).parent().parent().parent().addClass('selectedHistory');
        } else {
          $(this).parent().parent().animate({
            height : slideHeight
          },{
            complete:function(){
                $('.queryHistoryScroll-pane').jScrollPane();
              }
          });
          $(this).toggleClass('remore_minus').toggleClass('remore_plus');
          /* removes the selected style to the container */
          $(this).parent().parent().parent().removeClass('selectedHistory');
        }

        return false;
      });
      /* remove Block */
      $('.remove a').click(function() {
        // $(this).parent('.wrap').remove();
        $(this).closest('.container').remove();
      });
    }

  });

}


