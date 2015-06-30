/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

$(document).ready(function() {
  
  // determine pulse back end product
  $.ajax({
    url : 'pulse/pulseProductSupport',
    type : 'GET',
    dataType : 'json',
    async : true,
    success : function(data) {
      // Set product name in cookie
      setCookie('productname', data.product, 1);

      // modify UI text as per requirement
      customizeUI();
      
    },
    error: function(jqXHR, textStatus, errorThrown){

      $('#user_name').prop('disabled', true);
      $('#user_password').prop('disabled', true);
      $('input[type="submit"]').prop('disabled', true);
      $("#errorText").html("Unable to determine backend product [Gemfire / SQLfire]<br>"+textStatus);
      $("#errorMasterBlock").show();
    }
  });
  
});

// this function is called when login page loads
function pageOnLoad(){
  // get Pulse Version Details
  getPulseVersion();
  
  $.getJSON("pulse/authenticateUser", function(data) {
    
    // return isUserLoggedIn
    if(data.isUserLoggedIn){
      // redirect user on Login Page 
      window.location.href = "clusterDetail.html";
    }else{
      
      // check if host and port available
      /*
      var cookieHost = getCookie('host');
      var cookiePort = getCookie('port');
      
      if(cookieHost != null && cookieHost != undefined && cookieHost != '' ){
        $("#locator_host").val(cookieHost);
        $("#locator_host").hide();
      }
      
      if(cookiePort != null && cookiePort != undefined && cookiePort != '' ){
        $("#locator_port").val(cookiePort);
        $("#locator_port").hide();
      }*/
      
      var errorId = GetParam('error');
      if(errorId != ''){
        //$("#errorText").html('<span>'+errorText+'</span>');
        displayErrorMessage(errorId);
      }
    }

  }).error(function(data){
    // Display Error
    var errorId = GetParam('error');
    if(errorId != ''){
      //$("#errorText").html('<span>'+errorText+'</span>');
      displayErrorMessage(errorId);
    }
  });
  
}

function displayErrorMessage(errorId){
  var errorText = getErrorMessage(errorId);
  if(errorText != ""){
    errorText = "<span>"+errorText+"<span>";
    $("#errorText").html(errorText);
    $("#errorMasterBlock").show();
  }
}
function getErrorMessage(errorId){
  var errorText = "";
  switch(errorId){
    case 'AUTH_FAILED' : 
      errorText = "Authentication Failed";
      break;
      
    case 'BAD_CREDS' : 
      errorText = "Incorrect Username or password";
      break;
      
    case 'CRED_EXP' : 
      errorText = "Your Account is expired";
      break;
      
    case 'ACC_LOCKED' : 
      errorText = "Your account is locked";
      break;
      
    case 'ACC_DISABLED' : 
      errorText = "Your account is disabled";
      break;
      
    case 'UNAUTH_ACCESS' : 
      errorText = "HTTP Error 401 : Unauthorized Access";
      break;
      
    default : 
      errorText = "";
  }
  
  return errorText;
}

// function to validate the user inputs
function validate(){
  var valid = true;
  var errorText = "";
  /*
  //validation logic here , change valid to false if fail
  if($("#user_name").val() == undefined || $("#user_name").val() == null || $("#user_name").val() == ''){
    valid = false;
    errorText += '<span>Please enter User Name</span>';
  }
  
  if($("#user_password").val() == undefined || $("#user_password").val() == null || $("#user_password").val() == ''){
    valid = false;
    errorText += '<span>Please enter Password</span>';
  }
  
  // optional fields
  if($("#locator_host").val() == undefined || $("#locator_host").val() == null || $("#locator_host").val() == ''){
    valid = false;
    errorText += '<span>Please enter Locator Host</span>';
  }
  
  if($("#locator_port").val() == undefined || $("#locator_port").val() == null || $("#locator_port").val() == ''){
    valid = false;
    errorText += '<span>Please enter Locator Port</span>';
  }*/
  
  if(!valid){
    $("#errorText").html(errorText);
    $("#errorMasterBlock").show();
  }else{
    $("#errorMasterBlock").hide();
  }
  return valid;
}
