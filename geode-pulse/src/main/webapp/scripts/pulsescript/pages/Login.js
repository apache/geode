/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

$(document).ready(function() {
      customizeUI();
});

// this function is called when login page loads
function pageOnLoad(){
  // get Pulse Version Details
  getPulseVersion();
  
  $.getJSON("authenticateUser", function(data) {
    
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
