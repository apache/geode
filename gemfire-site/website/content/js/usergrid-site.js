// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
var start = 40;
var end = 210;

function setHeaderForScroll(scrollTop) {
  if ( ($( window ).width() > 768) && ($('header.secondary').length == 0) ) {

    if(scrollTop > start) {
        opacity = (Math.floor(scrollTop) - start)/end;
        //console.log(opacity);
        percent = Math.min(opacity, 1)
        red = Math.floor(36 + (52-36) * percent);
        green = Math.floor(129 - (129-73) * percent);
        blue = Math.floor(166 - (166-94) * percent);
        blur = Math.floor(2 * percent);
    } else {
        opacity = 0;
        red = 36;
        green = 129;
        blue = 166;
        blur = 0;
    }
    $("#home-logo").css("opacity", opacity);
    $("header").css("box-shadow", "0px 1px "+blur+"px rgb("+red+','+green+','+blue+")");
  } else {
    $("#home-logo").css("opacity", 1);
    $("header").css("box-shadow", "0px 1px 2px rgb(52,73,94)");
  }
}

$(document).ready(function() {

    $('table').addClass('table');

    // Detect initial scroll on page load
    setHeaderForScroll($("body").scrollTop());

    //reduce the opacity of the banner if the page is scrolled.
    $(window).scroll(function () {
      setHeaderForScroll($("body").scrollTop());
    });

    // $(".navbar-toggle").bind("click", function(){
    //     if($(".collapse").hasClass("collapse"))
    //         $("#home-logo").css("opacity", 100);
    //     else
    //         $("#home-logo").css("opacity", 0);
    // });


});
