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

(function($){
	
	// Creating a jQuery plugin:
	
	$.generateFile = function(options){
		
		options = options || {};
		
		if(!options.script || !options.filename || !options.content){
			throw new Error("Please enter all the required config options!");
		}
		
		// Creating a 1 by 1 px invisible iframe:
		
		var iframe = $('<iframe>',{
			width:1,
			height:1,
			frameborder:0,
			css:{
				display:'none'
			}
		}).appendTo('body');

		var formHTML = '<form action="" method="post">'+
			'<input type="hidden" name="filename" />'+
			'<input type="hidden" name="content" />'+
			'</form>';
		
		// Giving IE a chance to build the DOM in
		// the iframe with a short timeout:
		
		setTimeout(function(){
		
			// The body element of the iframe document:
		
			var body = (iframe.prop('contentDocument') !== undefined) ?
							iframe.prop('contentDocument').body :
							iframe.prop('document').body;	// IE
			
			body = $(body);
			
			// Adding the form to the body:
			body.html(formHTML);
			
			var form = body.find('form');
			
			form.attr('action',options.script);
			form.find('input[name=filename]').val(options.filename);
			form.find('input[name=content]').val(options.content);
			
			// Submitting the form to download.php. This will
			// cause the file download dialog box to appear.
			
			form.submit();
		},50);
	};
	
})(jQuery);
