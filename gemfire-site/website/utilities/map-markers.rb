# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

require 'rest_client'
require 'csv'
require 'json/ext'

rapportive_token = "BAgiX3BYOTZUVXlNalExclVBNWIyazVNcjBxK3UzdURNUnovTXVTamRZVTVmRmNsakw5WGZrUHJIYXFRaVV2YkRYaGctLWNFQjJLUmZNam05cjdmZDEzVGFPL3c9PQ==--71d66f8c1b8eafb0a8f31691b55b95fbce58857a"
rapportive_qs    = "?viewport_height=325&view_type=cv&user_email=tim.anglade%40gmail.com&client_version=ChromeExtension+rapportive+1.4.1&client_stamp=1382671311"

places = {}

# CSV.foreach("usergrid.csv") do |row|
# 	begin
# 		next if row[0].start_with?('Members ')
# 		next if row[0].start_with?('Email ')
# 		email = row[0]
# 		#puts email

# 		response = RestClient.get "http://profiles.rapportive.com/contacts/email/#{URI.escape(email)}#{rapportive_qs}", {"user-agent" => "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.71 Safari/537.36", "origin" => "https://mail.google.com", "referer" => "https://mail.google.com/mail/u/0/", "x-session-token" => rapportive_token}
# 		rapportive = JSON.parse(response.to_str)

# 		next unless rapportive['contact']['location']
# 		location = rapportive['contact']['location'].gsub(/\sBay\sArea$/,'').gsub(' Area,',',').gsub(/\sArea$/,'').gsub(/^Greater\s/,'')

# 		puts location

# 		places[location] ? places[location] += 1 : places[location] = 1
# 	rescue => e
# 	 	puts e
# 	 	sleep 10
# 	 	retry
# 	end
# end

# places.each do |place, count|
# 	puts "\"#{place}\",#{count}"
# end

places2 = {"Sydney, Australia"=>2, "Provo, Utah"=>1, "San Francisco Bay"=>27, "Bay of Plenty, New Zealand"=>1, "Kenya"=>1, "Istanbul, Turkey"=>2, "Iasi County, Romania"=>1, "Vancouver, Canada"=>2, "United Kingdom"=>3, "Jacksonville, Florida"=>1, "Austin, Texas"=>6, "Brazil"=>1, "Hartford, Connecticut"=>2, "Dublin"=>1, "Melbourne, Australia"=>1, "Egypt"=>1, "Los Angeles"=>7, "Seoul, Korea"=>1, "Sri Lanka"=>2, "Denver"=>6, "Quebec, Canada"=>1, "New York City"=>5, "Minneapolis-St. Paul"=>1, "Turkey"=>2, "Raleigh-Durham, North Carolina"=>3, "Korea"=>4, "Jakarta Selatan"=>1, "Sarasota, Florida"=>1, "Bologna, Italy"=>1, "Philadelphia"=>2, "United States"=>1, "Fort Collins, CO"=>2, "Toronto, Canada"=>2, "Seattle"=>6, "Israel"=>1, "Kingston upon Thames, United Kingdom"=>1, "Valencia, Spain"=>1, "London, United Kingdom"=>2, "Washington D.C. Metro"=>1, "Phoenix, Arizona"=>2, "Portland, Oregon"=>1, "Madison, Wisconsin"=>1, "Greensboro/Winston-Salem, North Carolina"=>1, "San Francisco, CA"=>1, "Penang, Malaysia"=>1, "Asheville, North Carolina"=>1, "China"=>1, "Santa Barbara, California"=>1, "Singapore"=>2, "Norfolk, Virginia"=>1, "Hong Kong"=>3, "Paris, France"=>1, "Madrid, Spain"=>1, "Pune, India"=>3, "nashville, TN"=>1, "Dublin, Ireland"=>1, "Miami/Fort Lauderdale"=>1, "Detroit"=>1, "720 32nd St, Oakland, CA 94609"=>1, "Pensacola, Florida"=>1, "Bucharest, Romania"=>1, "Cleveland/Akron, Ohio"=>1, "Bengaluru, India"=>7, "Ireland"=>1, "Socorro, New Mexico"=>1, "Peru"=>1, "Copenhagen, Denmark"=>1, "Somewhere"=>1, "Istanbul, Turkey"=>1, "Shanghai City, China"=>1, "Atlanta"=>2, "Thiruvananthapuram, India"=>1, "Chennai, Tamil Nadu"=>1, "Melbourne, Florida"=>1, "Maryland"=>1, "Hyderabad, India"=>1, "Sao Paulo, Brazil"=>1, "Utrecht, Netherlands"=>1, "Parbhani, India"=>1, "Mumbai, India"=>1, "Bangalore, Karnataka, India"=>1, "Porto, Portugal"=>1, "Beijing, China"=>1, "Boston"=>1, "Dallas/Fort Worth"=>1, "Greece"=>1, "Mountains"=>1, "Stanford, California"=>1, "Japan"=>1}

countries = {}

places2.each do |place, count|
	begin
		response = RestClient.get "http://maps.googleapis.com/maps/api/geocode/json?address=#{URI.escape(place)}&sensor=false"
		j = JSON.parse(response.to_str)
		geocoding = j["results"][0]["geometry"]
		j["results"][0]["address_components"].each do |c|
			next unless c["types"].include?("country")
			countries[c["short_name"]] ? countries[c["short_name"]] += 1 : countries[c["short_name"]] = 1
		end

		#puts "new google.maps.Marker({\nmap:map,\nanimation: google.maps.Animation.DROP,\nposition: new google.maps.LatLng(#{geocoding['location']['lat']},#{geocoding['location']['lng']})\n});"
		sleep 0.1
	rescue => e
 		puts e
	end
end

puts "Found #{countries.size} countries"
