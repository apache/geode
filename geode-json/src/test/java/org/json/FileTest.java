package org.json;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;

public class FileTest {
  @Test
  public void testFile() throws IOException, JSONException {
    String ref = "[\n" + "  {\n" + "    \"_id\": \"58309f3bd307b72ae49a9b23\",\n"
        + "    \"index\": 0,\n" + "    \"guid\": \"5764ebd8-b333-469e-8d83-4eb5658f1566\",\n"
        + "    \"isActive\": true,\n" + "    \"balance\": \"$1,099.93\",\n"
        + "    \"picture\": \"http://placehold.it/32x32\",\n" + "    \"age\": 37,\n"
        + "    \"eyeColor\": \"blue\",\n" + "    \"name\": \"Barrera Wilkerson\",\n"
        + "    \"gender\": \"male\",\n" + "    \"company\": \"VURBO\",\n"
        + "    \"email\": \"barrerawilkerson@vurbo.com\",\n"
        + "    \"phone\": \"+1 (817) 429-2473\",\n"
        + "    \"address\": \"522 Vanderveer Street, Detroit, Wyoming, 4320\",\n"
        + "    \"about\": \"Et officia aute ullamco magna adipisicing non ut cupidatat cupidatat aliquip. Tempor occaecat ex ad dolore aliquip mollit ea esse ipsum. Est incididunt sunt commodo duis est. Reprehenderit in ut reprehenderit ad culpa ea fugiat et est adipisicing aliquip. Id mollit voluptate qui pariatur officia.\\r\\n\",\n"
        + "    \"registered\": \"2016-06-29T08:54:14 +07:00\",\n"
        + "    \"latitude\": -87.548434,\n" + "    \"longitude\": 64.251242,\n"
        + "    \"tags\": [\n" + "      \"aliqua\",\n" + "      \"ex\",\n" + "      \"sit\",\n"
        + "      \"magna\",\n" + "      \"dolor\",\n" + "      \"laborum\",\n" + "      \"non\"\n"
        + "    ],\n" + "    \"friends\": [\n" + "      {\n" + "        \"id\": 0,\n"
        + "        \"name\": \"Byers Pratt\"\n" + "      },\n" + "      {\n"
        + "        \"id\": 1,\n" + "        \"name\": \"Kennedy Contreras\"\n" + "      },\n"
        + "      {\n" + "        \"id\": 2,\n" + "        \"name\": \"Frazier Monroe\"\n"
        + "      }\n" + "    ],\n"
        + "    \"greeting\": \"Hello, Barrera Wilkerson! You have 3 unread messages.\",\n"
        + "    \"favoriteFruit\": \"banana\"\n" + "  },\n" + "  {\n"
        + "    \"_id\": \"58309f3b1f506440093a41d1\",\n" + "    \"index\": 1,\n"
        + "    \"guid\": \"de1a6cc9-f8b3-426e-b68a-cc30e1fff3c1\",\n" + "    \"isActive\": false,\n"
        + "    \"balance\": \"$3,397.60\",\n" + "    \"picture\": \"http://placehold.it/32x32\",\n"
        + "    \"age\": 32,\n" + "    \"eyeColor\": \"blue\",\n"
        + "    \"name\": \"Trisha Morris\",\n" + "    \"gender\": \"female\",\n"
        + "    \"company\": \"AMTAP\",\n" + "    \"email\": \"trishamorris@amtap.com\",\n"
        + "    \"phone\": \"+1 (805) 423-3375\",\n"
        + "    \"address\": \"495 Tampa Court, Libertytown, New Hampshire, 5177\",\n"
        + "    \"about\": \"Elit culpa Lorem dolor sit laborum ut ullamco ullamco nostrud reprehenderit adipisicing eiusmod. Aliqua quis dolor esse sint. Dolore in excepteur laborum anim ut consectetur. Nisi officia est eu ex ex id. Ipsum duis ullamco ad ut labore dolor. In amet tempor deserunt ullamco velit eu fugiat.\\r\\n\",\n"
        + "    \"registered\": \"2015-02-08T06:14:19 +08:00\",\n"
        + "    \"latitude\": -81.956277,\n" + "    \"longitude\": 143.685584,\n"
        + "    \"tags\": [\n" + "      \"cillum\",\n" + "      \"ullamco\",\n"
        + "      \"magna\",\n" + "      \"cillum\",\n" + "      \"voluptate\",\n"
        + "      \"magna\",\n" + "      \"exercitation\"\n" + "    ],\n" + "    \"friends\": [\n"
        + "      {\n" + "        \"id\": 0,\n" + "        \"name\": \"Fuentes Stout\"\n"
        + "      },\n" + "      {\n" + "        \"id\": 1,\n"
        + "        \"name\": \"Violet Vargas\"\n" + "      },\n" + "      {\n"
        + "        \"id\": 2,\n" + "        \"name\": \"Schmidt Wilder\"\n" + "      }\n"
        + "    ],\n" + "    \"greeting\": \"Hello, Trisha Morris! You have 4 unread messages.\",\n"
        + "    \"favoriteFruit\": \"strawberry\"\n" + "  },\n" + "  {\n"
        + "    \"_id\": \"58309f3beaef2f31339b3755\",\n" + "    \"index\": 2,\n"
        + "    \"guid\": \"0bf387b7-abc2-4828-becc-1269928f7c3d\",\n" + "    \"isActive\": false,\n"
        + "    \"balance\": \"$1,520.64\",\n" + "    \"picture\": \"http://placehold.it/32x32\",\n"
        + "    \"age\": 37,\n" + "    \"eyeColor\": \"blue\",\n"
        + "    \"name\": \"Deanna Santiago\",\n" + "    \"gender\": \"female\",\n"
        + "    \"company\": \"MEGALL\",\n" + "    \"email\": \"deannasantiago@megall.com\",\n"
        + "    \"phone\": \"+1 (916) 511-2291\",\n"
        + "    \"address\": \"919 Fayette Street, Homestead, Utah, 8669\",\n"
        + "    \"about\": \"Sit amet ex quis velit irure Lorem non quis aliquip dolor pariatur nulla Lorem officia. Deserunt officia sit velit labore sint nostrud elit aliquip labore ullamco consectetur id amet. Ullamco duis commodo sit incididunt. Fugiat consectetur ad incididunt officia. Sint cillum minim laborum laboris id cillum est exercitation in eiusmod qui.\\r\\n\",\n"
        + "    \"registered\": \"2015-11-18T08:39:28 +08:00\",\n" + "    \"latitude\": 79.105701,\n"
        + "    \"longitude\": -146.901754,\n" + "    \"tags\": [\n" + "      \"non\",\n"
        + "      \"ullamco\",\n" + "      \"cillum\",\n" + "      \"ipsum\",\n"
        + "      \"amet\",\n" + "      \"aliqua\",\n" + "      \"aliquip\"\n" + "    ],\n"
        + "    \"friends\": [\n" + "      {\n" + "        \"id\": 0,\n"
        + "        \"name\": \"Hanson Anderson\"\n" + "      },\n" + "      {\n"
        + "        \"id\": 1,\n" + "        \"name\": \"Pollard Soto\"\n" + "      },\n"
        + "      {\n" + "        \"id\": 2,\n" + "        \"name\": \"Barlow Campbell\"\n"
        + "      }\n" + "    ],\n"
        + "    \"greeting\": \"Hello, Deanna Santiago! You have 7 unread messages.\",\n"
        + "    \"favoriteFruit\": \"apple\"\n" + "  },\n" + "  {\n"
        + "    \"_id\": \"58309f3b49a68ad01346f27f\",\n" + "    \"index\": 3,\n"
        + "    \"guid\": \"d29c0dcc-48fb-4ca4-a63b-b47c0e6d6398\",\n" + "    \"isActive\": false,\n"
        + "    \"balance\": \"$2,069.96\",\n" + "    \"picture\": \"http://placehold.it/32x32\",\n"
        + "    \"age\": 29,\n" + "    \"eyeColor\": \"green\",\n"
        + "    \"name\": \"Brooks Gates\",\n" + "    \"gender\": \"male\",\n"
        + "    \"company\": \"TERRAGEN\",\n" + "    \"email\": \"brooksgates@terragen.com\",\n"
        + "    \"phone\": \"+1 (875) 483-2224\",\n"
        + "    \"address\": \"562 Noll Street, Kipp, Louisiana, 7659\",\n"
        + "    \"about\": \"Reprehenderit laboris mollit nulla commodo quis laborum commodo. Laborum aliquip laboris officia minim ipsum laborum ipsum reprehenderit quis laboris est sint culpa. Culpa magna aute mollit exercitation.\\r\\n\",\n"
        + "    \"registered\": \"2016-05-04T10:34:38 +07:00\",\n" + "    \"latitude\": 72.77079,\n"
        + "    \"longitude\": -134.291768,\n" + "    \"tags\": [\n" + "      \"est\",\n"
        + "      \"sunt\",\n" + "      \"laboris\",\n" + "      \"ea\",\n" + "      \"proident\",\n"
        + "      \"aute\",\n" + "      \"excepteur\"\n" + "    ],\n" + "    \"friends\": [\n"
        + "      {\n" + "        \"id\": 0,\n" + "        \"name\": \"Roxanne Morgan\"\n"
        + "      },\n" + "      {\n" + "        \"id\": 1,\n"
        + "        \"name\": \"Tamara Kelly\"\n" + "      },\n" + "      {\n"
        + "        \"id\": 2,\n" + "        \"name\": \"Cleveland Bush\"\n" + "      }\n"
        + "    ],\n" + "    \"greeting\": \"Hello, Brooks Gates! You have 1 unread messages.\",\n"
        + "    \"favoriteFruit\": \"banana\"\n" + "  },\n" + "  {\n"
        + "    \"_id\": \"58309f3be746700e9af9a645\",\n" + "    \"index\": 4,\n"
        + "    \"guid\": \"54382bd6-c476-469d-9e1c-e546f959db51\",\n" + "    \"isActive\": true,\n"
        + "    \"balance\": \"$2,012.57\",\n" + "    \"picture\": \"http://placehold.it/32x32\",\n"
        + "    \"age\": 40,\n" + "    \"eyeColor\": \"brown\",\n"
        + "    \"name\": \"Jackie Thomas\",\n" + "    \"gender\": \"female\",\n"
        + "    \"company\": \"HINWAY\",\n" + "    \"email\": \"jackiethomas@hinway.com\",\n"
        + "    \"phone\": \"+1 (843) 470-2096\",\n"
        + "    \"address\": \"910 Emerson Place, Gwynn, Federated States Of Micronesia, 4688\",\n"
        + "    \"about\": \"Id cupidatat laboris elit est eiusmod esse nostrud. Ex commodo nisi voluptate est nisi laborum officia sint incididunt pariatur qui deserunt ullamco. Fugiat proident magna ipsum sit sint id adipisicing sit nostrud labore sit officia. Eiusmod exercitation non enim excepteur amet irure ullamco consectetur cupidatat proident Lorem reprehenderit aliquip. Veniam esse dolor Lorem incididunt proident officia enim in incididunt culpa. Mollit voluptate commodo aliquip anim ipsum nostrud ut labore enim labore qui do minim incididunt. Quis irure proident voluptate nisi qui sunt aute duis irure.\\r\\n\",\n"
        + "    \"registered\": \"2014-08-03T09:21:43 +07:00\",\n" + "    \"latitude\": 84.871256,\n"
        + "    \"longitude\": 2.043339,\n" + "    \"tags\": [\n" + "      \"tempor\",\n"
        + "      \"ut\",\n" + "      \"deserunt\",\n" + "      \"esse\",\n" + "      \"nostrud\",\n"
        + "      \"dolore\",\n" + "      \"ex\"\n" + "    ],\n" + "    \"friends\": [\n"
        + "      {\n" + "        \"id\": 0,\n" + "        \"name\": \"Lois Walters\"\n"
        + "      },\n" + "      {\n" + "        \"id\": 1,\n"
        + "        \"name\": \"Brewer Buchanan\"\n" + "      },\n" + "      {\n"
        + "        \"id\": 2,\n" + "        \"name\": \"Mccormick Fleming\"\n" + "      }\n"
        + "    ],\n" + "    \"greeting\": \"Hello, Jackie Thomas! You have 2 unread messages.\",\n"
        + "    \"favoriteFruit\": \"banana\"\n" + "  }\n" + "]";

    JSONArray x1 = (JSONArray) new JSONTokener(
        new InputStreamReader(this.getClass().getResourceAsStream("/sample-01.json"))).nextValue();
    JSONArray x2 = (JSONArray) new JSONTokener(ref).nextValue();

    Assert.assertTrue(jsonEquals(x1, x2));
  }

  private boolean jsonEquals(JSONArray x1, JSONArray x2) throws JSONException {
    if (x1.length() != x2.length()) {
      return false;
    }

    for (int i = 0; i < x1.length(); i++) {
      Object element1 = x1.get(i);
      Object element2 = x2.get(i);
      if (!jsonEquals(element1, element2)) {
        return false;
      }
    }
    return true;
  }

  private boolean jsonEquals(JSONObject x1, JSONObject x2) throws JSONException {
    if (x1.length() != x2.length()) {
      return false;
    }
    Set<String> names = x1.keySet();
    for (String name : names) {
      if (!jsonEquals(x1.get(name), x2.get(name))) {
        return false;
      }
    }
    return true;
  }

  private boolean jsonEquals(Object element1, Object element2) throws JSONException {
    if (!element1.getClass().equals(element2.getClass())) {
      return false;
    }
    if (element1 instanceof JSONObject) {
      return jsonEquals((JSONObject) element1, (JSONObject) element2);
    }
    if (element1 instanceof JSONArray) {
      return jsonEquals((JSONArray) element1, (JSONArray) element2);
    }
    return element1.equals(element2);
  }
}
