package utils;

import org.json.JSONObject;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class EncodeData {



    public static boolean isQuotedWithPlace(JSONObject tweet) {
        return !tweet.isNull("quoted_status") && tweet.getJSONObject("quoted_status").getString("place") != "null";
    }

    public static boolean isRetweetedWithPlace(JSONObject tweet){
        return !tweet.isNull("retweeted_status") && tweet.getJSONObject("retweeted_status").getString("place") != "null";
    }
}
