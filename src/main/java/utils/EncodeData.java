package utils;

import org.json.JSONObject;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class EncodeData {

    public static String encodeJson(HashMap<String, JSONObject> tweets) {
        String ret = "{";
        int count = 0;
        int size = tweets.size();

        for (Map.Entry<String, JSONObject> entry : tweets.entrySet()) {
            ret += "\"" + entry.getKey() + "\": \"" + Base64.getEncoder().encodeToString(entry.getValue().toString().getBytes()) + "\"";
            if (count < size - 1) ret += ", ";
            count++;
        }
        ret += "}";

        return ret;
    }
}
