import org.apache.commons.lang.time.DateUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import org.json.JSONArray;
import org.json.JSONObject;

import static utils.Constants.*;
import static utils.EncodeData.isQuotedWithPlace;

public class DisasterRedisBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    RedisClient client;
    transient RedisConnection<String, String> connection;
    private HashMap<String,Integer> hashtagCount = new HashMap<>();
    private HashMap<String,Integer> top5Hashtag = new HashMap<>();
    private TreeMap<Date,Integer> tweetByHour = new TreeMap<>();
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        client = new RedisClient("localhost",6379);
        connection = client.connect();
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals("top_hashtag")){
            updateHashtag((JSONArray) tuple.getValue(0));
            Map m = updateTopTenHashtag();
            String out = m.keySet()+DELIMITER+m.values();
            connection.publish(ANALYSIS_CHANNEL,out);
        }
        else if(tuple.getSourceStreamId().equals("time")){
            updateTweetByHour(tuple.getString(0));
            String out = "TIME"+DELIMITER+tweetByHour.keySet()+DELIMITER+tweetByHour.values();
            connection.publish(ANALYSIS_CHANNEL,out);
        }
        else{
            String tweet = tuple.getString(0);
            String tweet_worked = parseTweet(tweet);
            connection.publish(REDIS_CHANNEL, tweet_worked);
        }

    }

    private HashMap<String, Integer> updateTopTenHashtag() {
        PriorityQueue<String> topN = new PriorityQueue<String>(10, new Comparator<String>() {
            public int compare(String s1, String s2) {
                return Integer.compare(hashtagCount.get(s1), hashtagCount.get(s2));
            }
        });

        for(String key:hashtagCount.keySet()){
            if (topN.size() < 10)
                topN.add(key);
            else if (hashtagCount.get(topN.peek()) < hashtagCount.get(key)) {
                topN.poll();
                topN.add(key);
            }
        }
        HashMap<String,Integer> top10 = new HashMap<>();
        for(String k : topN){
            top10.put(k,hashtagCount.get(k));
        }
        return top10;
    }

    private void updateTweetByHour(String time) {
        SimpleDateFormat parserSDF = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
        try {
            Date date = DateUtils.truncate(parserSDF.parse(time), Calendar.MINUTE);
            if(tweetByHour.containsKey(date))
                tweetByHour.put(date,tweetByHour.get(date)+1);
            else
                tweetByHour.put(date,1);
        } catch (ParseException e) {}
    }

    private void updateHashtag(JSONArray hashtags) {
        for(int i = 0; i < hashtags.length();i++ ){
            JSONObject hashtag_obj = hashtags.getJSONObject(i);
            String hashtag = hashtag_obj.getString("text");
            if(hashtagCount.containsKey(hashtag))
                hashtagCount.put(hashtag,hashtagCount.get(hashtag)+1);
            else
                hashtagCount.put(hashtag,1);
        }
    }
    private String parseTweet(String tweet) {
        JSONObject tweet_obj = new JSONObject(tweet);
        StringBuilder out = new StringBuilder();

        JSONArray geo = retrieveGeoInfo(tweet_obj);

        out.append(tweet_obj.getString("text"))
                .append(DELIMITER)
                .append(geo)
                .append(DELIMITER)
                .append(tweet_obj.getString("id"))
                .append(DELIMITER)
                .append(tweet_obj.getString("sentiment"))
                .append(DELIMITER)
                .append(tweet_obj.getString("device"));

        return out.toString();
    }

    private JSONArray retrieveGeoInfo(JSONObject tweet) {

        if(tweet.getString("place") != "null"){
            return tweet.getJSONObject("place").getJSONObject("bounding_box").getJSONArray("coordinates");
        }
        else if(isQuotedWithPlace(tweet)){
            return tweet.getJSONObject("quoted_status").getJSONObject("place").getJSONObject("bounding_box").getJSONArray("coordinates");
        }
        else{
            return tweet.getJSONObject("retweeted_status").getJSONObject("place").getJSONObject("bounding_box").getJSONArray("coordinates");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
