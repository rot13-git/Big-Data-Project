import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import org.json.JSONObject;

import static utils.Constants.DELIMITER;
import static utils.Constants.REDIS_CHANNEL;
import static utils.EncodeData.isQuotedWithPlace;

public class DisasterRedisBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    RedisClient client;
    transient RedisConnection<String, String> connection;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        client = new RedisClient("localhost",6379);
        connection = client.connect();
    }

    @Override
    public void execute(Tuple tuple) {
        String tweet = tuple.getString(0);
        String tweet_worked = parseTweet(tweet);
        connection.publish(REDIS_CHANNEL, tweet_worked);
    }

    private String parseTweet(String tweet) {
        JSONObject tweet_obj = new JSONObject(tweet);
        StringBuilder out = new StringBuilder();

        String url_geo = retrieveGeoInfo(tweet_obj);

        out.append(tweet_obj.getString("text"))
        .append(DELIMITER)
        .append(url_geo)
        .append(DELIMITER)
        .append(tweet_obj.getString("sentiment"));

        return out.toString();
    }

    private String retrieveGeoInfo(JSONObject tweet) {

        if(tweet.getString("place") != "null"){
            return tweet.getJSONObject("place").getString("url");
        }
        else if(isQuotedWithPlace(tweet)){
            return tweet.getJSONObject("quoted_status").getJSONObject("place").getString("url");
        }
        else{
            return tweet.getJSONObject("retweeted_status").getJSONObject("place").getString("url");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
