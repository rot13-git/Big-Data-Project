import clojure.lang.IFn;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DisasterLogBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private HashMap<String, JSONObject> tweets;
    public static final Logger LOG = LoggerFactory.getLogger(DisasterLogBolt.class);

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.tweets = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String res = tuple.getValue(0).toString();
        if(res.equals("finish")){
            LOG.info(String.valueOf(tweets.size()));
        }
        else if(tweets.size()>500){
            LOG.info(String.valueOf(tweets.size()));
        }
        else{
            JSONObject tweet = new JSONObject(res);
            tweets.put(tweet.getString("id"),tweet);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
