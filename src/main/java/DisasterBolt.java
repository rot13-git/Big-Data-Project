import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONArray;
import org.json.JSONObject;
import org.mortbay.util.ajax.JSON;
import utils.Constants;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

import static utils.EncodeData.isQuotedWithPlace;
import static utils.EncodeData.isRetweetedWithPlace;

public class DisasterBolt extends BaseRichBolt {
    private StanfordCoreNLP stanfordCoreNLP;
    private OutputCollector outputCollector;
    private Properties properties;
    private boolean done = false;
    private HashMap<String,JSONObject> tweets = new HashMap<>();
    private HashMap<String,Integer> topHashtag = new HashMap<>();

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        this.properties = new Properties();
        properties.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        this.stanfordCoreNLP = new StanfordCoreNLP(properties);
    }

    @Override
    public void execute(Tuple tuple) {
        Object o = tuple.getValue(0);


        if(o.toString().equals("finish")){
            this.outputCollector.emit("tweet_stream", new Values("finish"));
        }
        else{
            //READ TWEET FROM SPOUT
            String ret = (String) o;
            JSONObject tweet = new JSONObject(ret);

            if(tweet.getString("place") != "null" || isRetweetedWithPlace(tweet) || isQuotedWithPlace(tweet)){

                //CALCULATE TOP HASHTAG
                JSONArray hashtags = extractHashtag(tweet);
                if(hashtags.length()>0)
                    this.outputCollector.emit("top_hashtag",new Values(hashtags));

                //EXTRACT TIME
                this.outputCollector.emit("time",new Values(tweet.getString("created_at")));

                //EXECUTE SENTIMENT ANALYSIS
                String sentiment = sentimentAnalysis(tweet.getString("text"));
                tweet.put("sentiment",sentiment);


                //EXTRACT DEVICE INFO
                String device = extractDevice(tweet.getString("source"));
                tweet.put("device",device);

                //SEND TO OUTPUT BOLT
                this.outputCollector.emit("tweet_stream", new Values(tweet.toString()));
            }
        }

    }

    private String extractDevice(String source) {
        String[] splitted = source.split(" ");
        String dev = splitted[splitted.length-1];
        dev = dev.substring(0,dev.length()-4);
        return dev;
    }

    private JSONArray extractHashtag(JSONObject tweet) {
        return tweet.getJSONObject("entities").getJSONArray("hashtags");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("tweet_stream",new Fields("tweet"));
        outputFieldsDeclarer.declareStream("top_hashtag",new Fields("hashtag"));
        outputFieldsDeclarer.declareStream("time",new Fields("time"));
    }

    public String sentimentAnalysis(String text){
        String cleared_text = clearTweet(text);
        String sentimentName="";
        CoreDocument coreDocument = new CoreDocument(cleared_text);
        stanfordCoreNLP.annotate(coreDocument);
        List<CoreSentence> sentences = coreDocument.sentences();
        for(CoreSentence sentence : sentences) {
            sentimentName = sentence.sentiment();
        }
        return sentimentName;
    }

    private String clearTweet(String tweet) {
        tweet = tweet.replaceAll("((www\\.[^\\s]+)|(https?://[^\\s]+))", "URL");

        //remove user names
        tweet = tweet.replaceAll("@[^\\s]+", "ATUSER");

        //remove # from hash tag
        tweet = tweet.replaceAll("#", "");

        //remove punctuation
        tweet = tweet.replaceAll("\\p{Punct}+", "");

        return tweet;
    }
}
