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
import org.json.JSONObject;
import utils.Constants;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static utils.EncodeData.isQuotedWithPlace;
import static utils.EncodeData.isRetweetedWithPlace;

public class DisasterBolt extends BaseRichBolt {
    private StanfordCoreNLP stanfordCoreNLP;
    private OutputCollector outputCollector;
    private Properties properties;
    private boolean done = false;
    private HashMap<String,JSONObject> tweets = new HashMap<>();

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
            //System.out.println(this.tweets);
            //System.out.println(this.tweets.size());
            /*
            if(!done) {
                done = true;
                reportResult();
            }
            */
            this.outputCollector.emit("tweet_stream", new Values("finish"));
        }
        else{
            //Map ret = (Map) o;
            //READ TWEET FROM SPOUT
            String ret = (String) o;
            JSONObject tweet = new JSONObject(ret);
            //System.out.println(tweet.getJSONObject("retweeted_status"));
            if(tweet.getString("place") != "null" || isRetweetedWithPlace(tweet) || isQuotedWithPlace(tweet)){
                //EXECUTE SENTIMENT ANALYSIS
                String sentiment = sentimentAnalysis(tweet.getString("text"));
                tweet.put("sentiment",sentiment);

                //this.tweets.put(tweet.getString("id"),tweet);
                //System.out.println(this.tweets.keySet());
                //SEND TO OUTPUT BOLT
                this.outputCollector.emit("tweet_stream", new Values(tweet.toString()));
            }
        }

    }

    private void reportResult() {
        //CREATE HTTP CLIENT
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost httppost = new HttpPost(Constants.POST_URL+"/test");

        //FORMAT REQUEST BODY OBJECT
        String jsonMap = new Gson().toJson(tweets);
        //System.out.println(this.tweets);
        try {
            httppost.setEntity(new StringEntity(jsonMap));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        /*
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String requestBody = objectMapper.writeValueAsString(tweets);
            System.out.println(requestBody);
            httppost.setEntity(new StringEntity(requestBody));

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        */
        httppost.setHeader("Content-type", "application/json");

        //SEND REQUEST
        try (CloseableHttpResponse response = httpClient.execute(httppost)) {
            HttpEntity entity = response.getEntity();
            EntityUtils.consume(entity);
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("tweet_stream",new Fields("tweet"));
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
