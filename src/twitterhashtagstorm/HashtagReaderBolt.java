/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterhashtagstorm;

import java.util.Map;

import twitter4j.*;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author SANDY
 */
public class HashtagReaderBolt implements IRichBolt {

    private OutputCollector collector;
    FileWriter writer;
    BufferedWriter bufferedWriter;
    List tag = new ArrayList();

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValueByField("tweet");
        for (HashtagEntity hashtage : tweet.getHashtagEntities()) {
            tag.add("#" + hashtage.getText() + "\t" + tweet.getUser().getName() + "\t@" + tweet.getUser().getScreenName() + "\t" + tweet.getCreatedAt() + "\t" + tweet.getUser().getLocation());
            this.collector.emit(new Values(hashtage.getText()));
        }
    }

    @Override
    public void cleanup() {
        for (int i = 0; i < tag.size(); i++) {
            try {
                writer = new FileWriter("twittertags.dat", true);
                bufferedWriter = new BufferedWriter(writer);
                //String str = "[" + i + "]=======>[#" + tag.get(i) + "]";
                String str = (String) tag.get(i);
                bufferedWriter.write(str);
                bufferedWriter.newLine();
                bufferedWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            //System.out.println("[" + i + "]=======>[#" + tag.get(i) + "]");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
