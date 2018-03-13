/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterhashtagstorm;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.tuple.Fields;

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
public class HashtagCounterBolt implements IRichBolt {

    Map<String, Integer> counterMap;
    private OutputCollector collector;
    FileWriter writer;
    BufferedWriter bufferedWriter;
    List tag = new ArrayList();

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.counterMap = new HashMap<String, Integer>();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String key = tuple.getString(0);

        if (!counterMap.containsKey(key)) {
            counterMap.put(key, 1);
        } else {
            Integer c = counterMap.get(key) + 1;
            counterMap.put(key, c);
        }

        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
            tag.add("#" + entry.getKey() + "\t" + entry.getValue());
        }
        for (int j = 0; j < tag.size(); j++) {
            try {
                writer = new FileWriter("twitterresult.dat", true);
                bufferedWriter = new BufferedWriter(writer);
                String str = (String) tag.get(j);
                bufferedWriter.write(str);
                bufferedWriter.newLine();
                bufferedWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
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
