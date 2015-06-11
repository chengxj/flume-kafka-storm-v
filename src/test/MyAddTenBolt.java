package test;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MyAddTenBolt extends BaseRichBolt {

	private static final long serialVersionUID = -7955122534130800167L;
	private OutputCollector collector;

	@Override
	public void execute(Tuple arg0) {
		System.out.println("MyAddTenBolt========================================================execute");
		// TODO Auto-generated method stub
		int c = arg0.getInteger(0);
		c = c+10;
		collector.emit(arg0,new Values(c));
		collector.ack(arg0);
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		System.out.println("MyAddTenBolt========================================================prepare");
		// TODO Auto-generated method stub
		this.collector = arg2;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		System.out.println("MyAddTenBolt========================================================declareOutputFields");
		// TODO Auto-generated method stub
		arg0.declare(new Fields("addten"));

	}

}
