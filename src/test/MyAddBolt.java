package test;

import java.util.Map;

import scala.annotation.varargs;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MyAddBolt extends BaseRichBolt {

	private static final long serialVersionUID = -7712937438175429127L;
	private OutputCollector collector;

	@Override
	public void execute(Tuple arg0) {
		System.out.println("MyAddBolt========================================================execute");
		// TODO Auto-generated method stub
		Integer a = arg0.getInteger(0);
		Integer b = arg0.getInteger(1);
		
		int c = a+b;
		
		if(c>10){
			collector.emit("addtenstrim",arg0,new Values(c));
			System.out.println("MyAddBolt==================================================================emit《大于10》");
		}else{
			collector.emit("printstrim",arg0,new Values(c));
			System.out.println("MyAddBolt==================================================================emit《小于10》");
		}
		collector.ack(arg0);
		System.out.println("MyAddBolt==================================================================ack");

	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector = arg2;
		System.out.println("MyAddBolt========================================================prepare");

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		System.out.println("MyAddBolt========================================================declareOutputFields");
		// TODO Auto-generated method stub
		arg0.declareStream("addtenstrim", new Fields("add"));
		arg0.declareStream("printstrim", new Fields("add"));
	}

}
