package test;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class Print extends BaseRichBolt {

	private static final long serialVersionUID = 64499411706133149L;
	private OutputCollector collector;

	@Override
	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		Integer c = arg0.getInteger(0);
		System.out.println("Print==============================================================execute======"+c);
		if(c == 10){
			collector.fail(arg0);
		}
		collector.ack(arg0);

	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector = arg2;
		System.out.println("Print==============================================================prepare======");

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		System.out.println("Print==============================================================declareOutputFields======");
	}

}
