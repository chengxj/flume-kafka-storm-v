package test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MySpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 6265292115877538083L;
	private SpoutOutputCollector collector;
	private List<Integer> list = new ArrayList<Integer>();
	private int i = 0;

	public MySpout() {
		for(int i=0;i<10;i++){
			list.add(i);
		}
	}

	@Override
	public void nextTuple() {
//		System.out.println("MySout========================================================nextTuple");
		// TODO Auto-generated method stub
		if(i<list.size()){
			int a = list.get(i);
//			System.out.println("MySout===========================================================================================a"+a);
			collector.emit(new Values(a,a),i);
//			System.out.println("MySpout======================================================collector.emit=================================a="+a+"i="+i);
			i++;
		}
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector = arg2;
//		System.out.println("MySout========================================================open");

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("a","b"));
		System.out.println("MySout========================================================declareOutputFields");

	}
	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		int in = (int) msgId;
		collector.emit(new Values(list.get(in),list.get(in)),in);
		System.out.println("MySout========================================================fail");
	}

}
