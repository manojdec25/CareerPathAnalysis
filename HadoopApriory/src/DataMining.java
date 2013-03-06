import java.io.IOException;
import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*; 
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;


public class DataMining{
	
	public ArrayList<String> getInputsForItem(String item) throws IOException {	
		ArrayList<String> inputs = new ArrayList();
		Configuration config = HBaseConfiguration.create();
		HTable htable = new HTable(config, "AprioriTable");
		Get g = new Get(Bytes.toBytes("Key"));
		g.addFamily(Bytes.toBytes("input"));
		Result r = htable.get(g);
		NavigableMap<byte[], byte[]> map = r.getFamilyMap(Bytes.toBytes("input"));
		for (byte[] value : map.values()) {
			String stringvalue = Bytes.toString(value);
			String[] items = stringvalue.split(" ");
			boolean find = false;
			for (int i=0; i<items.length;i++){
				if (items[i].equals(item)){
					find = true;
					break;
				}
			}
			if (find) {
				inputs.add(stringvalue);
			}
		}
		return inputs;
	}
	
	public ArrayList<String> getFrequentItemsSets(String item) throws IOException {
		Configuration config = HBaseConfiguration.create();
		ArrayList<String> outputs = new ArrayList();
		HTable htable = new HTable(config, "AprioriTable");
		Get g = new Get(Bytes.toBytes("Output"));
		g.addFamily(Bytes.toBytes("count"));
		Result r = htable.get(g);
		NavigableMap<byte[], byte[]> map = r.getFamilyMap(Bytes.toBytes("count"));
		for (byte[] key : map.keySet()) {
			String stringkey = Bytes.toString(key);
			if (stringkey.indexOf('{') > -1) {
				stringkey = stringkey.substring(1, stringkey.length()-1);
			}
			String[] items = stringkey.split(" ");
			boolean find = false;
			for (int i=0; i<items.length;i++){
				if (items[i].equals(item)){
					find = true;
					break;
				}
			}
			if (find) {
				outputs.add(stringkey);
			}
		}
		return outputs;
	}
	
	public void getRecommandations(ArrayList<String> inputs, ArrayList<String> frequentItmesSets) {
		for (String frequentItemsSet : frequentItmesSets) {
			System.out.println("************************************************************************");
			System.out.println("Recommandations which includes " + frequentItemsSet);
			ArrayList<String> frequentItems = new ArrayList();
			String[] items = frequentItemsSet.split(" ");
			if (items.length > 2) {
				for (String item : items) {
					frequentItems.add(item);
				}
				
				ArrayList<String> inputItems = new ArrayList();
				for (String input : inputs) {
					String[] items2 = input.split(" ");
					for (String item2 : items2) {
						inputItems.add(item2);
					}
					if(inputItems.containsAll(frequentItems)) {
						System.out.println(input);
					}
				}		
			}	
		}
	}
	
	
	public static void main(String[] args) {
		System.out.println("Start Data Mining");
		DataMining dm = new DataMining();
		ArrayList<String> inputs = new ArrayList();
		ArrayList<String> outputs = new ArrayList();
		
		try {
			inputs = dm.getInputsForItem(args[0]);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			outputs = dm.getFrequentItemsSets(args[0]);
			OutputComparator comparator = new OutputComparator();
	        Collections.sort(outputs, comparator);
	        for (String output : outputs) {
	        	System.out.println(output);
	        }
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("Recommendations: ");
		dm.getRecommandations(inputs, outputs);
		
	}

}
