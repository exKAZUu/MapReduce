package jp.ac.nii;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

public class Main {
	public static void main(String[] args) {
		MapReduce mapReduce = new MapReduce();
		List<String> lines = new ArrayList<String>();
		lines.add("Apache Hadoop is an open-source software framework written in Java for distributed storage and distributed processing of very large data sets on computer clusters built from commodity hardware. All the modules in Hadoop are designed with a fundamental assumption that hardware failures (of individual machines, or racks of machines) are commonplace and thus should be automatically handled in software by the framework.");
		mapReduce.start(lines);
	}
}

class MapReduce {
	private TreeMap<String, List<Integer>> keyValueMap;
	private PrintStream out;

	public MapReduce() {
		keyValueMap = new TreeMap<String, List<Integer>>();
		try {
			out = new PrintStream("result.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void start(List<String> lines) {
		map(lines);
		for (Entry<String, List<Integer>> keyValue : keyValueMap.entrySet()) {
			reduce(keyValue.getKey(), keyValue.getValue());
		}
	}

	protected void map(List<String> lines) {
		for (String line : lines) {
			String[] words = line.split(" ");
			for (String word : words) {
				emit(word, 1);
			}
		}
	}

	protected void emit(String key, int value) {
		if (!keyValueMap.containsKey(key)) {
			keyValueMap.put(key, new ArrayList<Integer>());
		}
		List<Integer> list = keyValueMap.get(key);
		list.add(value);
	}

	protected void reduce(String key, List<Integer> values) {
		int sum = 0;
		for (Integer value : values) {
			sum += value;
		}
		write(key, sum);
	}

	protected void write(String key, int value) {
		out.println(key + "," + value);
	}
}