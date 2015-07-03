package jp.ac.nii;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

public class Main {
	public static void main(String[] args) {
		// 実行すると result.txt ファイルが作成されます。
		// 最も出現頻度が高い単語は"in"で3回です。
		MapReduce mapReduce = new MapReduce();
		List<String> lines = new ArrayList<String>();
		lines.add("Apache Hadoop is an open-source software framework written in Java for distributed storage and distributed processing of very large data sets on computer clusters built from commodity hardware. All the modules in Hadoop are designed with a fundamental assumption that hardware failures (of individual machines, or racks of machines) are commonplace and thus should be automatically handled in software by the framework.");
		mapReduce.start(lines);
	}
}

class MapReduce {
	/**
	 * KeyとValueのペアを記憶するためのフィールド。Keyで自動的にソートされる。
	 */
	private TreeMap<String, List<Integer>> keyValueMap;
	
	/**
	 * reduce の結果を出力するためのフィールド。
	 */
	private PrintStream out;

	public MapReduce() {
		keyValueMap = new TreeMap<String, List<Integer>>();
		try {
			out = new PrintStream("result.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * MapReduce を開始します。
	 * @param lines MapReduceの対象となるデータ。
	 */
	public void start(List<String> lines) {
		map(lines);
		for (Entry<String, List<Integer>> keyValue : keyValueMap.entrySet()) {
			reduce(keyValue.getKey(), keyValue.getValue());
		}
	}

	protected void map(List<String> lines) {
		// emit を使ってワードカウントのMapを実装してください。
	}

	protected void reduce(String key, List<Integer> values) {
		// write を使ってワードカウントのReduceを実装してください。
	}

	/**
	 * Mapの結果を出力します。
	 * @param key キー
	 * @param value バリュー
	 */
	protected void emit(String key, int value) {
		if (!keyValueMap.containsKey(key)) {
			keyValueMap.put(key, new ArrayList<Integer>());
		}
		List<Integer> list = keyValueMap.get(key);
		list.add(value);
	}

	/**
	 * Reduceの結果を出力します。
	 * @param key キー
	 * @param value バリュー
	 */
	protected void write(String key, int value) {
		out.println(key + "," + value);
	}
}