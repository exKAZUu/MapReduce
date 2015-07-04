package jp.ac.nii;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;

public class Main {
	public static void main(String[] args) throws FileNotFoundException {
		// 実行すると result.csv ファイルが作成されます。
		// 最も出現頻度が高い単語は"the"で793回です。
		List<String> lines = readAliceText();
		MapReduce mapReduce = new MapReduce();
		mapReduce.start(lines);
	}

	private static List<String> readAliceText() throws FileNotFoundException {
		FileInputStream inputStream = new FileInputStream("alice.txt");
		Scanner scanner = new Scanner(inputStream);
		List<String> lines = new ArrayList<String>();
		while (scanner.hasNextLine()) {
			lines.add(scanner.nextLine().toLowerCase());
		}
		scanner.close();
		return lines;
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
			out = new PrintStream("result.csv");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * MapReduce を開始します。
	 * 
	 * @param lines
	 *            MapReduceの対象となるデータ。
	 */
	public void start(List<String> lines) {
		map(lines);
		for (Entry<String, List<Integer>> keyValue : keyValueMap.entrySet()) {
			reduce(keyValue.getKey(), keyValue.getValue());
		}
	}

	protected void map(List<String> lines) {
		// emit と  isWord を使ってワードカウントのMapを実装してください。
	}

	protected void reduce(String key, List<Integer> values) {
		// write を使ってワードカウントのReduceを実装してください。
	}

	private boolean isWord(String word) {
		for (int i = 0; i < word.length(); i++) {
			if (!Character.isAlphabetic(word.charAt(i))) {
				return false;
			}
		}
		return word.length() > 0;
	}

	/**
	 * Mapの結果を出力します。
	 * 
	 * @param key
	 *            キー
	 * @param value
	 *            バリュー
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
	 * 
	 * @param key
	 *            キー
	 * @param value
	 *            バリュー
	 */
	protected void write(String key, int value) {
		out.println(key + "," + value);
	}
}