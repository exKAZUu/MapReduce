package jp.ac.nii;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class Shuffler {
	private List<TreeMap<String, List<Integer>>> keyValueMaps;
	private int numberOfReducers;

	public Shuffler(int numberOfReducers) {
		this.numberOfReducers = numberOfReducers;
		keyValueMaps = new ArrayList<TreeMap<String, List<Integer>>>();
		for (int i = 0; i < numberOfReducers; i++) {
			keyValueMaps.add(new TreeMap<String, List<Integer>>());
		}
	}

	public void shuffleAndSort(String key, int value) {
		// 何番目のReducerにKeyとValueのペアを送るか決める
		int index = getPartition(key, value, numberOfReducers);
		TreeMap<String, List<Integer>> keyValueMap = keyValueMaps.get(index);

		if (!keyValueMap.containsKey(key)) {
			keyValueMap.put(key, new ArrayList<Integer>());
		}
		List<Integer> list = keyValueMap.get(key);
		list.add(value);
	}

	/**
	 * 何番目のReducerにKeyとValueのペアを送るか決めます。
	 * 
	 * @param key
	 *            キー
	 * @param value
	 *            バリュー
	 * @param numberOfReducers
	 *            Reducerの個数
	 * @return Reducerのインデックス
	 */
	protected int getPartition(String key, Integer value, int numberOfReducers) {
		return Math.abs(key.hashCode()) % numberOfReducers;
	}

	public List<TreeMap<String, List<Integer>>> getKeyValueMaps() {
		return keyValueMaps;
	}
}