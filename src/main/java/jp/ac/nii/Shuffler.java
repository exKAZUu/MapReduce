package jp.ac.nii;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class Shuffler<Key, Value> {
	private List<TreeMap<Key, List<Value>>> keyValueMaps;
	private int numberOfReducers;

	public Shuffler(int numberOfReducers) {
		this.numberOfReducers = numberOfReducers;
		keyValueMaps = new ArrayList<TreeMap<Key, List<Value>>>();
		for (int i = 0; i < numberOfReducers; i++) {
			keyValueMaps.add(new TreeMap<Key, List<Value>>());
		}
	}

	public void shuffleAndSort(Key key, Value value) {
		// 何番目のReducerにKeyとValueのペアを送るか決める
		int index = getPartition(key, value, numberOfReducers);
		TreeMap<Key, List<Value>> keyValueMap = keyValueMaps.get(index);

		if (!keyValueMap.containsKey(key)) {
			keyValueMap.put(key, new ArrayList<Value>());
		}
		List<Value> list = keyValueMap.get(key);
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
	protected int getPartition(Key key, Value value, int numberOfReducers) {
		return Math.abs(key.hashCode()) % numberOfReducers;
	}

	public List<TreeMap<Key, List<Value>>> getKeyValueMaps() {
		return keyValueMaps;
	}
}