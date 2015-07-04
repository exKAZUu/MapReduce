package jp.ac.nii;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class Shuffler<Key, Value> {
	private final List<TreeMap<Key, List<Value>>> keyValueMaps;
	private final Partitioner<Key, Value> partitioner;
	private final int numberOfReducers;

	public Shuffler(Partitioner<Key, Value> partitioner, int numberOfReducers) {
		this.partitioner = partitioner;
		this.numberOfReducers = numberOfReducers;
		keyValueMaps = new ArrayList<TreeMap<Key, List<Value>>>();
		for (int i = 0; i < numberOfReducers; i++) {
			keyValueMaps.add(new TreeMap<Key, List<Value>>());
		}
	}

	public void shuffleAndSort(Key key, Value value) {
		// 何番目のReducerにKeyとValueのペアを送るか決める
		int index = partitioner.getPartition(key, value, numberOfReducers);
		TreeMap<Key, List<Value>> keyValueMap = keyValueMaps.get(index);

		if (!keyValueMap.containsKey(key)) {
			keyValueMap.put(key, new ArrayList<Value>());
		}
		List<Value> list = keyValueMap.get(key);
		list.add(value);
	}

	public List<TreeMap<Key, List<Value>>> getKeyValueMaps() {
		return keyValueMaps;
	}
}