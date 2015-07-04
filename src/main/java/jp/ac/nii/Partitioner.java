package jp.ac.nii;

public class Partitioner<Key, Value> {
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
}
