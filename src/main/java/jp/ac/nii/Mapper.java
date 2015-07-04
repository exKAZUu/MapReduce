package jp.ac.nii;

import java.util.List;

public abstract class Mapper<Input, Key, Value> {
	private Shuffler<Key, Value> shuffler;

	protected abstract void map(List<Input> lines);

	/**
	 * Mapの結果を出力します。
	 * 
	 * @param key
	 *            キー
	 * @param value
	 *            バリュー
	 */
	protected final void emit(Key key, Value value) {
		shuffler.shuffleAndSort(key, value);
	}

	protected final void setShuffler(Shuffler<Key, Value> shuffler) {
		this.shuffler = shuffler;
	}
}