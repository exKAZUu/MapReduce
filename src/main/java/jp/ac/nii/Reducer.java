package jp.ac.nii;

import java.io.PrintStream;
import java.util.List;

public abstract class Reducer<Key, Value> {
	private PrintStream out;

	protected abstract void reduce(Key key, List<Value> values);

	/**
	 * Reduceの結果を出力します。
	 * 
	 * @param key
	 *            キー
	 * @param value
	 *            バリュー
	 */
	protected final void write(Object key, Object value) {
		out.println(key + "," + value);
	}

	protected final void finish() {
		out.close();
	}

	protected final void setPrintStream(PrintStream out) {
		this.out = out;
	}
}