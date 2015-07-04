package jp.ac.nii;

import java.io.PrintStream;
import java.util.List;

public class Reducer {
	private PrintStream out;

	protected void reduce(String key, List<Integer> values) {
		// write を使ってワードカウントのReduceを実装してください。
		int sum = 0;
		for (Integer value : values) {
			sum += value;
		}
		write(key, sum);
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

	public void finish() {
		out.close();
	}

	public void setPrintStream(PrintStream out) {
		this.out = out;
	}
}