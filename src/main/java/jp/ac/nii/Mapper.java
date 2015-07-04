package jp.ac.nii;

import java.util.List;

public class Mapper {
	private Shuffler shuffler;

	public void map(List<String> lines) {
		// emit と isWord を使ってワードカウントのMapを実装してください。
		for (String line : lines) {
			String[] words = line.split(" ");
			for (String word : words) {
				if (isWord(word)) {
					emit(word, 1);
				}
			}
		}
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
		shuffler.shuffleAndSort(key, value);
	}

	public void setShuffler(Shuffler shuffler) {
		this.shuffler = shuffler;
	}
}