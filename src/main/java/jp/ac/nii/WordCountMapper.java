package jp.ac.nii;

import java.util.List;

public class WordCountMapper extends Mapper<String, String, Integer> {
	@Override
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
}