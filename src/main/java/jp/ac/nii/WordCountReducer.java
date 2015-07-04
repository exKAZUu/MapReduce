package jp.ac.nii;

import java.util.List;

public class WordCountReducer extends Reducer<String, Integer> {
	@Override
	protected void reduce(String key, List<Integer> values) {
		int sum = 0;
		for (Integer value : values) {
			sum += value;
		}
		write(key, sum);
	}
}