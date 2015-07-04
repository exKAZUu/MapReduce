package jp.ac.nii;

import java.util.List;

public class WordLengthCountReducer extends Reducer<Integer, Integer> {
	@Override
	protected void reduce(Integer key, List<Integer> values) {
		int sum = 0;
		for (Integer value : values) {
			sum += value;
		}
		write(key, sum);
	}
}