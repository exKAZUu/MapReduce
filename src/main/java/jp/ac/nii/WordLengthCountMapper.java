package jp.ac.nii;

import java.util.List;

public class WordLengthCountMapper extends Mapper<String, Integer, Integer> {
	@Override
	public void map(List<String> lines) {
		for (String line : lines) {
			String[] wordAndCount = line.split(",");
			emit(wordAndCount[0].length(), Integer.parseInt(wordAndCount[1]));
		}
	}
}