package jp.ac.nii;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {
	public static void main(String[] args) throws FileNotFoundException,
			InstantiationException, IllegalAccessException {
		// 実行すると result.csv ファイルが作成されます。
		// 最も出現頻度が高い単語は"the"で793回です。
		List<String> lines = readAliceText();
		Job mapReduce = new Job();
		mapReduce.setMapper(Mapper.class);
		mapReduce.setReducer(Reducer.class);
		mapReduce.setNumberOfLinesPerMapper(100);
		mapReduce.setNumberOfReducers(5);
		mapReduce.start(lines);
	}

	private static List<String> readAliceText() throws FileNotFoundException {
		FileInputStream inputStream = new FileInputStream("alice.txt");
		Scanner scanner = new Scanner(inputStream);
		List<String> lines = new ArrayList<String>();
		while (scanner.hasNextLine()) {
			lines.add(scanner.nextLine().toLowerCase());
		}
		scanner.close();
		return lines;
	}
}