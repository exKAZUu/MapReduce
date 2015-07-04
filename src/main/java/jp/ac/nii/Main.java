package jp.ac.nii;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {
	public static void main(String[] args) throws InstantiationException,
			IllegalAccessException, IOException {
		// 実行すると result.csv ファイルが作成されます。
		// まず、各単語の出現頻度を数えます。
		System.out.println("-------- First Stage --------");
		Job<String, String, Integer> mapReduce = new Job<String, String, Integer>();
		mapReduce.setMapper(WordCountMapper.class);
		mapReduce.setReducer(WordCountReducer.class);
		mapReduce.setNumberOfLinesPerMapper(100);
		mapReduce.setNumberOfReducers(5);
		mapReduce.start(readTextFile("alice.txt"));
		Files.copy(new File("result.csv").toPath(),
				new File("word_frequency.csv").toPath());

		// 次に、得られた出現頻度の結果 (word_frequency.csv) から、単語を構成する文字数と出現頻度を計算します。
		// 例えば、1文字の単語の出現頻度は415回、2文字の単語の出現頻度は1814回になります。
		System.out.println();
		System.out.println("-------- Second Stage --------");
		Job<String, Integer, Integer> mapReduce2 = new Job<String, Integer, Integer>();
		mapReduce2.setMapper(WordLengthCountMapper.class);
		mapReduce2.setReducer(WordLengthCountReducer.class);
		mapReduce2.setNumberOfLinesPerMapper(100);
		mapReduce2.setNumberOfReducers(5);
		mapReduce2.start(readTextFile("word_frequency.csv"));
	}

	private static List<String> readTextFile(String fileName)
			throws FileNotFoundException {
		FileInputStream inputStream = new FileInputStream(fileName);
		Scanner scanner = new Scanner(inputStream);
		List<String> lines = new ArrayList<String>();
		while (scanner.hasNextLine()) {
			lines.add(scanner.nextLine().toLowerCase());
		}
		scanner.close();
		return lines;
	}
}