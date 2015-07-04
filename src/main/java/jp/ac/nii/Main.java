package jp.ac.nii;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {
	public static void main(String[] args) throws InstantiationException,
			IllegalAccessException, IOException {
		// 実行すると result.csv ファイルが作成されます。
		System.out.println("-------- First Stage --------");
		Job<String, String, Integer> mapReduce = new Job<String, String, Integer>();
		mapReduce.setMapper(DenominatorCalcMapper.class);
		mapReduce.setReducer(DenominatorCalcReducer.class);
		mapReduce.setNumberOfLinesPerMapper(100);
		mapReduce.setNumberOfReducers(5);
		mapReduce.start(readTextFile("goods_pair.csv"));
		Files.copy(new File("result.csv").toPath(),
				new File("denominator.csv").toPath(),
				StandardCopyOption.REPLACE_EXISTING);

		System.out.println();
		System.out.println("-------- Second Stage --------");
		Job<String, String, Integer> mapReduce2 = new Job<String, String, Integer>();
		mapReduce2.setMapper(NumeratorCalcMapper.class);
		mapReduce2.setReducer(NumeratorCalcReducer.class);
		mapReduce2.setNumberOfLinesPerMapper(100);
		mapReduce2.setNumberOfReducers(5);
		mapReduce.start(readTextFile("goods_pair.csv"));
		Files.copy(new File("result.csv").toPath(),
				new File("numerator.csv").toPath(),
				StandardCopyOption.REPLACE_EXISTING);

		System.out.println();
		System.out.println("-------- Second Stage --------");
		Job<String, String, String> mapReduce3 = new Job<String, String, String>();
		mapReduce3.setMapper(AssociationCalcMapper.class);
		mapReduce3.setReducer(AssociationCalcReducer.class);
		mapReduce3.setNumberOfLinesPerMapper(100);
		mapReduce3.setNumberOfReducers(5);

		List<String> lines = new ArrayList<String>();
		List<String> denominatorLines = readTextFile("denominator.csv");
		List<String> numeratorLines = readTextFile("numerator.csv");
		for (String line : denominatorLines) {
			String newLine = "D" + line;
			lines.add(newLine);
		}
		for (String line : numeratorLines) {
			String newLine = "N" + line;
			lines.add(newLine);
		}
		mapReduce3.start(lines);
		Files.copy(new File("result.csv").toPath(),
				new File("association.csv").toPath(),
				StandardCopyOption.REPLACE_EXISTING);
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