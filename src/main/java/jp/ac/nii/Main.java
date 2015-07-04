package jp.ac.nii;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {
	public static void main(String[] args) throws InstantiationException,
			IllegalAccessException, IOException {
		// 以下の式の分母を計算します。
		// 関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
		// 出力結果例：
		//		ごま団子,5313
		//		カスタードシュークリーム,5162
		//		クリームあんみつ,5249		
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

		// 以下の式の分子を計算します。
		// 関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
		// 出力結果例：
		//		あんドーナツ,ところてん,152
		//		あんドーナツ,みつまめ,158
		//		あんドーナツ,オレンジピールチョコレート,125
		//      ...
		//      ところてん,あんドーナツ,152
		//
		// 注意：ペアの順序をひっくり返したデータも出力に含めてください。
		System.out.println();
		System.out.println("-------- Second Stage --------");
		Job<String, String, Integer> mapReduce2 = new Job<String, String, Integer>();
		mapReduce2.setMapper(NumeratorCalcMapper.class);
		mapReduce2.setReducer(NumeratorCalcReducer.class);
		mapReduce2.setNumberOfLinesPerMapper(100);
		mapReduce2.setNumberOfReducers(5);
		mapReduce2.start(readTextFile("goods_pair.csv"));
		Files.copy(new File("result.csv").toPath(),
				new File("numerator.csv").toPath(),
				StandardCopyOption.REPLACE_EXISTING);

		// 以下の式の関連度を計算します。
		// 関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
		// 出力結果例：
		//		ごま団子,うぐいすあんぱん,0.025032938076416336
		//		ごま団子,ねりようかん,0.02973837756446452
		//		ごま団子,カスタードシュークリーム,0.031055900621118012
		//
		// 注意：関連度が0.025を超える場合のみ出力してください
		System.out.println();
		System.out.println("-------- Third Stage --------");
		Job<String, String, String> mapReduce3 = new Job<String, String, String>();
		mapReduce3.setMapper(AssociationCalcMapper.class);
		mapReduce3.setReducer(AssociationCalcReducer.class);
		mapReduce3.setNumberOfLinesPerMapper(100);
		mapReduce3.setNumberOfReducers(5);

		// denominator.csv と numerator.csv を結合して読み込みます。
		// 正しく別が付くように、前者は各行に「D」を後者は各行に「N」を付加します。
		List<String> lines = new ArrayList<String>();
		List<String> denominatorLines = readTextFile("denominator.csv");
		List<String> numeratorLines = readTextFile("numerator.csv");
		for (String line : denominatorLines) {
			String newLine = "D" + line;
			lines.add(newLine);
		}
		for (String line : numeratorLines) {
			// TODO: denominatorLinesを参考に記述してください。
		}
		mapReduce3.start(lines);
		Files.copy(new File("result.csv").toPath(),
				new File("association.csv").toPath(),
				StandardCopyOption.REPLACE_EXISTING);
	}

	private static List<String> readTextFile(String fileName)
			throws FileNotFoundException {
		FileInputStream inputStream = new FileInputStream(fileName);
		Scanner scanner = new Scanner(inputStream, "SJIS");
		List<String> lines = new ArrayList<String>();
		while (scanner.hasNextLine()) {
			lines.add(scanner.nextLine().toLowerCase());
		}
		scanner.close();
		return lines;
	}
}