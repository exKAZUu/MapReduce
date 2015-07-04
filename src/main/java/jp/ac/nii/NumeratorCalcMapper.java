package jp.ac.nii;

import java.util.List;

/**
 * 以下の式の分子を計算するジョブのMapperです。
 *   関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 */
public class NumeratorCalcMapper extends Mapper<String, String, Integer> {
	@Override
	protected void map(List<String> lines) {
		for (String line : lines) {
			String[] itemPair = line.split(",");
			emit(itemPair[0] + "," + itemPair[1], 1);
			emit(itemPair[1] + "," + itemPair[0], 1);
		}
	}
}
