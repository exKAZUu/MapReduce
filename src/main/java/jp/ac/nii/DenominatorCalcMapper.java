package jp.ac.nii;

import java.util.List;

/**
 * 以下の式の分母を計算するジョブのマッパーです。
 *  関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 */
public class DenominatorCalcMapper extends Mapper<String, String, Integer> {
	@Override
	protected void map(List<String> lines) {
		for (String line : lines) {
			String[] itemPair = line.split(",");
			emit(itemPair[0], 1);
			emit(itemPair[1], 1);
		}
	}
}
