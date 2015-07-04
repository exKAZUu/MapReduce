package jp.ac.nii;

import java.util.List;

/**
 * 以下の式の分母を計算するジョブのMapperです。
 *   関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 */
public class DenominatorCalcMapper extends /* TODO: 適切に実装してください。 */ {
	@Override
	protected void map(List<String> lines) {
		for (String line : lines) {
			String[] itemPair = line.split(",");
			emit(itemPair[0], 1);
			emit(itemPair[1], 1);
		}
	}
}
