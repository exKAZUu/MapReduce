package jp.ac.nii;

import java.util.List;

/**
 * 以下の式の分子を計算するジョブのマッパーです。
 *  関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 */
public class NumeratorCalcReducer extends Reducer<String, Integer> {
	@Override
	protected void reduce(String key, List<Integer> values) {
		int sum = 0;
		for (Integer value : values) {
			sum += value;
		}
		write(key, sum);
	}
}
