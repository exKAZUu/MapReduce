package jp.ac.nii;

import java.util.List;

/**
 * 以下の式の分子を計算するジョブのマッパーです。
 * 関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 */
public class AssociationCalcReducer extends Reducer<String, String> {
	@Override
	protected void reduce(String key, List<String> values) {
		// 分母データを探して取得する
		int denominator = 0;
		for (String value : values) {
			if (!value.contains(":")) {
				denominator = Integer.parseInt(value);
				break;
			}
		}
		// 分子データを処理する
		// このコードではfor文を2回実行してしまっているが、
		// Hadoop MapReduce ではセカンダリソートという方法を使って、1回のループで処理をする
		for (String value : values) {
			if (value.contains(":")) {
				String[] itemAndValue = value.split(":");
				int association = Integer.parseInt(itemAndValue[1])
						/ denominator;
				write(key, itemAndValue[0] + "," + association);
			}
		}
	}
}
