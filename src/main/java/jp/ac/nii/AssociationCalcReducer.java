package jp.ac.nii;

import java.util.List;

/**
 * 以下の式の関連度を計算するジョブのReducerです。
 *   関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 */
public class AssociationCalcReducer extends Reducer<String, String> {
	@Override
	protected void reduce(String key, List<String> values) {
		// 分母データを探して取得する
		double denominator = 0;
		for (String value : values) {
			if (!value.contains(":")) {
				denominator = Integer.parseInt(value);
				break;
			}
		}
		// 分子データを処理する
		// このコードではfor文を2回実行してしまっているが、
		// Hadoop MapReduce 2回ループを回すことができないので、
		// セカンダリソートという方法を使って、1回のループで処理をする（次回以降の講義で説明）
		for (String value : values) {
			if (value.contains(":")) {
				String[] itemAndValue = value.split(":");
				double association = Integer.parseInt(itemAndValue[1])
						/ denominator;
				// 関連度が2.5%を超える場合のみ出力
				if (association > 0.025) {
					write(key, itemAndValue[0] + "," + association);
				}
			}
		}
	}
}
