package jp.ac.nii;

import java.util.List;

/**
 * 以下の式の関連度を計算するジョブのReducerです。
 *   関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 */
public class AssociationCalcReducer extends /* TODO: 適切に実装してください。 */ {
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
				// TODO: 関連度が2.5%を超える場合のみ出力するようにコードを記述してください
			}
		}
	}
}
