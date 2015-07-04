package jp.ac.nii;

import java.util.List;

/**
 * 以下の式の関連度を計算するジョブのMapperです。
 *   関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 */
public class AssociationCalcMapper extends Mapper<String, String, String> {
	@Override
	protected void map(List<String> lines) {
		for (String line : lines) {
			String[] items = line.substring(1).split(",");
			if (line.startsWith("D")) {
				// 分母データ
				emit(items[0], items[1]);
			} else  {
				// 分子データ
				emit(items[0], items[1] + ":" + items[2]);
			}			
		}
	}
}
