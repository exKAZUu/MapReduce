package jp.ac.nii;

class WordCountPartitioner extends Partitioner<String, Integer> {
	@Override
	protected int getPartition(String key, Integer value,
			int numberOfReducers) {
		// TODO: hashCode() を使わないPartitionを作って、hashCode()版と比較しましょう。
	}
}