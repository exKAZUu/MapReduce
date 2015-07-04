package jp.ac.nii;

class WordCountPartitioner extends Partitioner<String, Integer> {
	@Override
	protected int getPartition(String key, Integer value,
			int numberOfReducers) {
		return key.length() % numberOfReducers;
	}
}