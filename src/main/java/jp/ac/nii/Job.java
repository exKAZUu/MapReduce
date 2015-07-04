package jp.ac.nii;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;

public class Job {
	private Class<Mapper> mapperClass;
	private Class<Reducer> reducerClass;
	private int numberOfLinesPerMapper;
	private int numberOfReducers;

	public Job() {
	}

	/**
	 * MapReduce を開始します。
	 * 
	 * @param lines
	 *            MapReduceの対象となるデータ。
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws FileNotFoundException
	 */
	public void start(List<String> lines) throws InstantiationException,
			IllegalAccessException, FileNotFoundException {
		if (mapperClass == null || reducerClass == null
				|| numberOfLinesPerMapper <= 0 || numberOfReducers <= 0) {
			throw new RuntimeException(
					"MapReduce does not be correctly initialized.");
		}

		Shuffler shuffler = new Shuffler(numberOfReducers);
		int numberOfMappers = /* TODO: numberOfLinesPerMapper で計算してください。 */;

		ArrayList<Mapper> mappers = initializeMappers(shuffler, numberOfMappers);
		ArrayList<Reducer> reducers = initializeReducers();

		map(lines, mappers);
		reduce(shuffler, reducers);

		concatenateResult();
	}

	private ArrayList<Mapper> initializeMappers(Shuffler shuffler,
			int numberOfMappers) throws InstantiationException,
			IllegalAccessException {
		// TODO: initializeReducers を参考に実装してください。
		// ただし、reducer.setPrintStream の代わりに、 mapper.setShuffler を呼ぶこと。
		return null;	// TODO: この行は削除すること
	}

	private ArrayList<Reducer> initializeReducers()
			throws InstantiationException, IllegalAccessException,
			FileNotFoundException {
		ArrayList<Reducer> reducers = new ArrayList<Reducer>();
		for (int i = 0; i < numberOfReducers; i++) {
			Reducer reducer = reducerClass.newInstance();
			reducer.setPrintStream(new PrintStream("result_" + i + ".csv"));
			reducers.add(reducer);
		}
		return reducers;
	}

	private void map(List<String> lines, ArrayList<Mapper> mappers) {
		for (int i = 0; i < mappers.size(); i++) {
			List<String> subList = splitLines(lines, mappers, i);
			// TODO: mapper に subList を渡して、mapを実行してください。
		}
	}

	private List<String> splitLines(List<String> lines,
			ArrayList<Mapper> mappers, int i) {
		int from = i * numberOfLinesPerMapper;
		int to = from + numberOfLinesPerMapper;
		if (i + 1 == mappers.size()) {
			to = lines.size();
		}
		return lines.subList(from, to);
	}

	private void reduce(Shuffler shuffler, ArrayList<Reducer> reducers) {
		for (int i = 0; i < reducers.size(); i++) {
			Reducer reducer = reducers.get(i);
			TreeMap<String, List<Integer>> keyValueMap = shuffler
					.getKeyValueMaps().get(i);
			for (Entry<String, List<Integer>> keyValue : keyValueMap.entrySet()) {
				reducer.reduce(keyValue.getKey(), keyValue.getValue());
			}
			reducer.finish();
		}
	}

	private void concatenateResult() throws FileNotFoundException {
		PrintStream out = new PrintStream("result.csv");
		for (int i = 0; i < numberOfReducers; i++) {
			FileInputStream intput = new FileInputStream("result_" + i + ".csv");
			// TODO: Main.readAliceText() を参考に result_X.csv を result.csv に結合するコードを記述する。
			// 単純に複数のファイルの中身を一つのファイルに結合するだけでよいです。
		}
		out.close();
	}

	public void setMapper(Class<Mapper> mapperClass) {
		this.mapperClass = mapperClass;
	}

	public void setReducer(Class<Reducer> reducerClass) {
		this.reducerClass = reducerClass;
	}

	public void setNumberOfLinesPerMapper(int numberOfLinesPerMapper) {
		this.numberOfLinesPerMapper = numberOfLinesPerMapper;
	}

	public void setNumberOfReducers(int numberOfReducers) {
		this.numberOfReducers = numberOfReducers;
	}
}