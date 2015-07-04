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
		int numberOfMappers = lines.size() / numberOfLinesPerMapper;
		if (lines.size() % numberOfLinesPerMapper != 0) {
			numberOfMappers++;
		}

		// 並列実行するために複数の Mapper と Reducer を生成する
		ArrayList<Mapper> mappers = initializeMappers(shuffler, numberOfMappers);
		ArrayList<Reducer> reducers = initializeReducers();

		// Mapperでmapを実行してから、Reducerでreduceを実行する
		mapParallel(lines, mappers);
		reduceParallel(shuffler, reducers);

		// Reducerの出力結果を一つのファイルにまとめる
		mergeResult();
	}

	/**
	 * 複数のMapperインスタンスを生成する。
	 */
	private ArrayList<Mapper> initializeMappers(Shuffler shuffler,
			int numberOfMappers) throws InstantiationException,
			IllegalAccessException {
		ArrayList<Mapper> mappers = new ArrayList<Mapper>();
		for (int i = 0; i < numberOfMappers; i++) {
			Mapper mapper = mapperClass.newInstance();
			mapper.setShuffler(shuffler);
			mappers.add(mapper);
		}
		return mappers;
	}

	/**
	 * 複数のReducerインスタンスを生成する。
	 */
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

	/**
	 * 複数のMapperのmapを並列に実行する。
	 * 
	 * @param lines
	 *            入力データ
	 * @param mappers
	 *            Mapperインスタンスのリスト
	 */
	private void mapParallel(List<String> lines, ArrayList<Mapper> mappers) {
		for (int i = 0; i < mappers.size(); i++) {
			List<String> subList = splitLines(lines, mappers, i);
			Mapper mapper = mappers.get(i);
			mapper.map(subList);
		}
	}

	/**
	 * 入力データを分割する。
	 */
	private List<String> splitLines(List<String> lines,
			ArrayList<Mapper> mappers, int i) {
		int from = i * numberOfLinesPerMapper;
		int to = Math.min(from + numberOfLinesPerMapper, lines.size());
		return lines.subList(from, to);
	}

	/**
	 * 複数のReducerのreduceを並列に実行する。
	 * 
	 * @param shuffler
	 *            Shuffler インスタンス
	 * @param reducers
	 *            Reducer インスタンスのリスト
	 */
	private void reduceParallel(Shuffler shuffler, ArrayList<Reducer> reducers) {
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

	/**
	 * 複数の result_X.csv ファイルを一つの result.csv にマージする。
	 * 
	 * @throws FileNotFoundException
	 */
	private void mergeResult() throws FileNotFoundException {
		PrintStream out = new PrintStream("result.csv");
		for (int i = 0; i < numberOfReducers; i++) {
			FileInputStream intput = new FileInputStream("result_" + i + ".csv");
			Scanner scanner = new Scanner(intput);
			while (scanner.hasNextLine()) {
				out.println(scanner.nextLine());
			}
			scanner.close();
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