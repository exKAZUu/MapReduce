package jp.ac.nii;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;

public class Job<Input, Key, Value> {
	private Class<? extends Mapper<Input, Key, Value>> mapperClass;
	private Class<? extends Reducer<Key, Value>> reducerClass;
	private Class<? extends Partitioner<Key, Value>> partitionerClass;
	private int numberOfLinesPerMapper;
	private int numberOfReducers;

	public Job() {
		partitionerClass = null;
	}

	/**
	 * MapReduce を開始します。
	 * 
	 * @param lines
	 *            MapReduceの対象となるデータ。
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 */
	public void start(List<Input> lines) throws InstantiationException,
			IllegalAccessException, FileNotFoundException,
			UnsupportedEncodingException {
		if (mapperClass == null || reducerClass == null
				|| numberOfLinesPerMapper <= 0 || numberOfReducers <= 0) {
			throw new RuntimeException(
					"MapReduce does not be correctly initialized.");
		}

		Partitioner<Key, Value> partitioner;
		if (partitionerClass == null) {
			partitioner = new Partitioner<Key, Value>();
		} else {
			partitioner = partitionerClass.newInstance();
		}
		Shuffler<Key, Value> shuffler = new Shuffler<Key, Value>(partitioner,
				numberOfReducers);
		int numberOfMappers = lines.size() / numberOfLinesPerMapper;
		if (lines.size() % numberOfLinesPerMapper != 0) {
			numberOfMappers++;
		}

		// 並列実行するために複数の Mapper と Reducer を生成する
		ArrayList<Mapper<Input, Key, Value>> mappers = initializeMappers(
				shuffler, numberOfMappers);
		ArrayList<Reducer<Key, Value>> reducers = initializeReducers();

		// Mapperでmapを実行してから、Reducerでreduceを実行する
		mapParallel(lines, mappers);
		reduceParallel(shuffler, reducers);

		// Reducerの出力結果を一つのファイルにまとめる
		mergeResult();
	}

	/**
	 * 複数のMapperインスタンスを生成する。
	 */
	private ArrayList<Mapper<Input, Key, Value>> initializeMappers(
			Shuffler<Key, Value> shuffler, int numberOfMappers)
			throws InstantiationException, IllegalAccessException {
		ArrayList<Mapper<Input, Key, Value>> mappers = new ArrayList<Mapper<Input, Key, Value>>();
		for (int i = 0; i < numberOfMappers; i++) {
			Mapper<Input, Key, Value> mapper = mapperClass.newInstance();
			mapper.setShuffler(shuffler);
			mappers.add(mapper);
		}
		System.out.println("# Mappers: " + mappers.size());
		return mappers;
	}

	/**
	 * 複数のReducerインスタンスを生成する。
	 * 
	 * @throws UnsupportedEncodingException
	 */
	private ArrayList<Reducer<Key, Value>> initializeReducers()
			throws InstantiationException, IllegalAccessException,
			FileNotFoundException, UnsupportedEncodingException {
		ArrayList<Reducer<Key, Value>> reducers = new ArrayList<Reducer<Key, Value>>();
		for (int i = 0; i < numberOfReducers; i++) {
			Reducer<Key, Value> reducer = reducerClass.newInstance();
			reducer.setPrintStream(new PrintStream("result_" + i + ".csv",
					"SJIS"));
			reducers.add(reducer);
		}
		System.out.println("# Reducers: " + reducers.size());
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
	private void mapParallel(List<Input> lines,
			ArrayList<Mapper<Input, Key, Value>> mappers) {
		// 簡単のため並列ではなく逐次実行する
		for (int i = 0; i < mappers.size(); i++) {
			List<Input> subList = splitLines(lines, mappers, i);
			Mapper<Input, Key, Value> mapper = mappers.get(i);
			mapper.map(subList);
		}
	}

	/**
	 * 入力データを分割する。
	 */
	private List<Input> splitLines(List<Input> lines,
			ArrayList<Mapper<Input, Key, Value>> mappers, int i) {
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
	private void reduceParallel(Shuffler<Key, Value> shuffler,
			ArrayList<Reducer<Key, Value>> reducers) {
		// 簡単のため並列ではなく逐次実行する
		for (int i = 0; i < reducers.size(); i++) {
			Reducer<Key, Value> reducer = reducers.get(i);
			TreeMap<Key, List<Value>> keyValueMap = shuffler.getKeyValueMaps()
					.get(i);
			System.out.println("Reducer " + i + " will process "
					+ keyValueMap.size() + " records.");
			for (Entry<Key, List<Value>> keyValue : keyValueMap.entrySet()) {
				reducer.reduce(keyValue.getKey(), keyValue.getValue());
			}
			reducer.finish();
		}
	}

	/**
	 * 複数の result_X.csv ファイルを一つの result.csv にマージする。
	 * 
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 */
	private void mergeResult() throws FileNotFoundException,
			UnsupportedEncodingException {
		PrintStream out = new PrintStream("result.csv", "SJIS");
		for (int i = 0; i < numberOfReducers; i++) {
			FileInputStream intput = new FileInputStream("result_" + i + ".csv");
			Scanner scanner = new Scanner(intput, "SJIS");
			while (scanner.hasNextLine()) {
				out.println(scanner.nextLine());
			}
			scanner.close();
		}
		out.close();
	}

	public void setMapper(Class<? extends Mapper<Input, Key, Value>> mapperClass) {
		this.mapperClass = mapperClass;
	}

	public void setReducer(Class<? extends Reducer<Key, Value>> reducerClass) {
		this.reducerClass = reducerClass;
	}

	public void setPartitioner(
			Class<? extends Partitioner<Key, Value>> partitionerClass) {
		this.partitionerClass = partitionerClass;
	}

	public void setNumberOfLinesPerMapper(int numberOfLinesPerMapper) {
		this.numberOfLinesPerMapper = numberOfLinesPerMapper;
	}

	public void setNumberOfReducers(int numberOfReducers) {
		this.numberOfReducers = numberOfReducers;
	}
}