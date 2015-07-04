package jp.ac.nii;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;

public class Job<Input, Key, Value> {
	private Class<? extends Mapper<Input, Key, Value>> mapperClass;
	private Class<? extends Reducer<Key, Value>> reducerClass;
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
	public void start(List<Input> lines) throws InstantiationException,
			IllegalAccessException, FileNotFoundException {
		if (mapperClass == null || reducerClass == null
				|| numberOfLinesPerMapper <= 0 || numberOfReducers <= 0) {
			throw new RuntimeException(
					"MapReduce does not be correctly initialized.");
		}

		Shuffler<Key, Value> shuffler = new Shuffler<Key, Value>(
				numberOfReducers);
		int numberOfMappers = lines.size() / numberOfLinesPerMapper;

		ArrayList<Mapper<Input, Key, Value>> mappers = initializeMappers(
				shuffler, numberOfMappers);
		ArrayList<Reducer<Key, Value>> reducers = initializeReducers();

		map(lines, mappers);
		reduce(shuffler, reducers);

		concatenateResult();
	}

	private ArrayList<Mapper<Input, Key, Value>> initializeMappers(
			Shuffler<Key, Value> shuffler, int numberOfMappers)
			throws InstantiationException, IllegalAccessException {
		ArrayList<Mapper<Input, Key, Value>> mappers = new ArrayList<Mapper<Input, Key, Value>>();
		for (int i = 0; i < numberOfMappers; i++) {
			Mapper<Input, Key, Value> mapper = mapperClass.newInstance();
			mapper.setShuffler(shuffler);
			mappers.add(mapper);
		}
		return mappers;
	}

	private ArrayList<Reducer<Key, Value>> initializeReducers()
			throws InstantiationException, IllegalAccessException,
			FileNotFoundException {
		ArrayList<Reducer<Key, Value>> reducers = new ArrayList<Reducer<Key, Value>>();
		for (int i = 0; i < numberOfReducers; i++) {
			Reducer<Key, Value> reducer = reducerClass.newInstance();
			reducer.setPrintStream(new PrintStream("result_" + i + ".csv"));
			reducers.add(reducer);
		}
		return reducers;
	}

	private void map(List<Input> lines,
			ArrayList<Mapper<Input, Key, Value>> mappers) {
		for (int i = 0; i < mappers.size(); i++) {
			List<Input> subList = splitLines(lines, mappers, i);
			Mapper<Input, Key, Value> mapper = mappers.get(i);
			mapper.map(subList);
		}
	}

	private List<Input> splitLines(List<Input> lines,
			ArrayList<Mapper<Input, Key, Value>> mappers, int i) {
		int from = i * numberOfLinesPerMapper;
		int to = from + numberOfLinesPerMapper;
		if (i + 1 == mappers.size()) {
			to = lines.size();
		}
		return lines.subList(from, to);
	}

	private void reduce(Shuffler<Key, Value> shuffler,
			ArrayList<Reducer<Key, Value>> reducers) {
		for (int i = 0; i < reducers.size(); i++) {
			Reducer<Key, Value> reducer = reducers.get(i);
			TreeMap<Key, List<Value>> keyValueMap = shuffler.getKeyValueMaps()
					.get(i);
			for (Entry<Key, List<Value>> keyValue : keyValueMap.entrySet()) {
				reducer.reduce(keyValue.getKey(), keyValue.getValue());
			}
			reducer.finish();
		}
	}

	private void concatenateResult() throws FileNotFoundException {
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

	public void setMapper(Class<? extends Mapper<Input, Key, Value>> mapperClass) {
		this.mapperClass = mapperClass;
	}

	public void setReducer(Class<? extends Reducer<Key, Value>> reducerClass) {
		this.reducerClass = reducerClass;
	}

	public void setNumberOfLinesPerMapper(int numberOfLinesPerMapper) {
		this.numberOfLinesPerMapper = numberOfLinesPerMapper;
	}

	public void setNumberOfReducers(int numberOfReducers) {
		this.numberOfReducers = numberOfReducers;
	}
}