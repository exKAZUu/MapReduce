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
		ArrayList<Mapper> mappers = new ArrayList<Mapper>();
		for (int i = 0; i < numberOfMappers; i++) {
			Mapper mapper = mapperClass.newInstance();
			mapper.setShuffler(shuffler);
			mappers.add(mapper);
		}

		ArrayList<Reducer> reducers = new ArrayList<Reducer>();
		for (int i = 0; i < numberOfReducers; i++) {
			Reducer reducer = reducerClass.newInstance();
			reducer.setPrintStream(new PrintStream("result_" + i + ".csv"));
			reducers.add(reducer);
		}

		for (int i = 0; i < mappers.size(); i++) {
			int from = i * numberOfLinesPerMapper;
			int to = from + numberOfLinesPerMapper;
			if (i + 1 == mappers.size()) {
				to = lines.size();
			}
			Mapper mapper = mappers.get(i);
			mapper.map(lines.subList(from, to));
		}
		for (int i = 0; i < reducers.size(); i++) {
			Reducer reducer = reducers.get(i);
			TreeMap<String, List<Integer>> keyValueMap = shuffler
					.getKeyValueMaps().get(i);
			for (Entry<String, List<Integer>> keyValue : keyValueMap.entrySet()) {
				reducer.reduce(keyValue.getKey(), keyValue.getValue());
			}
			reducer.finish();
		}

		concatenateResult();
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