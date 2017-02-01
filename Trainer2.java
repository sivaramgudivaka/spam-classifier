package fin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import de.bwaldvogel.liblinear.Predict;
import de.bwaldvogel.liblinear.Train;

public class Trainer2 {
	static List<String> trainList = new ArrayList<>();
	static List<String> testList = new ArrayList<>();
	static HashSet<String> spams = new HashSet<>();
	static HashSet<String> hams = new HashSet<>();
    
	public static void main(String[] args) throws Exception
	{
		getDirFiles("C:/Users/Sivaram/Downloads/trec07p/trec07p/clean/");
		//getMatrix();
		List<String> vocab = FileUtils.readLines(new File("D:/HW-7/vocabulary"));
		getTopWords(vocab);
		trainer(vocab);
		writeDocNames();
		System.out.println("done!");
	}
	
	private static void writeDocNames() throws Exception {
		List<String> lines = FileUtils.readLines(new File("D:/HW-7/result"));
		FileWriter fw = new FileWriter(new File("D:/HW-7/result_final"));
		lines.remove(0);
		for(int i = 0; i < lines.size(); i++)
		{
			String line = lines.get(i);
			fw.write(line + " " + testList.get(i) + "\n");
		}
		fw.close();
		System.out.println("generating doc names and respective probabilites -- successful");
	}

	private static void getTopWords(List<String> vocabulary) throws Exception {
		String modelPath = "D:/HW-7/model";
		File model = new File(modelPath);
		List<String> lines = FileUtils.readLines(model);
		HashMap<String, Double> scoreMap = new HashMap<>();
		for(int i = 0; i < lines.size(); i++)
		{
			String line = lines.get(i).trim();
			scoreMap.put(vocabulary.get(i), Double.parseDouble(line));
		}
		writeToFile(scoreMap);
	}

	public static void trainer(List<String> vocabulary) throws Exception
	{
		String trainArff = "D:/HW-7/train", testArff = "D:/HW-7/test";
		String model = "D:/HW-7/model";
		String result = "D:/HW-7/result";

		String[] args1 = { "-s", "0", trainArff, model };
		Train.main(args1);

		String[] args2 = { "-b", "1", testArff, model, result };
		Predict.main(args2);		
		System.out.println("generating results --successful");
		getTopWords(vocabulary);
	}

	private static void getMatrix() throws Exception
	{
		List<String> totalDocs = new ArrayList<>();
		totalDocs.addAll(trainList);
		totalDocs.addAll(testList);
		HashMap<String, HashSet<String>> wordMap = new HashMap<>();
		HashSet<String> temp = new HashSet<>();
		//get vocabulary and generate the <doc, [words]> hashmap
		for(String doc : totalDocs)
		{
			HashSet<String> trms = getTermVector(doc);
			temp.addAll(trms);
			wordMap.put(doc, trms);
		}
		System.out.println("extracted term vectors from ES! you can close it now.");
		List<String> vocabulary = new ArrayList<String>(temp);
		Collections.sort(vocabulary);
		writeVocabToFile(vocabulary);
		//free temp
		temp = new HashSet<>();
		System.out.println("vocabulary : "+vocabulary.size());
		//iterate over the docs and write respective matrix content
		writeMatrix("train", wordMap, vocabulary);
		writeMatrix("test", wordMap, vocabulary);
		trainer(vocabulary);
	}

	private static void writeVocabToFile(List<String> vocabulary) throws IOException {
		FileWriter fw = new FileWriter("D:/HW-7/vocabulary");
		for(String word : vocabulary)
			fw.write(word+"\n");
		fw.close();
		System.out.println("writing vocabulary file to disk --successful");
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Map sortByValue(Map unsortMap) {   
	       List list = new LinkedList(unsortMap.entrySet());
	     
	       Collections.sort(list, new Comparator() {
	           public int compare(Object o1, Object o2) {
	               return ((Comparable) ((Map.Entry) (o2)).getValue())
	                           .compareTo(((Map.Entry) (o1)).getValue());
	           }
	       });
	     
	       Map sortedMap = new LinkedHashMap();
	       for (Iterator it = list.iterator(); it.hasNext();) {
	           Map.Entry entry = (Map.Entry) it.next();
	           sortedMap.put(entry.getKey(), entry.getValue());
	       }
	       return sortedMap;
	   }
	     
	    @SuppressWarnings("rawtypes")
		public static void writeToFile(HashMap<String, Double> doc_score_map) throws Exception {
	        //initialize writers and buffers
	        File file = new File("D:/HW-7/scores");
	        RandomAccessFile raf = new RandomAccessFile(file, "rw");
	        
	        //start writing
	         int i = 1;
	         Map set = sortByValue(doc_score_map);
	         for(Object key: set.keySet()){
	             raf.writeBytes(key.toString() + " " + i + " "  + set.get(key) + "\n");
	             i++;
	         }   
	          
	        //close open buffers
	        raf.close();
	        System.out.println("generating top words --successful");
	    }

	private static void writeMatrix(String split, HashMap<String,HashSet<String>> wordMap, List<String> vocabulary) throws Exception
	{
		File matrix = new File("D:/HW-7/"+split);
		RandomAccessFile raf = new RandomAccessFile(matrix, "rw"); 
		for(String doc : split.equals("train")?trainList:testList)
		{
			String label = spams.contains(doc)?"0":"1";
			int i = 1;
			String line = label;
			for(String term : vocabulary)
			{	
				if(wordMap.get(doc).contains(term))
					line += " " + i + ":" + "1";
				i++;
			}
			//write to file
			raf.writeBytes(line+"\n");
		}
		raf.close();
		System.out.println("generating matrix for "+ split + " data -- successful");
	}

	private static HashSet<String> getTermVector(String doc) throws Exception {
		HashSet<String> terms = new HashSet<>();
		String u = "http://localhost:9200/ap_dataset91/document/"+doc+"/_termvector?fields=text&positions=false&field_statistics=false";
		URL url = new URL(u);
		BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"));
		String res = "";
		for (String line; (line = reader.readLine()) != null;)
			res += line;
		JSONObject job = new JSONObject(res);
		JSONArray jar = job.getJSONObject("term_vectors").getJSONObject("text").getJSONObject("terms").names();
		for(int j = 0; j < jar.length(); j++)
			terms.add(jar.get(j).toString());
		return terms;
	}

	public static void getDirFiles(String dirPath) throws IOException
    {
        File file = new File(dirPath);
        for(String fname : file.list()){
        	String[] temp = fname.split("-");
        	if(temp[2].equals("train"))
        		trainList.add(temp[0]);
        	else
        		testList.add(temp[0]);
        	
        	if(temp[1].equals("spam"))
        		spams.add(temp[0]);
        	else
        		hams.add(temp[0]);
        }
        Collections.sort(trainList);
        Collections.sort(testList);
        System.out.println("read "+trainList.size()+", "+testList.size()+" files --successful");
        System.out.println("read "+spams.size()+", "+hams.size()+" files --successful");
    }
}
