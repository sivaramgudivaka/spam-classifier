package fin;

import static org.elasticsearch.index.query.QueryBuilders.spanNearQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanTermQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.SpanNearQueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;

import weka.classifiers.Classifier;
import weka.classifiers.trees.J48;
import weka.core.Instances;

public class Trainer {
	static List<String> spWords;
	static List<String> trainList = new ArrayList<>();
	static List<String> testList = new ArrayList<>();
	static HashSet<String> spams = new HashSet<>();
	static HashSet<String> hams = new HashSet<>();
	//constants
    static String index = "ap_dataset91";
    static String type = "document";
    
    static Node node = nodeBuilder().node();
    static Client client = node.client();
    
	public static void main(String[] args) throws Exception
	{
		System.out.println("Started ElasticSearch.");
		getSpamWordsList();
		getDirFiles("C:/Users/Sivaram/Downloads/trec07p/trec07p/clean/");
		generateArffFiles(getSpans("train"), "train");
		generateArffFiles(getSpans("test"), "test");
		node.close();
		trainer();
		System.out.println("done!");
	}

	private static void generateArffFiles(HashMap<String, HashSet<String>> spans, String split) throws Exception
	{
		File matrix = new File("D:/HW-7/"+split+".arff");
		RandomAccessFile raf = new RandomAccessFile(matrix, "rw");
		raf.writeBytes("@RELATION "+ split + "\n");
		String line = "";
		for(String spam : spWords)
			line += "@ATTRIBUTE "+ spam.replaceAll(" ", "_") +" NUMERIC\n";
		line += "@ATTRIBUTE class {spam, ham}\n@DATA\n";
		raf.writeBytes(line);
		List<String> files;
		if(split.equals("train"))
			files = trainList;
		else
			files = testList;
		for(String doc : files)
		{
			line = "";
			for(String word : spWords)
			{
				if(spans.containsKey(doc) && spans.get(doc).contains(word))
					line += "1,";
				else
					line += "0,";
			}
			//class
			if(spams.contains(doc))
				line += "spam" + "\n";
			else
				line += "ham" + "\n";
			
			//write to file
			raf.seek(raf.getFilePointer());
			raf.writeBytes(line);
		}
		raf.close();
		System.out.println("generated arff file for: "+split);
	}
	
	public static void trainer() throws Exception
	{
		HashMap<String, Double> scoreMap = new HashMap<>();
		BufferedReader reader = new BufferedReader(new FileReader("D:/HW-7/train.arff"));
		Instances train = new Instances(reader);
		train.setClassIndex(train.numAttributes() - 1);
		
		reader = new BufferedReader(new FileReader("D:/HW-7/test.arff"));
		Instances test = new Instances(reader);
		test.setClassIndex(test.numAttributes() - 1);
		
		reader.close();
		
		// train classifier
		Classifier cls = new J48();
		cls.buildClassifier(train);
		// evaluate classifier for train
		for(int i = 0; i < train.numInstances(); i++)
		{
			double[] prob = cls.distributionForInstance(train.instance(i));
			//insert in to main map
			scoreMap.put(trainList.get(i), prob[0]);
		}
		writeToFile(scoreMap, "train");
		System.out.println("results generated for train set");
		
		scoreMap = new HashMap<>();
		
		// evaluate classifier for train
		for(int i = 0; i < test.numInstances(); i++)
		{
			double[] prob = cls.distributionForInstance(test.instance(i));
			//insert in to main map
			scoreMap.put(testList.get(i), prob[0]);
		}
		writeToFile(scoreMap, "test");
		System.out.println("results generated for test set");
	}

	private static void getSpamWordsList() throws IOException {
		File f = new File("D:/spam3.txt");
		spWords = FileUtils.readLines(f);
		Collections.sort(spWords);
		System.out.println("generated spam words.");
	}

	private static HashMap<String, HashSet<String>> getSpans(String split)
	{
		PorterStemmer stemmer = new PorterStemmer();
		HashMap<String, HashSet<String>> result = new HashMap<>(); //<doc, [words]>
		// Span Near
		for(String spam : spWords)
		{
			SpanNearQueryBuilder qb = spanNearQuery();
			for(String word : spam.split(" ")){
				qb = qb.clause(spanTermQuery("text", stemmer.stem(word)));
			}
			qb = qb.slop(0)  
					.inOrder(true);
			SearchResponse scrollResp = client.prepareSearch(index)
										        .setSearchType(SearchType.SCAN)
										        .setScroll(new TimeValue(60000))
										        .setQuery(qb)
										        .setSize(100).execute().actionGet(); 
			while (true)
			{
				for (SearchHit hit : scrollResp.getHits().getHits())
				{
					String split_ret = (String) hit.getSource().get("split");
					if(!split.equals(split_ret))
						continue;
	                String doc = hit.getId();
	                if(result.containsKey(doc))
	                	result.get(doc).add(spam);
	                else{
	                	HashSet<String> temp = new HashSet<String>();
	                	temp.add(spam);
	                	result.put(doc, temp);
	                }
				}
				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
									.setScroll(new TimeValue(600000))
									.execute().actionGet();
				//Break condition: No hits are returned
				if (scrollResp.getHits().getHits().length == 0)
					break;
			}
		}
		System.out.println("got span query map for "+ split+" "+result.size());
		return result;
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
	   	public static void writeToFile(HashMap<String, Double> doc_score_map, String name) throws Exception {
	           //initialize writers and buffers
	           File file = new File("D:/HW-7/result_"+name+".txt");
	           RandomAccessFile raf = new RandomAccessFile(file, "rw");
	           raf.seek(file.length());
	           
	           //start writing
	            Map set = sortByValue(doc_score_map);
	            for(Object key: set.keySet())
	                raf.writeBytes(key + " "  + set.get(key) +"\n");
	             
	           //close open buffers
	           raf.close();
	       }
}
