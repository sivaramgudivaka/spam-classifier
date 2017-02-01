/*****
 Before running this program, create a folder called clean. 
 *****/
package fin;
import java.io.*;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
 
 
public class Cleaner{
	static String dirPath = "C:/Users/Sivaram/Downloads/trec07p/trec07p/";
	static Stack<String> spams = new Stack<String>();
	static Stack<String> hams = new Stack<>();
    
    public static void main(String args[]) throws Exception{
        
        //generate spam/ham
        generateSpamAndHam();
        
        System.out.println("index documents --starting");
        //train set
        IndexDataSet(60335, 40223, "train");
        //test set
        IndexDataSet(15084, 9976, "test");
        System.out.println("index documents --successful");
    }
    
    public static void IndexDataSet(int size, int spamLimit, String split) throws Exception
    {
    	RandomAccessFile raf = null;
        String label = "spam";
        File file;
        String CFile;
        for(int i = 0; i < size; i++)
        {
        	if(i < spamLimit)
        	{
        		//pick from spam
        		CFile = spams.pop();
         		file = new File(dirPath+"data/"+CFile);
        	}        	
        	else
        	{
        		//pick from ham
        		CFile = hams.pop();
        		file = new File(dirPath+"data/"+CFile);
        		label = "ham";
        	}
        	File cleanFile = new File(dirPath+"clean/"+CFile+"-"+label+"-"+split);
         	//index the document
        	raf = new RandomAccessFile(cleanFile, "rw");
        	raf.writeBytes(parse(file));
            System.out.println(i);
         }
        raf.close();
    }
    
    public static String parse(File file) throws IOException
    {
    	String res = "";
        String textFromFile = FileUtils.readFileToString(file);
        Document doc = Jsoup.parse(textFromFile);
		textFromFile = doc.text();
        String[] words = textFromFile.split("\\s+");
        for(String word : words){
        	if(word.length() <= 13)
        		res += word + " ";
        }
        //remove punctuation
        return res.replaceAll("[^a-zA-Z\\s+]", "").replaceAll("\\s+", " ");
    }
    
    public static void generateSpamAndHam() throws Exception
    {
        List<String> lines = FileUtils.readLines(new File(dirPath+"full/index"));
        for(String line : lines)
        {
        	String[] lineContents = line.split(" ");
        	String mayBeSpam = lineContents[0];
        	String fname = lineContents[1];
        	if(mayBeSpam.contains("spam"))
        		spams.push(fname);
        	else
        		hams.push(fname);
        }
        System.out.println(spams.size());
        System.out.println(hams.size());
    }
 }