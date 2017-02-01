package fin;
import java.io.*;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
 
import static org.elasticsearch.node.NodeBuilder.*;
 
 
public class Indexer{
	static String dirPath = "C:/Users/Sivaram/Downloads/trec07p/trec07p/";
	//constants
    static String index = "ap_dataset91";
    static String type = "document";
    
    static Node node = nodeBuilder().node();
    static Client client = node.client();
    
    public static void main(String args[]) throws Exception{
    	
        // 1) set settings and analysis
        XContentBuilder settingsBuilder = getSettingsBuilder();
        client.admin().indices().prepareCreate(index)
        .setSettings(ImmutableSettings.settingsBuilder().loadFromSource(settingsBuilder.string()))
        .execute()
        .actionGet();
         
        // 2) set mapping
        XContentBuilder mappingBuilder = getMappingBuilder();
        client.admin().indices().preparePutMapping(index)
        .setType(type)
        .setSource(mappingBuilder)
        .execute()
        .actionGet();
        
        System.out.println("index documents --starting");
        IndexDataSet();
        System.out.println("index documents --successful");
        //close client
        node.close();
    }
    
    public static void IndexDataSet() throws Exception
    {
    	File[] files = getDirFiles(dirPath+"clean/");
        String contents;
        int i = 0;
        for(File file : files)
        {
        	String[] FInfo = file.getName().split("-");
        	String label = FInfo[1];
        	String split = FInfo[2];
        	contents = FileUtils.readFileToString(file);
         	//index the document
             XContentBuilder doc_json = XContentFactory.jsonBuilder()
                     .startObject()
                     .field("text", contents)
                     .field("label", label)
                     .field("split", split)
                     .endObject();
             client.prepareIndex(index, type, FInfo[0])
                     .setSource(doc_json)
                     .execute()
                     .actionGet();
             ++i;
             System.out.println(i);
         }
    }
    
    public static File[] getDirFiles(String dirPath)
    {
        File file = new File(dirPath);
        File[] files = file.listFiles();
        System.out.println("read "+files.length+" files --successful");
        return files;
    }
    
    private static XContentBuilder getMappingBuilder() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
        .startObject("document")
        .startObject("properties")
        .startObject("label")
        .field("type", "string")
        .field("store", true)
        .field("index", "not_analyzed")
        .endObject()
        .startObject("split")
        .field("type", "string")
        .field("store", true)
        .field("index", "not_analyzed")
        .endObject()
        .startObject("text")
        .field("type", "string")
        .field("store", true)
        .field("index", "analyzed")
        .field("term_vector", "with_positions")
        .field("analyzer", "my_english")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
            return builder;
        }
 
    private static XContentBuilder getSettingsBuilder() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
        .startObject("settings")
        .startObject("index")
        .startObject("score")
        .field("type", "default")
        .endObject()
        .field("number_of_shards", 1)
        .field("number_of_replicas", 0)
        .endObject()
        .endObject()
        .startObject("analysis")
        .startObject("analyzer")
        .startObject("my_english")
        .field("type", "english")
        .field("stopwords_path","D:/elasticsearch-1.7.1/config/stoplist.txt")
        .endObject()
        .endObject()
        .endObject()
        .endObject();
        return builder;
    } 
 }