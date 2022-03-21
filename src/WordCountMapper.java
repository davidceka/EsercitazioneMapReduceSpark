import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper < LongWritable, Text, Text, IntWritable > { // i primi 2 data types si
		// riferiscono alla coppia
		// key-value in input, gli altri
		// 2 alla coppia in output

			private Set < String > stopWordList = new HashSet < String > (Arrays.asList("i","me","my","myself","we","our", "ours",
			"ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her",
			"hers", "herself", "it", "its", "itself","they","them","their","theirs","themselves","what","which","who","whom","this",
			"that","these","those","am","is","are","was","were","be","been","being","have","has","had","having","do","does","did",
			"doing","a","an","the","and","but","if","or","because","as","until","while","of","at","by","for","with","about","against","between",
			"into","through","during","before","after","above","below","to","from","up","down","in","out","on","off","over","under",
			"again","further","then","once","here","there","when","where","why","how","all","any","both","each","few","more","most","other",
			"some","such","no","nor","not","only","own","same","so","than","too","very","s","t","can","will","just","don","should","now",""));

			/*private Set < String > stopWordList = newHashSet("i","me","my","myself","we","our", "ours",
			"ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her",
			"hers", "herself", "it", "its", "itself","they","them","their","theirs","themselves","what","which","who","whom","this",
			"that","these","those","am","is","are","was","were","be","been","being","have","has","had","having","do","does","did",
			"doing","a","an","the","and","but","if","or","because","as","until","while","of","at","by","for","with","about","against","between",
			"into","through","during","before","after","above","below","to","from","up","down","in","out","on","off","over","under",
			"again","further","then","once","here","there","when","where","why","how","all","any","both","each","few","more","most","other",
			"some","such","no","nor","not","only","own","same","so","than","too","very","s","t","can","will","just","don","should","now","");*/


			protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String[] riga = value.toString().split("\t"); // converto in String perchè non lavoriamo con i Text
				
				if(riga[0].equals("marketplace")/*||riga[13]==""*/) {
					return;
				}
				
				StringTokenizer words = new StringTokenizer(riga[13].toLowerCase(), " "); //.,?!;:()[]{}'
				while (words.hasMoreTokens()) {
					String word = words.nextToken().toLowerCase();
					if (!stopWordList.contains(word)) { // ignoro le stringhe vuote
						context.write(new Text(riga[7]+word), new IntWritable(1));
					}

				}
			};

			// usare hashtable per rimuovere le stop words
	}