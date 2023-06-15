package cis5550.ranker;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import cis5550.application.searchApp;
import cis5550.generic.Master;
import cis5550.jobs.*;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.*;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

public class Ranker {
	
	private static final Logger logger = Logger.getLogger(Ranker.class);
	
	public String queryRaw;
	List<String> queryParsed; 
	List<String> queryParsedAndStemmed;

	Map<String, Integer> queryTFsMap = new HashMap<String, Integer>(); //word=tf
	Map<String, Double> queryWtqMap = new ConcurrentHashMap<String, Double>();  //word=w_t,q

	Map<String, String> indexURLsMap = new ConcurrentHashMap<String, String>(); 

	// spoke http://simple.crawltest.cis5550.net:80/CN9CmwX.html 19 0.01573194590680384 http://simple.crawltest.cis5550.net:80/LE4.html 20 0.060985247166179764 
	Map<String, ConcurrentHashMap<String, Double>> wordAndDocumentWtdMap = new ConcurrentHashMap<String, ConcurrentHashMap<String, Double>>();  

	Set<String> relevantDocuments = Collections.synchronizedSet(new HashSet<String>()); //stores urls of relevant documents
	Map<String, String> hashURLForRelevantDocumentsMap = new ConcurrentHashMap<String, String>(); //stores hash, url

	Map<String, Double> relevantPageRankScoresMap = new ConcurrentHashMap<String, Double>(); 
	Map<String, Double> documentNetScoreMap = new ConcurrentHashMap<String, Double>(); //stores url, score  

	public Map<Double, String> topHitList = new TreeMap<Double, String>(Collections.reverseOrder()); //stores top search hits in ascending order (highest rank first)

	KVSClient kvs ;
	private String corpusIDFTable = "corpusIDF"; 
	private String indexTable = "index";
	private String pageRankTable = "pageranks";
	private String wTermDocument = "wTermDocument";
	
	private double maxPageRank = 0;
	private double minPageRank = 0;
	private double maxTFIDF = 0;
	private double minTFIDF = 0; 
	
	//source for common english stop words: //https://github.com/watson-developer-cloud/doc-tutorial-downloads/blob/master/discovery-data/custom_stopwords_en.json

	String[] stopwordsArr = {"a", "about", "above", "after", "again", "am", "an", "and", "any", "are", "as", "at", 
			"be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can", 
			"did", "do", "does", "doing", "don", "down", "during", "each", "few", "for", "from", "further", 
			"had", "has", "have", "having", "he", "her", "here", "hers", "herself", "him", "himself", "his", "how", 
			"i", "im", "if", "in", "into", "is", "it", "its", "itself", "just", "me", "more", "most", "my", "myself", 
			"no", "nor", "not", "now", "of", "off", "on", "once", "only", "or", "other", "our", "ours", "ourselves", "out", "over", "own", 
			"s", "same", "she", "should", "so", "some", "such", 
			"t", "than", "that", "the", "their", "theirs", "them", "themselves", "then", "there", "these", "they", "this", "those", "through", "to", "too", 
			"under", "until", "up", "very", "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom", "why", "will", "with", 
			"you", "your", "yours", "yourself", "yourselves"};
	
	List<String> stopWords = Arrays.asList(stopwordsArr); 
	
	public Ranker(String queryRawArg, String page, KVSClient kvsArg) {
		queryRaw = queryRawArg;
		queryParsedAndStemmed = new LinkedList<String>(); 
		kvs = kvsArg;

		try {
			getResults(page);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	

	/**
	 * 1) reads the search terms,
	 * 2) looks up the corresponding entries in the index, 
	 * 3) looks up the relevant PageRank values for the URLs in these entries, 
	 * 4) computes TF/IDF scores for these URLs, 
	 * 5) combines the two kinds of scores in some way, 
	 * 6) sorts the URLs by the combined scores, and then 
	 * 7) assembles a “results” page, which is sent back to the user’s browser.
	 * @returns List<String>
	 * @throws IOException 
	 */
	public Map<String, Object> getResults(String page) throws IOException 
	{
		boolean removeStopWordsFromQueryFlag = false;
		boolean threadingFlag = false; 
		long startTime = System.currentTimeMillis(); 
		
		Map<String, Object> schema = new HashMap<>();
		String[] urls = null;

		System.out.println("Ranker - started");
		parseQuery(removeStopWordsFromQueryFlag); 
		processQuery(); 
		
		logger.debug("queryParsedAndStemmed: " + queryParsedAndStemmed.toString());

		System.out.println("queryParsedAndStemmed: " + queryParsedAndStemmed.toString());


		if (threadingFlag == true) {
			//THREADING---start
			Thread threadsQuery[] = new Thread[queryParsedAndStemmed.size()];

			//Step 1: indexLookup and calculateQueryWordTF
			for (int i=0; i<queryParsedAndStemmed.size(); i++) {
				final int j = i;
				threadsQuery[i] = new Thread("(Step.1) thread '" + queryParsedAndStemmed.get(i) + "'") {
					@Override
					public void run() {
						try {
							indexLookUp(queryParsedAndStemmed.get(j)); //accesses kvs
							calculateQueryWordTF(queryParsedAndStemmed.get(j)); 
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				};
				threadsQuery[i].start();
			}
			//wait for indexLookup and calculateQueryWordTF to finish
			for (int i=0; i<threadsQuery.length; i++) {
				try {
					threadsQuery[i].join();
				} catch (InterruptedException ie) {
					ie.printStackTrace();
				}
			}

			//Step 2: calculateQueryWordWtq & getDocumentWtds
			for (int i=0; i<queryParsedAndStemmed.size(); i++) {
				final int j = i;
				threadsQuery[i] = new Thread("(Step.2) thread '" + queryParsedAndStemmed.get(i) + "'") {
					@Override
					public void run() {
						try {
							calculateQueryWordWtq(queryParsedAndStemmed.get(j));
							//accesses kvs
							getDocumentWtds(queryParsedAndStemmed.get(j)); 
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				};
				threadsQuery[i].start();
			}
			//wait for calculateQueryWordWtq and getDocumentWtds to finish
			for (int i=0; i<threadsQuery.length; i++) {
				try {
					threadsQuery[i].join();
				} catch (InterruptedException ie) {
					ie.printStackTrace();
				}
			}


			//Step 3: 
			collectRelevantDocuments(); //no kvs accessing

			//Step 4: collectHashURLMapping of relevant Documents
			Thread threadsDocs[] = new Thread[relevantDocuments.size()];

			List<String> relevantDocumentsList = new ArrayList<String> (relevantDocuments);
			for (int i=0; i<relevantDocuments.size(); i++) {
				final int j = i;
				threadsDocs[i] = new Thread("(Step.4) thread '" + relevantDocumentsList.get(i) + "'") {
					@Override
					public void run() {
						try {
							collectHashURLMapping(relevantDocumentsList.get(j)); //accesses kvs
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				};
				threadsDocs[i].start();
			}
			//wait for collectHashURLMapping to finish
			for (int i=0; i<threadsDocs.length; i++) {
				try {
					threadsDocs[i].join();
				} catch (InterruptedException ie) {
					ie.printStackTrace();
				}
			}


			//Step 5: extract PageRanks of relevant Documents
			for (int i=0; i<relevantDocuments.size(); i++) {
				final int j = i;
				threadsDocs[i] = new Thread("(Step.5) thread '" + relevantDocumentsList.get(i) + "'") {
					@Override
					public void run() {
						try {
							collectHashURLMapping(relevantDocumentsList.get(j)); //accesses kvs
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				};
				threadsDocs[i].start();
			}
			//wait for extract PageRanks to finish
			for (int i=0; i<threadsDocs.length; i++) {
				try {
					threadsDocs[i].join();
				} catch (InterruptedException ie) {
					ie.printStackTrace();
				}
			}

			//THREADING---end --continue with calculateDocumentNetScores()... 
			//---- no kvs accesses more below this line ---

			calculateDocumentNetScores(); 
			//		Map<String, Object> schema = new HashMap<>();
			//logger.debug("documentNetScoreMap: " + documentNetScoreMap.toString());
			//when there is no relevant document return empty schema
			if (documentNetScoreMap.isEmpty()) {
				return schema;
			}
			maxTFIDF =  Collections.max(documentNetScoreMap.values());
			//logger.debug("INFO (for planned normalization) max TFIDF: " + maxTFIDF); 

			combineTFIDFWithPageRanks(); 

			// urls only contain url from topHitList(removed scores)
			//		String[] urls = new String[topHitList.size()];
			urls = new String[topHitList.size()];



		} else {
			for (String word : queryParsedAndStemmed) {
				indexLookUp(word); //accesses kvs
				calculateQueryWordTF(word); 
			}
			logger.debug("queryTFsMap: " + queryTFsMap.toString());
			logger.debug("indexURLsMap: " + indexURLsMap.toString());
//			System.out.println("indexURLsMap:"+indexURLsMap.toString());


			for (String word : queryParsedAndStemmed) {
				calculateQueryWordWtq(word);
				
				logger.debug("queryWtqMap: " + queryWtqMap.toString());
				getDocumentWtds(word); 
				logger.debug("wordAndDocumentWtdMap: " + wordAndDocumentWtdMap.toString());
				
			}


			collectRelevantDocuments();
			logger.debug("relevantDocuments: " + relevantDocuments.toString());

			for (String docHashed : relevantDocuments) {
				collectHashURLMapping(docHashed);
			}


			//extract PageRanks for relevant URLs (documents)
			for (String url : relevantDocuments) {
				String urlUnhashed = hashURLForRelevantDocumentsMap.get(url);
				//			getPageRanks(url);
				getPageRanks(urlUnhashed);
			}
			
			System.out.println("relevantPageRankScoresMap:"+relevantPageRankScoresMap.toString());

			logger.debug("relevantPageRankScoresMap: " + relevantPageRankScoresMap);

			logger.debug("INFO (for planned normalization) max PageRank Value: " + maxPageRank); 

			//---- no kvs accesses more below this line ---

			calculateDocumentNetScores(); 
			//		Map<String, Object> schema = new HashMap<>();
			//logger.debug("documentNetScoreMap: " + documentNetScoreMap.toString());
			//when there is no relevant document return empty schema
			if (documentNetScoreMap.isEmpty()) {
				return schema;
			}
			maxTFIDF =  Collections.max(documentNetScoreMap.values());
			//logger.debug("INFO (for planned normalization) max TFIDF: " + maxTFIDF); 

			combineTFIDFWithPageRanks(); 

			// urls only contain url from topHitList(removed scores)
			//		String[] urls = new String[topHitList.size()];
			urls = new String[topHitList.size()];
		}

		//remove duplicates

		int i = 0;
		
//		System.out.println("urls: " + topHitList.toString()); 
		ArrayList<String> urlList = new ArrayList<String>();
		
		Set<String> topHitSet = new HashSet<String>();
		//remove duplicates in urls
		for (Map.Entry<Double, String> entry : topHitList.entrySet()) {
			String url = entry.getValue();
			if(!topHitSet.contains(url)) {
				topHitSet.add(url);
				urlList.add(url);
//				urls[i] = url;
//				i++;
			}
			
		}
		String[] result = new String[urlList.size()];
		urls = urlList.toArray(result);
		
		
//		Set<String> set = new HashSet<String>(Arrays.asList(urls));
//		urls = set.toArray(new String[set.size()]);
//		System.out.println("urls: " +Arrays.toString(urls));
//		System.out.println("topHitList: " + topHitList.toString()); 
//

//		
//		long endTime = System.currentTimeMillis(); 
//		long queryProcessingDurationMS = endTime - startTime;
//		long queryProcessingDurationS = TimeUnit.MILLISECONDS.toSeconds(endTime - startTime);  
//		if (queryProcessingDurationS == 0) {
//			System.out.println("queryProcessingDuration in milliseconds: " + Long.toString(queryProcessingDurationMS));
//		} else {
//			System.out.println("queryProcessingDuration in seconds: " + Long.toString(queryProcessingDurationS));
//		}
//	
		logger.debug("min TF-IDF: " + minTFIDF);
		logger.debug("max TF-IDF: " + maxTFIDF);
		logger.debug("min PageRank: " + minPageRank);
		logger.debug("max PageRank: " + maxPageRank);


		//implement pagination
		

		// TODO NEEDS TO BE REMOVED ONLY FOR DEBUGGING. Testing pagination
		boolean DEBUGMODE = false;
		if (!DEBUGMODE) {

			//not testing pagination. 
			int shownItems = 5;
			// Get total number of results
			int totalResults = urls.length;
			// Get total number of pages
			int totalPages = (int) Math.ceil(totalResults / (double) shownItems);
			// Get current page
			int currentPage = Integer.parseInt(page);
			// Get start index of results
			int startIndex = (currentPage - 1) * shownItems;
			// Get end index of results
			
			int endIndex = (startIndex + shownItems) > totalResults ? totalResults : (startIndex + shownItems);
			List<String> stringResults = Arrays.asList(urls).subList(startIndex, endIndex);

			schema.put("pages", totalPages);
			schema.put("page", currentPage);
			schema.put("results", stringResults);
		} else {
			//testing pagination

			int shownItems = 5;
			// Get total number of results
//			int totalResults = urls.length;
			int totalResults = topHitList.size();
			// Get total number of pages
			int totalPages = (int) Math.ceil(totalResults / (double) shownItems);
			// Get current page
			int currentPage = Integer.parseInt(page);
			// Get start index of results
			int startIndex = (currentPage - 1) * shownItems;

			List<String> dummyResults = new ArrayList<>();
			for (int x = 1; x <= 50; x++) {
				String item = "http://simple.crawltest.cis5550.net:80/Item" + x + ".html";
				dummyResults.add(item);
			}
	
			// Filter the dummy result via pagination
			int dummyendIndex = (startIndex + shownItems) > 50 ? 50 : (startIndex + shownItems);
			
			dummyResults = dummyResults.subList(startIndex, dummyendIndex);
		
			schema.put("pages", 10);
			schema.put("page", 1);
			schema.put("results", dummyResults);
		}
//		System.out.println(schema);
		return schema;
	}
	
	
	
	/**
	 * to Lowercase; 
	 * remove punctuation (same as indexer); 
	 * split by multiple whitespace;
	 * add to list of query search terms;
	 * remove stop words;
	 */
	public void parseQuery(boolean removeStopWordsFromQueryFlag) 
	{
		String queryLower = queryRaw.toLowerCase(); 
		String queryWithoutPunctuation = Indexer.removePunctuation(queryLower);		
		queryParsed = Arrays.asList(queryWithoutPunctuation.split("\\s+"));
		List<String> queryParsedWithoutStopWords = queryParsed; 

		if (removeStopWordsFromQueryFlag == true) {
			//remove stop words from query
			for (String word : queryParsed) {
				if (stopWords.contains(word)) queryParsedWithoutStopWords.remove(word);		
			}

			//only use query without stop words, if the query still contains other words
			if (queryParsedWithoutStopWords.size() > 0) {
				queryParsedAndStemmed.addAll(queryParsedWithoutStopWords);
			} else {
				queryParsedAndStemmed.addAll(queryParsed);
			}
		} else {
			queryParsedAndStemmed.addAll(queryParsed);
		}
	}
	
	/**
	 * Stemm words and add them to the list of search terms
	 */
	public void processQuery() {
		
		for (String word : queryParsed) {
			String stemWord = Stemmer.stemWord(word);
			if (!stemWord.equals("") && !queryParsedAndStemmed.contains(stemWord)) {
				queryParsedAndStemmed.add(stemWord);
			}
		}
	}
		
	/**
	 * look up in the index, for each word, in which URLs the word occurs
	 * @throws IOException 
	 */
	public void indexLookUp(String word) throws IOException {
		if (!indexURLsMap.containsKey(word)) {
			Row indexRow = kvs.getRow(indexTable, word);
			if (indexRow == null) {
				logger.debug("indexRow:" + "null (index row for this word '" + word +"' " + "does not exist)");
				indexURLsMap.put(word, "");
			} else {
//				String value = indexRow.get("acc");

//				// TODO REMOVE THIS LATER - HAS TO BE DONE BY THE COMPONENT THAT FETCH IT
//				String[] urlArr = value.split(",");
//				String[] urlList = new String[urlArr.length];
//				for (int i = 0; i < urlArr.length; i++) {
//					int lastColonIndex = urlArr[i].lastIndexOf(":");
//					if (lastColonIndex > 0) {
//						urlList[i] = urlArr[i].substring(0, lastColonIndex);
//					} else {
//						urlList[i] = urlArr[i];
//					}
//				}
//
//				// Add for each urlList to the indexURLsMap
//
//				for (String url : urlList) {
//					indexURLsMap.put(word, url);
//				}
				String urls = indexRow.get("acc"); 
				indexURLsMap.put(word, urls); //here in the urls are also word positions included: http://simple.crawltest.cis5550.net:80/UJ2p.html:17 49 106 154	
			}
		}
	}
	
	
	/**
	 * calculated how often a word appears in the query
	 * @param word
	 */
	public void calculateQueryWordTF(String word) {
		if(!word.equals("")) {
			logger.debug("curr word in query: " + word);
			Integer count = queryTFsMap.get(word); //returns null if map does not yet contain word as key
			if (count == null) {
				queryTFsMap.put(word, 1);
			} else {
				queryTFsMap.put(word, count + 1);
			}
		}
	}
	
	/**
	 * calculated w_t,q = tf * idf, where idf is extracted from the corpusIDF for this word
	 * @param word
	 * @throws IOException
	 */
	public void calculateQueryWordWtq(String word) throws IOException {
		
		if (!queryWtqMap.containsKey(word)) {
			Row keyWordRow = kvs.getRow(corpusIDFTable, word);
			if (keyWordRow == null) {
				logger.debug("keyWordRow: " + "null");
				queryWtqMap.put(word, 0.0);
			} else {
				logger.debug("keyWordRow: " + keyWordRow.toString());
				String idfStr = keyWordRow.get("acc");
				double idf = Double.parseDouble(idfStr);
				int tf = queryTFsMap.get(word); 
				double queryWtq = idf * tf; 
				if (maxTFIDF < queryWtq) maxTFIDF = queryWtq;
				if (minTFIDF > queryWtq) minTFIDF = queryWtq;
				logger.debug("queryWtq = " + idf +"*" + tf);
				queryWtqMap.put(word, queryWtq);
			}
		}
	}


	
	/**
	 * get values for document w_t,d 
	 * and stores them in a map <Url, w_td> for the given word
	 * @param word
	 * @throws IOException
	 */
	public void getDocumentWtds(String word) throws IOException {

		if (!wordAndDocumentWtdMap.containsKey(word)) { //only access kvs, if the Row of this word was not yet fetched

			Row wordURLwTD = kvs.getRow(wTermDocument, word);
			ConcurrentHashMap<String, Double> innerMap = new ConcurrentHashMap<String, Double>(); 

			if (wordURLwTD == null) {
				wordAndDocumentWtdMap.put(word, innerMap);
			} else {
				Set<String> wordURLs = wordURLwTD.columns(); 
				Iterator<String> itr = wordURLs.iterator(); 
				while (itr.hasNext()) {
					String currURL = itr.next();
					String wTDStr = wordURLwTD.get(currURL); //get w_t,d of current document in value map of Row
					logger.debug(currURL + ", wTDStr: " + wTDStr);
					double wTD = Double.parseDouble(wTDStr);
					innerMap.put(currURL, wTD); 
				}
			}
			wordAndDocumentWtdMap.put(word, innerMap); 
		}
	}
	
	/**
	 * Collect of all query terms the relevant documents in which any of the query terms occur; 
	 */
	public void collectRelevantDocuments() {
		for (String key : wordAndDocumentWtdMap.keySet()) {
			relevantDocuments.addAll(wordAndDocumentWtdMap.get(key).keySet());
		}		
	}
	
	/**
	 * Collect of all relevant documents the hash-To-URL mapping from the kvs;
	 */
	public void collectHashURLMapping(String docHashed) throws IOException {
			Row docHashURL = kvs.getRow("HashURL", docHashed);
			if(docHashURL!=null) {
				hashURLForRelevantDocumentsMap.put(docHashed, docHashURL.get("url"));
			}
			logger.debug("docHashURL: " + docHashURL);
	}
	
	/**
	 * calculate net score for all documents: sum of all (w_t,d * w_t,q)
	 * 
	 * requires the document w_t,d and the query w_t,q values
	 * @throws IOException 
	 */
	public void calculateDocumentNetScores() {
		for (String keyWord : wordAndDocumentWtdMap.keySet()) {
			Map<String, Double> documentWtd =  wordAndDocumentWtdMap.get(keyWord);
			
			for (String document : documentWtd.keySet()) {
				double docWtd = documentWtd.get(document); 
				double queryWtq = queryWtqMap.get(keyWord); 
				double intermediateProductScore = docWtd * queryWtq;
				
				String documentUnHashed = hashURLForRelevantDocumentsMap.get(document);
//				Double docNetScore = documentNetScoreMap.get(document);
				Double docNetScore = documentNetScoreMap.get(documentUnHashed);
				if (documentUnHashed != null) {
					if (docNetScore == null) {	
//						documentNetScoreMap.put(document, intermediateProductScore);
						documentNetScoreMap.put(documentUnHashed, intermediateProductScore);
					} else {
//						documentNetScoreMap.put(document, docNetScore + intermediateProductScore);
						documentNetScoreMap.put(documentUnHashed, docNetScore + intermediateProductScore);
					}
				}
			}
		}
	}
	
		
	/**
	 * for a given URL, gets PageRanks score from kvs	
	 * @param urlArg
	 * @throws IOException
	 */
	public void getPageRanks(String urlArg) throws IOException {
		if (!relevantPageRankScoresMap.containsKey(urlArg)) {
			Row urlPageRankRow = kvs.getRow(pageRankTable, urlArg);
//			System.out.println(urlArg);
			if(urlPageRankRow!=null) {
				String rankStr = urlPageRankRow.get("rank"); 
				double rank = Double.parseDouble(rankStr);	
				
				if (rank > maxPageRank) maxPageRank = rank;
				if (rank < minPageRank) minPageRank = rank;
				
				relevantPageRankScoresMap.put(urlArg, rank);
			}
			
		}
		
	}
	
	/**
	 * combined TF-IDF with pageRank
	 */
	public void combineTFIDFWithPageRanks() {
		for (Map.Entry<String, Double> entry : documentNetScoreMap.entrySet()) {
			String document = entry.getKey();
			double tfIDF = entry.getValue(); 
			System.out.println("combineTFIDFWithPageRanks:"+ document);
			if(relevantPageRankScoresMap.get(document)!=null) {
				double pagerankScore = relevantPageRankScoresMap.get(document); 
				double adjustingFactor = 0.6;
				double combinedRank = adjustingFactor*tfIDF + (1-adjustingFactor)*pagerankScore;

				topHitList.put(combinedRank, document);
			}
			
			
			
			
		}
		 
	}
	
	/**
	 * Identify if URL path contains query key word, 
	 * if yes return a ranking boost factor.
	 */
	 

}
