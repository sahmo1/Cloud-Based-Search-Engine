package cis5550.jobs;

import cis5550.flame.*;
import cis5550.tools.Logger;
import cis5550.tools.Stemmer;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Indexer {
	private static final Logger logger = Logger.getLogger(Indexer.class);

	static int numberOfCrawledPagesInTotal = 0;
	
	 private static String bytesToHex(byte[] bytes) {
	        StringBuilder sb = new StringBuilder();
	        for (byte b : bytes) {
	            sb.append(String.format("%02x", b));
	        }
	        return sb.toString();
	    }
	 
	public static void run(FlameContext flameContext, String[] strings) throws Exception {

		boolean deleteIntermediateTablesFlag = true; // set this flag to true, to delete all intermediate (persistent)
		// tables between the subsequent transformations

//		flameContext.setConcurrencyLevel(50);

		// ----JOB Preparations------
//		flameContext.getKVS().persist("crawl");
//		flameContext.getKVS().persist("urlWordCount"); // -> currently this table is not required for TFIDF calculation
		flameContext.getKVS().persist("wTermDocument");
		flameContext.getKVS().persist("crawledPages");
		// hash version
		flameContext.getKVS().persist("HashURL");

//		flameContext.getKVS().persist("corpusDF");
//		flameContext.getKVS().persist("corpusIDF");
//		flameContext.getKVS().persist("index");

//		List<String> keepTables = new ArrayList<>();
//		keepTables.add("crawl");
//		keepTables.add("wTermDocument");
//		//		keepTables.add("urlWordCount");
//		keepTables.add("index");
//		keepTables.add("corpusDF");
//		keepTables.add("corpusIDF");

		// if (deleteIntermediateTablesFlag == true)
		// flameContext.getKVS().deleteIntermediateTable(keepTables);

		try {
//			flameContext.getKVS().setPersistFlag(true);

			// ----- PART 0: Indexing -----

			FlameRDD rowsFromCrawl = flameContext.fromTable("crawl", s -> { // .01
				if (s == null) {
					return null;
				}
				
				String page = s.get("page");
				String url = s.get("url");
				
				String regex = "^https?://[a-z0-9]+([\\-\\.]{1}[a-z0-9]+)*\\.[a-z]{2,5}(:[0-9]{1,5})?(/[a-zA-Z0-9\\-\\._\\?\\,\\'/\\\\+&amp;%$#\\=~]*)*$";
				Pattern pattern = Pattern.compile(regex);
				if (url != null) {
					Matcher matcher = pattern.matcher(url);
					if (matcher.matches() && (!url.contains("..") && (!url.contains(",")))) {
						
						// unhash version
//						return url + "," + page;
						
						
						
						
						// hash version
						try {
							
							MessageDigest md = MessageDigest.getInstance("SHA-256");
							 byte[] hashBytes = md.digest(url.getBytes());
							 String hashURL = bytesToHex(hashBytes);
							 
							 flameContext.getKVS().put("HashURL", hashURL, "url", url);
							 
							 return hashURL + "," + page;
						} catch (NoSuchAlgorithmException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
				}
				return null;
			});
//			logger.debug("rowsFromCrawl "+rowsFromCrawl.getTableName()+" done.");

			// count how many pages are crawled (number of total crawled documents (=N)) ->
			// used for corpus IDF
			// load crawl table
			// count rows

			int totalCrawledPages = rowsFromCrawl.count();
			String countString = "0";// do count on urlPage
			// turn totalCrawledPages into byte []

			if (totalCrawledPages != 0) {
				countString = Integer.toString(totalCrawledPages);
			}
			flameContext.getKVS().put("crawledPages", "total", "count", countString.getBytes());
//			System.out.println("No of crawled pages: " + totalCrawledPages);
			numberOfCrawledPagesInTotal = totalCrawledPages;

			// flameContext.getKVS().setPersistFlag(true);
//			flameContext.setConcurrencyLevel(10);

			FlamePairRDD urlAndPage = rowsFromCrawl.mapToPair(s -> { // .02
				// YONGs version
				String url = s.substring(0, s.indexOf(","));
//				logger.debug("url from rowsFromCrawl input: " + url);
//				if (url.endsWith(","))
//					logger.debug(url + "ends with comma");
////				logger.debug("page from rowsFromCrawl input: " + s.substring(s.indexOf(",")+1)); 
//				if (s.substring(s.indexOf(",") + 1).startsWith(","))
//					logger.debug(url + " text from URL starts with comma!");
				return new FlamePair(url, s.substring(s.indexOf(",")));

				// VERONIKAs version

				// logger.debug("row splitted array length: " + row.split(",",2).length);
				// String url = row.split(",", 2)[0]; //limit to split only once
				// String page = row.split(",", 2)[1];
				// logger.debug("2. - page length: " + page.length());
				//
				// FlamePair urlPage = new FlamePair(URLDecoder.decode(url, "UTF-8"), page);
				//
				// return urlPage /*new FlamePair(row.split(",")[0], row.split(",")[1])*/;

			});

			urlAndPage.saveAsTable("urlAndPage");
			rowsFromCrawl.saveAsTable("rowsFromCrawl");
//				logger.debug("rowsFromCrawl toString: " + rowsFromCrawl.toString());
			if (deleteIntermediateTablesFlag == true) {
				flameContext.getKVS().delete("rowsFromCrawl");
			}
//
//			logger.debug("rowsFromCrawl table: " + rowsFromCrawl.getTableName().toString());
//
//			keepTables.add(urlAndPage.getTableName());
//			if (deleteIntermediateTablesFlag == true) flameContext.getKVS().deleteIntermediateTable(keepTables);

			// flameContext.getKVS().setPersistFlag(false);
//			logger.debug("urlAndPage "+urlAndPage.getTableName()+" done.");

			FlamePairRDD wordAndUrl = urlAndPage.flatMapToPair(s -> { // .03
				List<FlamePair> flamePairs = new LinkedList<>();
				String modifiedPageContent = removePunctuation(filterOutHtmlTags(s._2())).toLowerCase();
				String[] words = modifiedPageContent.split("\\s+");

				/* regular urlandPage processing (from HW) */
//				String wordCount = "" + words.length;
				Set<String> removedDuplicateWords = new HashSet<>();
				for (String word : words) {
					// with word
					String regex = "[a-zA-Z]{2,12}";
					String numRegex = "\\b\\d{1,4}\\b";
					Matcher numMatcher = Pattern.compile(numRegex).matcher(word);
					Matcher matcher = Pattern.compile(regex).matcher(word);
					if (matcher.matches() || numMatcher.matches()) {
						if (!removedDuplicateWords.contains(word)) {
							removedDuplicateWords.add(word);
						}
						String stemWord = Stemmer.stemWord(word);
//						logger.debug("stemWord processing: " + stemWord);

						if (!removedDuplicateWords.contains(stemWord)) {
							matcher = Pattern.compile(regex).matcher(stemWord);
							numMatcher = Pattern.compile(numRegex).matcher(stemWord);
							if (!stemWord.equals("") && (numMatcher.matches() || matcher.matches())) {
								removedDuplicateWords.add(stemWord);
							}
						}
					}

//					logger.debug("removeDupliceWords: " + removedDuplicateWords);
				}

				for (String word : removedDuplicateWords) {
					if (!word.equals("")) {
						FlamePair wordAndURL = new FlamePair(word, s._1());
						flamePairs.add(wordAndURL);
					}
				}

				/*
				 * Word POSITION EC List<String> uniqueWords = new
				 * LinkedList<>(removedDuplicateWords); Iterator<String> uWords =
				 * uniqueWords.listIterator(); while (uWords.hasNext()) { String word =
				 * uWords.next(); if (word.equals("")) continue; String wordPosition =
				 * wordPositions(word, words); if (wordPosition.length() > 0) { FlamePair
				 * newFlamePair = new FlamePair(word, URLDecoder.decode(s._1(), "UTF-8") + ":" +
				 * wordPosition); // FlamePair newFlamePair = new FlamePair(word, //
				 * URLDecoder.decode(s._1(), "UTF-8") + ":" + word);
				 * flamePairs.add(newFlamePair); }
				 * 
				 * }
				 */
				// flameContext.getKVS().put("urlWordCount",URLDecoder.decode(s._1(), "UTF-8")
				// ,"wordCount",wordCount);
				return flamePairs;
			});
//			logger.debug("wordAndUrl "+wordAndUrl.getTableName()+" done.");
//			logger.debug("urlAndPage table: " + urlAndPage.getTableName().toString()); //should be deleted in subsequent step
//			keepTables.remove(urlAndPage.getTableName());
//			keepTables.add(wordAndUrl.getTableName());
//			if (deleteIntermediateTablesFlag == true) flameContext.getKVS().deleteIntermediateTable(keepTables);
//			logger.debug("wordAndUrl table: " + wordAndUrl.getTableName().toString()); //should not be deleted 

			// flameContext.getKVS().setPersistFlag(true);

			/*
			 * YONG's version with word positioning EC FlamePairRDD finalPair =
			 * wordAndUrl.foldByKey("", (a, b) -> // .04 { if (a.equals("")) return a + b;
			 * TreeMap<String, Integer> tm = new TreeMap<>(); String[] elements =
			 * a.split(","); String result = ""; int lenghOfB =
			 * b.substring(b.lastIndexOf(":") + 1).split(" ").length; int index =
			 * elements.length; for (int i = 0; i < elements.length; i++) { int numWords =
			 * elements[i].substring(elements[i].lastIndexOf(":") + 1).split(" ").length;
			 * tm.put(elements[i], numWords); } tm.put(b, lenghOfB); List<String> keySet =
			 * new ArrayList<>(tm.keySet()); keySet.sort((o1, o2) ->
			 * tm.get(o2).compareTo(tm.get(o1))); StringBuilder sb = new StringBuilder();
			 * Iterator<String> keys = keySet.listIterator(); while (keys.hasNext()) {
			 * String key = keys.next(); // result = result + (result.equals("") ? "" : ",")
			 * + key; if (sb.length() == 0) sb.append(key); else sb.append("," + key); }
			 * result = sb.toString(); return result; });
			 */

			// (w, u_i) pairs, with the same word w but different URLs u_i,
			// so we’ll need to fold all the URLs into a single comma-separated list,
			FlamePairRDD finalPair = wordAndUrl.foldByKey("", (String urlsList, String u) -> { // .4
				if (u == null || u.length() == 0)
					return urlsList;
				if (urlsList == null || urlsList.length() == 0)
					return u;
				return urlsList + "," + u;
			});

//			logger.debug("finalPair "+finalPair.getTableName()+" done.");
//
//			keepTables.remove(wordAndUrl.getTableName());
//			keepTables.add(finalPair.getTableName());
//			if (deleteIntermediateTablesFlag == true) flameContext.getKVS().deleteIntermediateTable(keepTables);
//
//			logger.debug("index "+finalPair.getTableName()+" done.");

			finalPair.saveAsTable("index");

			wordAndUrl.saveAsTable("wordAndUrl");
			if (deleteIntermediateTablesFlag == true) {

				flameContext.getKVS().delete("wordAndUrl");
			}

			flameContext.output("Created inverted index.\n");

			// ----PART 1: Corpus Scoring ----
			// ----- corpusDF ---------
			// syzygy: create pairs of (term, UrlNo) where the term is listed once with the
			// number of URLs it occurs in
			// flameContext.getKVS().setPersistFlag(true);

			FlamePairRDD termURLPair = finalPair.flatMapToPair(pair -> { // .05 //finalPair = "index" table
				String word = pair._1();
				String[] urls = pair._2().split(",");
				List<FlamePair> singleTermURLPairs = new LinkedList<FlamePair>();
				int urlNo = urls.length;
				FlamePair termAndURL = new FlamePair(word, Integer.toString(urlNo));
//				logger.debug("termAndURLCount: " + termAndURL.toString());
				singleTermURLPairs.add(termAndURL);
				return singleTermURLPairs;
			});

			// keepTables.remove(finalPair.getTableName());
//			keepTables.add(termURLPair.getTableName());
//			if (deleteIntermediateTablesFlag == true) flameContext.getKVS().deleteIntermediateTable(keepTables);

			// flameContext.getKVS().setPersistFlag(true);

			// .06
			FlamePairRDD wordDF = termURLPair.foldByKey("0",
					(aggrCount, urlNo) -> Integer.toString(Integer.parseInt(aggrCount) + Integer.parseInt(urlNo)));

//			logger.debug("wordDF "+termURLPair.getTableName()+" done.");
//
//			keepTables.remove(termURLPair.getTableName());
//			keepTables.add(wordDF.getTableName());
//			logger.debug("wordDF tablename: " + wordDF.getTableName());
//			//			wordDF.saveAsTable("corpusDF");
//
//			if (deleteIntermediateTablesFlag == true) flameContext.getKVS().deleteIntermediateTable(keepTables);

			wordDF.saveAsTable("corpusDF");
			termURLPair.saveAsTable("termURLPair");
			if (deleteIntermediateTablesFlag == true) {

				flameContext.getKVS().delete("termURLPair");
			}

			flameContext.output("Calculated corpusDF values.\n");

			// -----------corpusIDF-------
			// compute idf of each term
			// flameContext.getKVS().setPersistFlag(true);

			FlamePairRDD corpusIDFRaw = wordDF.flatMapToPair(pair -> { // wordDF = corpusDF //.07
				List<FlamePair> flamePairs = new LinkedList<>();
				String term = pair._1();
				int df = Integer.parseInt(pair._2());
				String totalCrawledPagesString = new String(
						flameContext.getKVS().get("crawledPages", "total", "count"));
				int value = Integer.parseInt(totalCrawledPagesString);
//				System.out.println("wordDF- totalCrawledPage: "+ value);
				double idf = computeCorpusIDF(df, value);
				flamePairs.add(new FlamePair(term, Double.toString(idf)));
//				logger.debug(".07 corpusIDF flamePairs:" + flamePairs.toString());
				return flamePairs;
			});

//			logger.debug("corpusIDF (not aggregated) table: " + corpusIDF.getTableName() + " done");

			// .08
			FlamePairRDD corpusIDFAggr = corpusIDFRaw.foldByKey("0",
					(idfAggr, idfVal) -> Double.toString(Double.parseDouble(idfAggr) + Double.parseDouble(idfVal)));

//			logger.debug("corpusIDFAggr "+corpusIDFAggr.getTableName()+" done.");

//			keepTables.add(corpusIDFAggr.getTableName());
			// corpusIDFAggr.saveAsTable("corpusIDF");
//			if (deleteIntermediateTablesFlag == true) flameContext.getKVS().deleteIntermediateTable(keepTables);

			corpusIDFAggr.saveAsTable("corpusIDF");
			if (deleteIntermediateTablesFlag == true) {
				corpusIDFRaw.saveAsTable("corpusIDFRaw");
				flameContext.getKVS().delete("corpusIDFRaw");
			}

			flameContext.output("Calculated corpus IDF values.\n");

			// ---PART 2: Document scoring ---

			// ---------- documentTFraw ----------
			// load crawl table
			// process each page word-by-word (lowercase, remove punctuation and html tags)
			// count how often each word occurs in this document

//			logger.debug("rowsFromCrawl2 started");
//			FlameRDD rowsFromCrawl2 = flameContext.fromTable("crawl", s ->  { //.09
//				try {
//					return URLDecoder.decode(s.get("url"), "UTF-8") + "," + s.get("page");
//				} catch (UnsupportedEncodingException e) {
//					e.printStackTrace();
//				}
//				return null;
//			});
//			logger.debug("rowsFromCrawl "+rowsFromCrawl2.getTableName()+" done.");

			// flameContext.getKVS().setPersistFlag(true);

//			FlamePairRDD urlAndPage2 = rowsFromCrawl2.mapToPair(s -> { //.10
//				String url =s.substring(0, s.indexOf(","));
//				return new FlamePair(URLDecoder.decode(url, "UTF-8") , s.substring(s.indexOf(",")+1));});

//			logger.debug("urlAndPage "+urlAndPage2.getTableName()+" done.");

//			keepTables.add(urlAndPage2.getTableName());
//			if (deleteIntermediateTablesFlag == true) flameContext.getKVS().deleteIntermediateTable(keepTables);

			// convert each (u, p) pair to lots of (w, u) pairs, where w is a word that
			// occurs in p
			FlamePairRDD urlWordFreq = urlAndPage.flatMapToPair(s -> { // .09
				List<FlamePair> flamePairs = new LinkedList<>();
//				logger.debug("urlWordFreq s1 & s2: " + s._1().toString() + " & " + s._2());
				String modifiedPageContent = removePunctuation(filterOutHtmlTags(s._2())).toLowerCase();
				// System.out.println("original: " + s._2());
				// System.out.println("modified: " + modifiedPageContent);
				String[] words = modifiedPageContent.split("\\s+");

				Map<String, Integer> wordCount = new ConcurrentHashMap<String, Integer>();

				for (String word : words) {
					
					String regex = "[a-zA-Z]{2,12}";
					String numRegex = "\\b\\d{1,4}\\b";
					Matcher numMatcher = Pattern.compile(numRegex).matcher(word);
					Matcher matcher = Pattern.compile(regex).matcher(word);
					if (matcher.matches() || numMatcher.matches()) {
						Integer count = wordCount.get(word); // returns null if map does not yet contain word as key
						if (count == null) {
							wordCount.put(word, 1);
						} else {
							wordCount.put(word, count + 1);
						}
						String stemWord = Stemmer.stemWord(word);
						matcher = Pattern.compile(regex).matcher(stemWord);
						numMatcher = Pattern.compile(numRegex).matcher(stemWord);
						if (!stemWord.equals("") && (numMatcher.matches() || matcher.matches())) {
							Integer countStemmed = wordCount.get(stemWord);

							if (countStemmed == null) {
								wordCount.put(stemWord, 1);
							} else {
								wordCount.put(stemWord, countStemmed + 1);
							}
						}

//
//						if (!removedDuplicateWords.contains(stemWord)) {
//							matcher = Pattern.compile(regex).matcher(stemWord);
//							if (!stemWord.equals("")&&((stemWord.length() < 10) || (matcher.matches()))) {
//								removedDuplicateWords.add(stemWord);
//							}
//						}
					}
					
									

//					logger.debug("removeDupliceWords: " + removedDuplicateWords);
				}

				logger.debug("wordCountMap size: " + wordCount.size());
				StringBuilder sb = new StringBuilder(); // value = {word1:countinDoc,word2:countinDoc}

				for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
					// key=word, value=50,URL2
					logger.debug("entry " + entry.toString());
					if (entry.getKey() == null)
						System.out.println(entry.getKey());
					sb.append(entry.getKey() + ":");
					sb.append(entry.getValue() + ","); // when separating by "," later there will be 1 last
														// empty entry
					// in STring[]!
					logger.debug("sb.toString: " + sb);
				}

				String termFrequencies = sb.toString();
				String url = s._1();

				FlamePair urlWordFreqPair = new FlamePair(url, termFrequencies);
				logger.debug("flamePairs added: " + urlWordFreqPair.toString());
				flamePairs.add(urlWordFreqPair);

//				logger.debug("flamePair List return: " + flamePairs.toString());
				return flamePairs;

			});

			urlWordFreq.saveAsTable("urlWordFreq");

			if (deleteIntermediateTablesFlag == true) {
				flameContext.getKVS().delete("urlAndPage");
			}
//			logger.debug("urlWordFreq "+urlWordFreq.getTableName()+" done.");
//
//			keepTables.remove(urlAndPage2.getTableName());
//			keepTables.add(urlWordFreq.getTableName());
//			if (deleteIntermediateTablesFlag == true) flameContext.getKVS().deleteIntermediateTable(keepTables);
//
//			flameContext.output("Calculated documents TF raw values.\n");

			// ---------- documentTFweighted ----------
			FlamePairRDD documentTFWeighted = urlWordFreq.flatMapToPair(s -> { // .10

				// calculate for each URL document the Euclidean length: sqrt(countinDoc1^2 +
				// countinDoc2^2 + ...)
				String urlKey = s._1();
				String[] wordWithFrequency = s._2().split(","); // let:1,revoir:2,attractions:1,vienna:1,you:9,
				logger.debug("wordFrequency []: " + Arrays.toString(wordWithFrequency));

				int sumOfSquaredTfRaws = 0;
//				logger.debug("sumOfSquaredTfRaws: " + sumOfSquaredTfRaws);
				for (String word : wordWithFrequency) {
					if (!word.equals("")) {
						String tfRawStr = word.split(":")[1];
//						logger.debug(urlKey + " "+ word + " - tfRawStr: " + tfRawStr);
						int tfRawSquared = (int) Math.pow(Integer.parseInt(tfRawStr), 2);
						sumOfSquaredTfRaws += tfRawSquared;
//						logger.debug("sumOfSquaredTfRaws: " + sumOfSquaredTfRaws);
					}
				}

				logger.debug(urlKey + " - sumOfSquaredTfRaws: " + sumOfSquaredTfRaws);
				double euclideanLengthDenominator = Math.sqrt(sumOfSquaredTfRaws);
				logger.debug(urlKey + " - euclideanLengthDenominator: " + euclideanLengthDenominator);

				// calculate for each word in the document the tf weighted
				List<FlamePair> wordTFWeigthedPairs = new LinkedList<FlamePair>();
				for (String wordFreq : wordWithFrequency) {
					logger.debug(urlKey + " wordFreq: " + wordFreq);
					String[] wordFreqArr = wordFreq.split(":");
					logger.debug("wordFreqArr: " + Arrays.toString(wordFreqArr));
					String word = null;
					if (wordFreqArr.length > 1)
						word = wordFreqArr[0]; // > 1 ? (was > 0 before)
					String freq = wordFreqArr[1];
					if (!word.equals("")) {
						String tfRawStr = freq;
						int tfRaw = Integer.parseInt(tfRawStr);
						double tfWeighted = 0;
						if (euclideanLengthDenominator != 0) {
							tfWeighted = tfRaw / euclideanLengthDenominator;
						}

						FlamePair wordTFWeighted = new FlamePair(word, urlKey + "," + Double.toString(tfWeighted));
						wordTFWeigthedPairs.add(wordTFWeighted);
					}
				}
				logger.debug(urlKey + " wordTFWeigthedPairs: " + wordTFWeigthedPairs);

				return wordTFWeigthedPairs;

			});

			documentTFWeighted.saveAsTable("documentTFWeighted");

			if (deleteIntermediateTablesFlag == true) {
//				urlWordFreq.saveAsTable("urlWordFreq");
				flameContext.getKVS().delete("urlWordFreq");
			}

//			logger.debug("documentTFWeighted "+documentTFWeighted.getTableName()+" done.");
//
//			keepTables.remove(urlWordFreq.getTableName());
//			keepTables.add(documentTFWeighted.getTableName());
//			if (deleteIntermediateTablesFlag == true) flameContext.getKVS().deleteIntermediateTable(keepTables);

			flameContext.output("Calculated documents TF weighted values.\n");

			FlamePairRDD wTermDocument = documentTFWeighted.flatMapToPair(pair -> { // .11
				List<FlamePair> flamePairs = new LinkedList<FlamePair>();

				String term = pair._1();
				String urlAndWeight = pair._2();
				String[] urlAndWeightArr = urlAndWeight.split(",", 2);
				logger.debug(".11 urlAndWeight:" + urlAndWeightArr.toString());
				String columnName = urlAndWeightArr[0];
				String wTDStr = urlAndWeightArr[1];

				// store each term into KVS, using term as key and value={url1=weight1,
				// url2=weight2, ...}
				// important for the ranker to extract the kvs table rows!
				flameContext.getKVS().put("wTermDocument", term, columnName, wTDStr);
				return flamePairs; // empty list
			});

			if (deleteIntermediateTablesFlag == true) {
				wTermDocument.saveAsTable("wTermDocumentOld");
				flameContext.getKVS().delete("wTermDocumentOld");
				flameContext.getKVS().delete("documentTFWeighted");
			}

			/*
			 * BACKUP solution to adjust index table without EC (CHING)
			 * flameContext.getKVS().persist("index2"); // filtered out word position
			 * FlamePairRDD filteredFinalPair = finalPair.flatMapToPair(pair -> { //.11
			 * List<FlamePair> flamePairs = new LinkedList<FlamePair>(); String word =
			 * pair._1(); String urls = pair._2(); String [] urlsList = urls.split(",");
			 * List<String> filteredList = new ArrayList<>(); for (String url: urlsList) {
			 * url=url.substring(0, url.lastIndexOf(":")); filteredList.add(url); } String
			 * filteredURLs = String.join(",", filteredList);
			 * flameContext.getKVS().put("index2", word, "acc", filteredURLs);
			 * flamePairs.add(new FlamePair(word, filteredURLs)); return flamePairs; } );
			 */

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static String wordPositions(String word, String[] words) {
		String result = "";
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < words.length; i++) {
			if (word.equals(words[i])) {
				// result = result + (result.equals("") ? "" : " ") + i;
				if (sb.length() == 0)
					sb.append(i);
				else
					sb.append(" " + i);
			}
		}
		result = sb.toString();
		return result;
	}

	public static String filterOutHtmlTags(String html) {
		Pattern pattern = Pattern.compile("<[^>]*>");
		Matcher matcher = pattern.matcher(html);
		String filteredOutText = matcher.replaceAll(" ");
		return filteredOutText;
	}

	public static String removePunctuation(String text) {
		String punctuationRemovedText = text.replaceAll("[^a-zA-Z0-9\\s]", " ");
		return punctuationRemovedText;
	}

	public static double computeCorpusIDF(int corpusDfArg, int totalCorpusSizeArg) {
		if (corpusDfArg > 0) {
			logger.debug("No of crawled pages: " + totalCorpusSizeArg);
			double idfRaw = Double.valueOf(totalCorpusSizeArg) / Double.valueOf(corpusDfArg);
			logger.debug("idfRAw: " + idfRaw);
			double idf = Math.log10(idfRaw);
			logger.debug("idf: " + idf);
			return idf;
		} else {
			return 0;
		}
	}
}
