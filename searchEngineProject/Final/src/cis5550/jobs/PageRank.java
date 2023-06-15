package cis5550.jobs;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;

public class PageRank {

	public static String normalizeURL(String url) {
		return normalizeURL(null, url);
	}

	public static String normalizeURL(String url, String tagUrl) {
		String parser[] = parseURL(tagUrl);
		String protocol = parser[0];

		String domain = parser[1];
		String port = parser[2];
		String page = parser[3];
		if ((page.endsWith(".jpg")) | (page.endsWith(".jpeg")) | (page.endsWith(".gif")) | (page.endsWith(".png"))
				| (page.endsWith(".txt"))) {
			return null;
		}
		int index = page.indexOf("#");

		if (index != -1) {
			page = page.substring(0, index);
		}
		if ((url == null) | ((protocol != null) && (domain != null) && (page != null))) {
			if (port == null) {
				if (protocol.equals("http")) {
					port = "80";
				} else {
					port = "443";
				}
			}

			return protocol + "://" + domain + ":" + port + page;
		}
		String baseParser[] = parseURL(url);
		String baseProtocol = baseParser[0];
		String baseDomain = baseParser[1];
		String basePort = baseParser[2];
		if ((protocol == null) && (domain == null) && (port == null)) {

			if (page.length() == 0) {
				return url + page;
			}

			if (page.startsWith("/")) {
				return baseProtocol + "://" + baseDomain + ":" + basePort + page;
			}
			if (page.startsWith("..")) {
				String currentPage = url;
				currentPage = currentPage.substring(0, currentPage.lastIndexOf("/"));
				while (page.startsWith("..")) {
					currentPage = currentPage.substring(0, currentPage.lastIndexOf("/"));
					page = page.substring(3);
				}
				return currentPage + "/" + page;
			}
			return url.substring(0, url.lastIndexOf("/") + 1) + page;
		}

		if (domain == null) {
			domain = baseProtocol;
		}
		if (protocol.equals("http") == false && protocol.equals("https") == false && (protocol != null)) {
			return null;
		}
		if (port == null) {
			if (basePort == null) {
				if (protocol.equals("http")) {
					port = "80";
				} else {
					port = "443";
				}
			} else {
				port = basePort;
			}

		}

		return protocol + "://" + domain + ":" + port + page;
	}

	public static String[] parseURL(String url) {
		String result[] = new String[4];
		int slashslash = url.indexOf("//");
		if (slashslash > 0) {
			result[0] = url.substring(0, slashslash - 1);
			int nextslash = url.indexOf('/', slashslash + 2);
			if (nextslash >= 0) {
				result[1] = url.substring(slashslash + 2, nextslash);
				result[3] = url.substring(nextslash);
			} else {
				result[1] = url.substring(slashslash + 2);
				result[3] = "/";
			}
			int colonPos = result[1].indexOf(':');
			if (colonPos > 0) {
				result[2] = result[1].substring(colonPos + 1);
				result[1] = result[1].substring(0, colonPos);
			}
		} else {
			result[3] = url;
		}

		return result;
	}

	public static String extractURL(String tag) {
		String[] splitTag = tag.split(" ");
		if (splitTag[0].toLowerCase().equals("<a") == false) {
			return null;
		} else {
			Pattern pattern = Pattern.compile("href=\"(.*?)\"");
			Matcher matcher = pattern.matcher(tag);
			if (matcher.find()) {
				String url = matcher.group(1);
				return url;
			}
		}
		return null;

	}

	public static List<String> findTags(String page) {
		LinkedList<String> list = new LinkedList<String>();
		StringBuilder tagBuilder = new StringBuilder();
		char[] ch = page.toCharArray();
		boolean flag = false;
		for (int i = 0; i < ch.length; i++) {
			if ((ch[i] == '<') && (ch[i + 1] != '/')) {
				flag = true;
				tagBuilder.append(ch[i]);
			} else if (flag && (ch[i] != '>')) {
				tagBuilder.append(ch[i]);
			} else if (flag && (ch[i] == '>')) {
				tagBuilder.append(ch[i]);
				String tag = tagBuilder.toString();
				tagBuilder.setLength(0);
				flag = false;
				list.add(tag);
			}

		}
		return list;

	}

	public static void run(FlameContext context, String[] args) throws Exception {
		
		boolean deleteIntermediateTablesFlag = true; // set this flag to true, to delete all intermediate (persistent)

		
//		context.setConcurrencyLevel(3);

		double threshold = 0.1;
		if (args.length >= 1) {
			threshold = Double.valueOf(args[0]);
		}
		
		FlameRDD rowsFromCrawl = context.fromTable("crawl", s -> { // .01
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
					return url + "," + page;		
				}
			}
			return null;
		});
		rowsFromCrawl.saveAsTable("rowsFromCrawl");
		
		FlamePairRDD oldStateTable = rowsFromCrawl.mapToPair(s -> {
				if(s!=null) {
					String page = s.substring(s.indexOf(",") + 1);
					String url = s.split(",")[0];
					if (url == null || page == null || page.equals("null")) {
						return null;
					}
					String links = "1.0,1.0";
					String regex = "^https?://[a-z0-9]+([\\-\\.]{1}[a-z0-9]+)*\\.[a-z]{2,5}(:[0-9]{1,5})?(/[a-zA-Z0-9\\-\\._\\?\\,\\'/\\\\+&amp;%$#\\=~]*)*$";
					Pattern pattern = Pattern.compile(regex);
					List<String> tagslist = findTags(page);
					Set<String> urlSet = new HashSet<>();
					for (String tag : tagslist) {
						String tagUrl = extractURL(tag);
						if (tagUrl != null) {
							String normalizeURL = normalizeURL(url, tagUrl);
							if (normalizeURL != null) {
								Matcher matcher = pattern.matcher(normalizeURL);
								if (matcher.matches() && (!normalizeURL.contains(".."))) {
									urlSet.add(normalizeURL);
								}
							}

						}

					}
					if (!urlSet.isEmpty()) {
						for (String link : urlSet) {
							links += "," + link;
						}
					}
					if(url!=null) {
						return new FlamePair(url, links);
					}
					
				}
					return null;
				});
		
		oldStateTable.saveAsTable("oldStateTable0");
		
		boolean convergenceFlag = false;
		int iteration =1;
		while (!convergenceFlag) {
			context.output("iteration: "+iteration+" \n");
			FlamePairRDD aggregatedTable = oldStateTable.flatMapToPair(s -> {
				String url = s._1();
				String rankString = s._2();
				List<String> rankSplit = Arrays.asList(rankString.split(","));
				double rc = Double.parseDouble(rankSplit.get(0));
				double n = rankSplit.size() - 2;

				List<FlamePair> results = new LinkedList<>();
				if (n == 0) {
					results.add(new FlamePair(url, Double.toString(0.0)));
				} else {
					for (int i = 2; i < rankSplit.size(); i++) {
						String link = rankSplit.get(i);

						double v = 0.85 * rc / n;
						results.add(new FlamePair(link, Double.toString(v)));
					}
				}
				return results;
			});
					
			String aggregatedTableName = "aggregatedTable" + Integer.toString(iteration);
			aggregatedTable.saveAsTable(aggregatedTableName);
		
			FlamePairRDD aggrTableFoldByKey = aggregatedTable.foldByKey("0.0", (a, b) -> "" + (Double.valueOf(a) + Double.valueOf(b)));
			String aggrTableFoldByKeyTableName = "aggrTableFoldByKey" + Integer.toString(iteration);
			aggrTableFoldByKey.saveAsTable(aggrTableFoldByKeyTableName);
			
			if (deleteIntermediateTablesFlag == true) context.getKVS().delete(aggregatedTableName);

			
			FlamePairRDD oldStateTableJoin = oldStateTable.join(aggrTableFoldByKey).flatMapToPair(s -> {
				String url = s._1();
				String rankString = s._2();
				List<String> rankSplit = Arrays.asList(rankString.split(","));
				String oldRc = rankSplit.get(0);
				String newComputed = "" + (Double.valueOf(rankSplit.get(rankSplit.size() - 1)) + 0.15);
				String links = "";
				if (rankSplit.size() > 3) {
					links = "," + rankSplit.get(2);
					for (int i = 3; i < rankSplit.size() - 1; i++) {
						links += "," + rankSplit.get(i);
					}
				}
				List<FlamePair> results = new LinkedList<>();
				results.add(new FlamePair(url, newComputed + "," + oldRc + links));
				return results;

			});
			
			
			oldStateTable = oldStateTableJoin;
			
			oldStateTableJoin.saveAsTable("oldStateTableJoin" + Integer.toString(iteration));

			
			if (deleteIntermediateTablesFlag == true) {
//				context.getKVS().delete("oldStateTable0");
				context.getKVS().delete(aggrTableFoldByKeyTableName);
			}
			
			FlameRDD maxDifferenceFlatMap = oldStateTable.flatMap(p -> {
				String rankString = p._2();
				List<String> rankSplit = Arrays.asList(rankString.split(","));
				Double abs_diff = Math.abs((Double.valueOf(rankSplit.get(0)) - Double.valueOf(rankSplit.get(1))));
				List<String> results = new LinkedList<>();
				results.add(abs_diff.toString());
				return results;
			});
			
			maxDifferenceFlatMap.saveAsTable("maxDifferenceFlatMap");
					
			String maxDifference = maxDifferenceFlatMap.fold("0.0", (s1, s2) -> "" + Math.max(Double.valueOf(s1), Double.valueOf(s2)));
			if (Double.valueOf(maxDifference) < threshold) {
				convergenceFlag = true;
			}
			
			oldStateTable.saveAsTable("oldStateTable" + iteration);
			
			if (deleteIntermediateTablesFlag == true) {
				context.getKVS().delete("oldStateTable" + Integer.toString(iteration-1));
				context.getKVS().delete("oldStateTableJoin" + Integer.toString(iteration-1));
				context.getKVS().delete("oldStateTableJoin" + Integer.toString(iteration));
				context.getKVS().delete("maxDifferenceFlatMap");
			}

			iteration++;

		}
		context.getKVS().persist("pageranks");

		//KVS
		FlamePairRDD resultsMap = oldStateTable.flatMapToPair(s -> {
			String url = s._1();
			String rankString = s._2();
			String rank = rankString.split(",")[0];
			List<FlamePair> results = new LinkedList<>();
			context.getKVS().put("pageranks", url, "rank", rank);
			return results;
		});
		
		resultsMap.saveAsTable("results");
		if (deleteIntermediateTablesFlag == true) {
			context.getKVS().delete("oldStateTable" + Integer.toString(iteration-1));
			context.getKVS().delete("oldStateTable" + Integer.toString(iteration));
			context.getKVS().delete("results");
		}
		
		
//		FlamePairRDD finalTable = oldStateTable.flatMapToPair(s -> {
//			String url = s._1();
//			String rankString = s._2();
//			String rank = rankString.split(",")[0];
//			List<FlamePair> results = new ArrayList<>();
//			results.add(new FlamePair(url, rank));
//			return results;
//		});
//		finalTable.saveAsTable("pageranks");

		context.output("OK");

	}

}