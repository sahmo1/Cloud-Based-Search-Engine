package cis5550.jobs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.RobotParser;
import cis5550.tools.URLParser;


public class Crawler {
    private static Boolean hasBlackList = false;
    private static Boolean ahchorMode = false;
    private static Boolean contentSennMode = false;
    private static final Logger logger = Logger.getLogger(Crawler.class);

    public static void run(FlameContext flameContext, String strings[]) throws Exception {
        final FlameContext ctx = flameContext;
        if (strings.length < 1) {
            flameContext.output("Error: There is no seed URL!");
        } else {
            flameContext.output("OK");
        }
        List<String> seedURL = new ArrayList<>();
        FileReader fr = new FileReader(new File("seedURL.txt"));
        BufferedReader bufferedReader = new BufferedReader(fr);
        String seed = "";
        while ((seed = bufferedReader.readLine()) != null) {
            seedURL.add(normalizeURL(seed, seed));
        }
//        seedURL.add(normalizeURL(strings[0], strings[0]));

        FlameRDD urlQueue = flameContext.parallelize(seedURL);
        flameContext.getKVS().persist("crawl");
        flameContext.getKVS().persist("hosts");
        flameContext.getKVS().persist("blacklist");

        if (strings.length >= 2) {
            hasBlackList = true;
            fr = new FileReader(new File("blacklist.txt"));
            bufferedReader = new BufferedReader(fr);
            String black = "";
            while ((black = bufferedReader.readLine()) != null) {
                String blackPattern = black.replaceAll("\\*", ".\\*");
                flameContext.getKVS().put("blacklist", Hasher.hash(black), "pattern", blackPattern);
            }
        }

        while(urlQueue.count() != 0) {
            urlQueue = urlQueue.flatMap(s -> {
                flameContext.setConcurrencyLevel(100);

                List<String> links = new ArrayList<>();
                try {
                    logger.info("Crawling the site URL: " + s);
                    Row newRow = null;
                    flameContext.getKVS().persist("crawl");
                    Long rateLimit = Long.valueOf(1000);

                    List<String> blackLists = new ArrayList<>();
                    String userAgent = "cis5550-crawler";

                    if(hasBlackList) {
                        Iterator<Row> blackListRows = flameContext.getKVS().scan("blacklist");
                        while(blackListRows.hasNext()) {
                            Row blackListRow = blackListRows.next();
                            String pattern = blackListRow.get("pattern");
                            blackLists.add(pattern);
                        }
                    }
                    if (flameContext.getKVS().existsRow("crawl", Hasher.hash(s))) return links;

//                Iterator<Row> rows = flameContext.getKVS().scan("crawl");
//                while(rows.hasNext()) {
//                    Row row = rows.next();
//                    if(row.get("url") != null && row.get("url").equals(s)) {
//                        return links;
//                    }
//                    if(row.key().equals(Hasher.hash(s))) {
//                        newRow = row;
//                        break;
//                    }
//                }
                    String method = URLParser.parseURL(s)[0];
                    String hostName = URLParser.parseURL(s)[1];
                    String portNum = URLParser.parseURL(s)[2];
                    String realPath = URLParser.parseURL(s)[3];

                    // filtering for method
                    if (!method.equals("http") && !method.equals("https")) {
                        logger.info("method is not http or https");
//                    System.out.println("method is not http or https");
                        return links;
                    }
                    // filtering for file extensions
//                    if (realPath.contains(".jpg") || realPath.contains(".jpeg")
//                            || realPath.contains(".gif") || realPath.contains(".png") ||  realPath.contains(".txt")
//                            || realPath.contains(".bmp") || realPath.contains(".mp3") || realPath.contains(".mp4")
//                            || realPath.contains(".avi") || realPath.contains(".mov") || realPath.contains(".wmv")
//                            || realPath.contains(".zip") || realPath.contains(".rar") || realPath.contains(".tar.gz")
//                            || realPath.contains(".doc") || realPath.contains(".xls") || realPath.contains(".pdf")
//                            || realPath.contains(".js") || realPath.contains(".css") || realPath.contains(".less")
//                            || realPath.contains(".exe") || realPath.contains(".dll") || realPath.contains(".ini")
//                            || realPath.contains(".sys")
//                    ) {
//                        logger.info("invalid contents");
//                        return links;
//                    }

                    if(flameContext.getKVS().getRow("hosts", hostName) == null || !flameContext.getKVS().getRow("hosts", hostName).columns().contains("robot-result")) {
                        URL robotURL = new URL(method + "://" + hostName + ":" + portNum + "/robots.txt");
                        HttpURLConnection robotConnection = (HttpURLConnection) robotURL.openConnection();
                        robotConnection.setRequestMethod("GET");
                        robotConnection.setRequestProperty("User-agent", userAgent);
                        robotConnection.setConnectTimeout(5000);
                        robotConnection.setReadTimeout(5000);
                        robotConnection.connect();

                        int robotResponseCode = robotConnection.getResponseCode();
                        flameContext.getKVS().put("hosts", hostName, "last-access-time", "0");
                        if (robotResponseCode == 200) {
                            InputStream inputStream = robotConnection.getInputStream();
                            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                            BufferedReader br = new BufferedReader(inputStreamReader);
                            String line;
                            StringBuilder sb = new StringBuilder();
                            while((line = br.readLine()) != null) {
                                sb.append(line + "\n");
                            }
                            String robotResult = sb.toString();
//                        byte[] robotResult = inputStream.readAllBytes();
                            flameContext.getKVS().put("hosts", hostName, "robot-result", robotResult);
                        } else {
                            flameContext.getKVS().put("hosts", hostName, "robot-result", (byte[]) null);
                        }


                    }
                    // robot protocol

                    String robotResult = new String(flameContext.getKVS().get("hosts", hostName, "robot-result"));
                    RobotParser robotParser = new RobotParser(robotResult);
                    List<String> userRules = new ArrayList<>();
                    List<String> generalRules = new ArrayList<>();

                    if(robotParser.hasAgent(userAgent)) userRules = robotParser.getRules(userAgent);
                    if (robotParser.hasAgent("*")) generalRules = robotParser.getRules("*");

                    if(robotParser.getRateLimit(userAgent) != null) {
                        rateLimit = robotParser.getRateLimit(userAgent);
                    } else if (robotParser.getRateLimit("*") != null) {
                        rateLimit = robotParser.getRateLimit("*");
                    }
//                    if(!userRules.isEmpty()) {
//                        Iterator<String> rules = userRules.listIterator();
//                        while(rules.hasNext()) {
//                            String rule = rules.next();
//                            if(rule.startsWith("allow:") && realPath.startsWith(rule.substring(6).trim())) {
//                                break;
//                            } else if (rule.startsWith("disallow:") && realPath.startsWith(rule.substring(9).trim())) {
//                                return links;
//                            }
//                        }
//                    }
//                    if(!generalRules.isEmpty()) {
//                        Iterator<String> rules = generalRules.listIterator();
//                        while(rules.hasNext()) {
//                            String rule = rules.next();
//                            if(rule.startsWith("allow:") && realPath.startsWith(rule.substring(6).trim())) {
//                                break;
//                            } else if (rule.startsWith("disallow:") && realPath.startsWith(rule.substring(9).trim())) {
//                                return links;
//                            }
//                        }
//                    }

                    if (Long.parseLong(new String(flameContext.getKVS().get("hosts", hostName, "last-access-time"))) + rateLimit > System.currentTimeMillis()) {
                        links.add(s);
                    } else {
                        flameContext.getKVS().put("hosts", hostName, "last-access-time", Long.toString(System.currentTimeMillis()));

                        URL url = new URL(s);
                        HttpURLConnection headConnection = (HttpURLConnection) url.openConnection();
                        headConnection.setRequestMethod("HEAD");
                        headConnection.setRequestProperty("User-agent", userAgent);
                        headConnection.setInstanceFollowRedirects(false);
                        headConnection.connect();

                        int responseCode = headConnection.getResponseCode();
                        String contentType = headConnection.getContentType();
                        int contentLength = headConnection.getContentLength();

                        if(newRow == null) newRow = new Row(Hasher.hash(s));
                        if (contentType != null) newRow.put("contentType", contentType);
                        if (contentLength != 0) newRow.put("length", Integer.toString(contentLength));


                        newRow.put("url", s);
                        newRow.put("responseCode", Integer.toString(responseCode));
                        if (responseCode == 301 || responseCode == 302 || responseCode == 303 || responseCode == 307 || responseCode == 308) {
                            String redirectedURL = headConnection.getHeaderField("Location");
//                        System.out.println(s + "/ " + responseCode);
                            if (redirectedURL != null) {
//                            System.out.print(redirectedURL + " / ");

                                if (!redirectedURL.contains("http") || !redirectedURL.contains(hostName) || !redirectedURL.contains(portNum)) {
                                    redirectedURL = normalizeURL(redirectedURL, s);
//                                System.out.println(redirectedURL + " / " + s + " / " + responseCode);
                                    logger.info(redirectedURL + " / " + s + " / " + responseCode);
                                }
                                links.add(redirectedURL);
                                flameContext.getKVS().putRow("crawl", newRow);
                                return links;
                            }
                            flameContext.getKVS().putRow("crawl", newRow);
                            return links;
                        }
                        if(responseCode != 200) {
                            flameContext.getKVS().putRow("crawl", newRow);
                            return links;
                        }
                        if (responseCode == 200 && contentType != null && contentType.startsWith("text/html")) {
                            URL getUrl = new URL(s);
                            HttpURLConnection getConnection = (HttpURLConnection) getUrl.openConnection();
                            getConnection.setRequestMethod("GET");
                            getConnection.setRequestProperty("User-agent", userAgent);
                            getConnection.connect();
                            int getResponseCode = getConnection.getResponseCode();
                            if (getResponseCode == 200) {
                                String contentLanguage = getConnection.getHeaderField("Content-Language");
                                if(contentLanguage != null && !contentLanguage.startsWith("en")) return links;

                                InputStream inputStream = getConnection.getInputStream();
                                newRow.put("responseCode", Integer.toString(getResponseCode));
                                byte[] result = inputStream.readAllBytes();
                                String pageContent = new String(result);
                                links = extractURL(pageContent, s, userRules, generalRules, blackLists);


                                if(contentSennMode) {
                                    String resultHash = Hasher.hash(new String(result));
                                    Row contentRow = new Row(Hasher.hash(s));
                                    contentRow.put("content", resultHash);
                                    contentRow.put("url", s);

                                    Iterator<Row> contentRows = flameContext.getKVS().scan("content-seen");
                                    while(contentRows.hasNext()) {
                                        Row row = contentRows.next();
                                        if(row.get("content").equals(resultHash) && !row.get("url").equals(s)) {
                                            newRow.put("canonicalURL", row.get("url"));
//                                    System.out.println("content duplicate: " + row.get("url") + " " + s);
                                            flameContext.getKVS().putRow("crawl", newRow);
                                            return links;
                                        }
                                    }
                                    flameContext.getKVS().putRow("content-seen", contentRow);
                                }
//                                String filteredPage = removePunctuation(filterOutHtmlTags(pageContent)).toLowerCase();
                                newRow.put("page", result);
//                                newRow.put("page", filteredPage.getBytes());
                                newRow.put("url", s);

                                if (ahchorMode) {
                                    List<String> texts = extractAnchorText(pageContent);
                                    Iterator<String> anchorUrls = links.listIterator();
                                    Iterator<String> anchorTexts = texts.listIterator();
                                    while (anchorUrls.hasNext()) {
                                        String anchorUrl = anchorUrls.next();
                                        String anchorText = anchorTexts.next();
//                                System.out.println(anchorUrl + " " + anchorText);
                                        String previousAnchorText = null;
//                                Row anchorRow = new Row(Hasher.hash(anchorUrl));
                                        if (flameContext.getKVS().get("crawl", Hasher.hash(anchorUrl), "anchors:" + s) != null) {
                                            previousAnchorText = new String(flameContext.getKVS().get("crawl", Hasher.hash(anchorUrl), "anchors:" + s));
//                                   System.out.println(anchorUrl + " " + previousAnchorText + " " + s);
                                        }
                                        if(previousAnchorText != null) {
                                            previousAnchorText = previousAnchorText + " " + anchorText;
                                            if(isAllowed(URLParser.parseURL(anchorUrl)[3], userRules, generalRules) && !isBlackListed(anchorUrl, blackLists) && !isFiltered(anchorUrl)) {
//                                        System.out.println(anchorUrl + " " + anchorText + " " + previousAnchorText + " " + s);
                                                flameContext.getKVS().put("crawl", Hasher.hash(anchorUrl), "anchors:" + s, previousAnchorText);
                                            }
                                        } else {
                                            if(isAllowed(URLParser.parseURL(anchorUrl)[3], userRules, generalRules) && !isBlackListed(anchorUrl, blackLists) && !isFiltered(anchorUrl)) {
//                                        System.out.println(anchorUrl + " " + anchorText + " " + previousAnchorText + " " + s);
                                                flameContext.getKVS().put("crawl", Hasher.hash(anchorUrl), "anchors:" + s, anchorText);
                                            }
                                        }
                                    }
                                }
                                flameContext.getKVS().putRow("crawl", newRow);
                            } else {
                                newRow.put("responseCode", Integer.toString(getResponseCode));
                                flameContext.getKVS().putRow("crawl", newRow);
                            }
                        } else {
                            flameContext.getKVS().putRow("crawl", newRow);
                        }
                    }
                } catch (java.net.SocketTimeoutException e) {
                    logger.error("Socket timeout error: " + e.getMessage());
                    return links;
                } catch (java.net.UnknownHostException e) {
                    logger.error("Unknown Host Exception: " + e.getMessage());
                    return links;
                } catch (java.net.ConnectException e) {
                    logger.error("Connect Exception: " + e.getMessage());
                    return links;
                } catch (java.net.SocketException e) {
                    logger.error("Socket Exception: " + e.getMessage());
                    return links;
                } catch (javax.net.ssl.SSLHandshakeException e) {
                    logger.error("SSL Hands hake Exception: " + e.getMessage());
                    return links;
                } catch (javax.net.ssl.SSLException e) {
                    logger.error("SSL Exception: " + e.getMessage());
                    return links;
                } catch (java.net.MalformedURLException e) {
                    logger.error("Malformed URL Exception: " + e.getMessage());
                    return links;
                } catch (java.net.ProtocolException e) {
                    logger.error("Protocol Exception: " + e.getMessage());
                    return links;
                } catch (java.util.ConcurrentModificationException e) {
                    logger.error("Concurrent Modification Exception: " + e.getMessage());
                    return links;
                }




//                catch (Exception e) {
//                    logger.error("Other Exceptions: " + e.getMessage());
//                    return links;
//                }


                return links;
            });
//            Thread.sleep(1000);
        }
    }
    public static Boolean isBlackListed(String url, List<String> blackListPatterns) throws IOException {
        if(!hasBlackList) return false;
        Iterator<String> blackLists = blackListPatterns.listIterator();
        while(blackLists.hasNext()) {
            String pattern = blackLists.next();
            if(pattern!=null) System.out.println(pattern);
            Pattern blackListPattern = Pattern.compile(pattern);
            Matcher blackMatcher = blackListPattern.matcher(url);
            if(blackMatcher.find()){
                System.out.println(blackMatcher.group());
                return true;
            }
        }
        return false;
    }

    public static Boolean isFiltered(String url) {
        // filtering for method
        String method = URLParser.parseURL(url)[0];
        String path = URLParser.parseURL(url)[3];

        if (!method.equals("http") && !method.equals("https")) {
//            System.out.println("method is not http or https");
            logger.info("method is not http or https");
            return true;
        }
        // filtering for file extensions
        if (path.contains(".jpg") || path.contains(".jpeg")
                || path.contains(".gif") || path.contains(".png") ||  path.contains(".txt")
                || path.contains(".bmp") || path.contains(".mp3") || path.contains(".mp4")
                || path.contains(".avi") || path.contains(".mov") || path.contains(".wmv")
                || path.contains(".zip") || path.contains(".rar") || path.contains(".tar.gz")
                || path.contains(".doc") || path.contains(".xls") || path.contains(".pdf")
                || path.contains(".js") || path.contains(".css") || path.contains(".less")
                || path.contains(".exe") || path.contains(".dll") || path.contains(".ini")
                || path.contains(".sys")

        ) {
            logger.info("invalid contents");
//            System.out.println("invalid contents: .jpg, jpeg, .gif, .png, or .txt");
            return true;
        }
        return false;
    }


    public static Boolean isAllowed(String path, List<String> userRules, List<String> generalRules) {
        boolean result = true;
        if(!userRules.isEmpty()) {
            Iterator<String> rules = userRules.listIterator();
            while(rules.hasNext()) {
                String rule = rules.next();
                if(rule.startsWith("allow:") && path.startsWith(rule.substring(6).trim())) {
                    result = true;
                    return result;
                } else if (rule.startsWith("disallow:") && path.startsWith(rule.substring(9).trim())) {
//                    System.out.println(rule + " " + path);
                    result = false;
                    return result;
                }
            }
        }
        if(!generalRules.isEmpty()) {
            Iterator<String> rules = generalRules.listIterator();
            while(rules.hasNext()) {
                String rule = rules.next();
                if(rule.startsWith("allow:") && path.startsWith(rule.substring(6).trim())) {
                    result = true;
                    return result;
                } else if (rule.startsWith("disallow:") && path.startsWith(rule.substring(9).trim())) {
//                    System.out.println(rule + " " + path);
                    result = false;
                    return result;
                }
            }
        }
        return result;
    }

    public static String normalizeURL(String url, String seedURL) {
        String normalizedURL = "";
        String method = URLParser.parseURL(url)[0];
        String domain = URLParser.parseURL(url)[1];
        String portNum = URLParser.parseURL(url)[2];
        String path = URLParser.parseURL(url)[3];

        String seedURLMethod = URLParser.parseURL(seedURL)[0];
        String seedURLDomain = URLParser.parseURL(seedURL)[1];
        String seedURLPath = URLParser.parseURL(seedURL)[3];

        if(method == null && seedURL != null) method = seedURLMethod;
        if(domain == null && seedURL != null) domain = seedURLDomain;
        if (portNum == null) {
            if(method.equals("http")) {
                portNum = "80";
            } else if (method.equals("https")) {
                portNum = "443";
            }
        }
        if(path.contains("#")) {
            path = path.split("#")[0];
        }

        if(path.contains("..")) {
            int countDots = 0;
            Pattern dotsPattern = Pattern.compile("\\.\\.");
            Matcher dotsMatcher = dotsPattern.matcher(path);
            while(dotsMatcher.find()){
                countDots++;
            }
            String realPath = path.split("\\.\\.")[path.split("\\.\\.").length-1];
            String levelUpPath = "";

            if (countDots > seedURLPath.split("/").length -2) normalizedURL = "";
            for (int i = 0; i < (seedURLPath.split("/").length - countDots -2); i++) {
                levelUpPath = levelUpPath + (levelUpPath.equals("") ? "/" : "/") + seedURLPath.split("/")[i+1];
            }
            normalizedURL = method + "://"+ domain + ":" + portNum + levelUpPath + realPath;

        } else {
            if(path.charAt(0)=='/') {
                normalizedURL = method + "://"+ domain + ":" + portNum + path;
            } else {
                String newSeedURLPath = seedURLPath.substring(0,seedURLPath.lastIndexOf("/"));
                normalizedURL = method + "://"+ domain + ":" + portNum + newSeedURLPath + "/" + path;
            }
        }
        return  normalizedURL;
    }

    public static List<String> extractAnchorText(String HTMLPage) {
        Pattern linkPattern = Pattern.compile("<a[^>]+href=[\"']?([\"'>]+)[\"']?[^>]*>(.+?)<\\/a>", Pattern.CASE_INSENSITIVE|Pattern.DOTALL);
        Matcher pageMatcher = linkPattern.matcher(HTMLPage);
        ArrayList<String> anchorTexts = new ArrayList<>();
        while(pageMatcher.find()) {
            String fromHref = pageMatcher.group().split("href=\"")[1].split("\">")[0];
            String anchorText = pageMatcher.group().split("href=\"")[1].split("\">")[1].split("</a>")[0];
//            System.out.println(anchorText);
            anchorTexts.add(anchorText);
        }
        return anchorTexts;
    }

    public static List<String> extractURL(String HTMLPage, String seedURL, List<String> userRules, List<String> generalRules, List<String> blackLists) throws IOException {
        HTMLPage = HTMLPage.replaceAll(",", "%2c");
        Pattern linkPattern = Pattern.compile("<a[^>]+href=[\"']?([\"'>]+)[\"']?[^>]*>(.+?)<\\/a>", Pattern.CASE_INSENSITIVE|Pattern.DOTALL);
        Matcher pageMatcher = linkPattern.matcher(HTMLPage);
        ArrayList<String> links = new ArrayList<>();
        String finalLink;

        String seedURLMethod = URLParser.parseURL(seedURL)[0];
        String seedURLDomain = URLParser.parseURL(seedURL)[1];
        String seedURLPortNum = URLParser.parseURL(seedURL)[2];
        String seedURLPath = URLParser.parseURL(seedURL)[3];

        if (seedURLPortNum == null) {
            if(seedURLMethod.equals("http")) {
                seedURLPortNum = "80";
            } else if (seedURLMethod.equals("https")) {
                seedURLPortNum = "443";
            }
        }
        while(pageMatcher.find()){
            if(pageMatcher.group().split("href=\"").length < 2) continue;
            String fromHref = pageMatcher.group().split("href=\"")[1].split("\"")[0];

            String result[] = URLParser.parseURL(fromHref);
            String method = result[0];
            String domainName = result[1];
            String portNum = result[2];
            String path = result[3];

            if (method != null && portNum == null) {
                if (method.equals("http")) portNum = "80";
                else if (method.equals("https")) portNum = "443";
            }
            if(path.isEmpty() || path == null) continue;
            if(domainName == null) {
                if(path.contains("#")) {
                    if (path.split("#").length < 1) continue;
                    String realPath = path.split("#")[0];
                    if(realPath.isEmpty()) {
                        continue;
                    } else {
                        String newSeedURLPath = seedURLPath.substring(0,seedURLPath.lastIndexOf("/"));
                        finalLink = seedURLMethod +  "://" + seedURLDomain+ ":" + seedURLPortNum + newSeedURLPath + "/" + realPath;
                        if(isAllowed(newSeedURLPath + "/" + realPath, userRules, generalRules) && !isBlackListed(finalLink, blackLists) && !isFiltered(finalLink)) links.add(finalLink);
                    }
                } else {
                    if(path.contains("..")) {

                        int countDots = 0;
                        Pattern dotsPattern = Pattern.compile("\\.\\.");
                        Matcher dotsMatcher = dotsPattern.matcher(path);
                        while(dotsMatcher.find()){
                            countDots++;
                        }
                        if(path.split("\\.\\.").length < 1) continue;
                        String realPath = path.split("\\.\\.")[path.split("\\.\\.").length-1];
                        String levelUpPath = "";

                        if (countDots > seedURLPath.split("/").length -2) continue;
                        for (int i = 0; i < (seedURLPath.split("/").length - countDots -2); i++) {
                            levelUpPath = levelUpPath + (levelUpPath.equals("") ? "/" : "/") + seedURLPath.split("/")[i+1];
                        }
                        finalLink = seedURLMethod + "://"+ seedURLDomain + ":" + seedURLPortNum + levelUpPath + realPath;
                        if(isAllowed(path.substring(path.indexOf("..")+2), userRules, generalRules) && !isBlackListed(finalLink, blackLists) && !isFiltered(finalLink)) links.add(finalLink);
                    } else {
                        if(path.length() == 0) continue;
                        if(path.charAt(0)=='/') {
                            finalLink = seedURLMethod +  "://" + seedURLDomain + ":" + seedURLPortNum + path;
                            if(isAllowed(path, userRules, generalRules) && !isBlackListed(finalLink, blackLists) && !isFiltered(finalLink)) links.add(finalLink);
                        } else {
                            String newSeedURLPath = seedURLPath.substring(0,seedURLPath.lastIndexOf("/"));
                            finalLink = seedURLMethod +  "://" + seedURLDomain + ":" + seedURLPortNum + newSeedURLPath + "/" + path;
                            if(isAllowed(newSeedURLPath + "/" + path, userRules, generalRules) && !isBlackListed(finalLink, blackLists) && !isFiltered(finalLink)) links.add(finalLink);
                        }
                    }
                }
            } else {
                if(path.contains("#")) {
                    String realPath = path.split("#")[0];
                    if (realPath.isEmpty()) {
                        finalLink = seedURLMethod + "://" + domainName + ":" + portNum + seedURLPath;
                        links.add(finalLink);
                    } else {
                        String newSeedURLPath = seedURLPath.substring(0, seedURLPath.lastIndexOf("/"));
                        finalLink = seedURLMethod + "://" + domainName + ":" + portNum + newSeedURLPath + "/" + realPath;
                        links.add(finalLink);
                    }
                } else {
                    if(path.length() == 0) continue;
                    finalLink = method +  "://" + domainName + ":" + portNum + path;
                    if(isAllowed(path, userRules, generalRules) && !isBlackListed(finalLink, blackLists) && !isFiltered(finalLink)) links.add(finalLink);
                }
            }
        }
        return links;
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
}