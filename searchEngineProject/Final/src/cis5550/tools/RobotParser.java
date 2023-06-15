package cis5550.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RobotParser {
//    private Map<String, List<String>> allowedRules = new HashMap<>();
//    private Map<String, List<String>> disallowedRules = new HashMap<>();
    private Map<String, List<String>> rules = new HashMap<>();
    private Map<String, Long> crawlDelay = new HashMap<>();
    public RobotParser(String robotsResult) throws IOException {

        String agent = "";
        String[] lines = robotsResult.split("\n");

        for (String line : lines) {
            // Ignore blank lines and comments
            if (line.isEmpty() || line.startsWith("#")) continue;

            if (line.toLowerCase().startsWith("user-agent:")) {
                agent = line.substring(11).trim().toLowerCase();
                rules.put(agent, new ArrayList<>());
            }
            if (line.toLowerCase().startsWith("disallow:")) {
                String path = line.trim();
                addPath(agent, path);
            }
            if (line.toLowerCase().startsWith("allow:")) {
                String path = line.trim();
                addPath(agent, path);
            }
            if (line.toLowerCase().startsWith("crawl-delay:")) {
                String trimmedLine = line.substring(12).trim().toLowerCase();
                String converted = trimmedLine.replaceAll("[^0-9.]+", "");
                Long rateLimit = (long) Double.parseDouble(converted) * 1000;
//                System.out.println(agent + " " + rateLimit);
                crawlDelay.put(agent, rateLimit);
            }
        }
    }

    public Long getRateLimit(String agent) {
        return crawlDelay.get(agent);
    }

    public Boolean hasAgent(String agent) {
        return rules.keySet().contains(agent);
    }

        private void addPath(String agent, String path) {
        List<String> paths = rules.get(agent);
        if (paths == null) {
            paths = new ArrayList<>();
            rules.put(agent, paths);
        }
        paths.add(path);
    }

//    private void addAllowedPath(String agent, String path) {
//        List<String> paths = allowedRules.get(agent);
//        if (paths == null) {
//            paths = new ArrayList<>();
//            allowedRules.put(agent, paths);
//        }
//        paths.add(path);
//    }
//
//    private void addDisallowedPath(String agent, String path) {
//        List<String> paths = disallowedRules.get(agent);
//        if (paths == null) {
//            paths = new ArrayList<>();
//            disallowedRules.put(agent, paths);
//        }
//        paths.add(path);
//    }

//    public List<String> getAllowRules(String agent) {
//        return allowedRules.get(agent.toLowerCase());
//    }
//
//    public List<String> getDisallowRules(String agent) {
//        return disallowedRules.get(agent.toLowerCase());
//    }

    public List<String> getRules(String agent) {
        return rules.get(agent.toLowerCase());
    }



}