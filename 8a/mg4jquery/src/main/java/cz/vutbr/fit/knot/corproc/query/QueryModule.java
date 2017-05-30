/**
 * cz.vutbr.fit.knot.corproc.main.Main class. Runs multiple threads which are requesting servers.
 * Each thread maintain one server.
 */

package cz.vutbr.fit.knot.corproc.query;

import org.json.JSONException;
import org.json.JSONObject;


import cz.vutbr.fit.knot.corproc.printer.ResponsePrinter;


import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;

import java.util.List;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class QueryModule {
    /**
     * Resurns number of received snippets.
     * @param results
     * @return
     * @throws JSONException
     */
    private static int numberOfRespondSnippets(List<JSONObject> results) throws JSONException {
        int allSnippets = 0;
        for (JSONObject result:
                results) {
            allSnippets += result == null ? 0 : result.getInt("recivedSnippets");
        }
        return allSnippets;
    }

    /**
     * From array with all responses (response). Return response from 'hostname' server.
     * @param hostname
     * @return
     * @throws JSONException
     */
    protected static JSONObject getResponseFromHost(String hostname, List<JSONObject> response) throws JSONException{
        for (JSONObject res:
                response) {
            if (res.getString("hostname").equals(hostname)){
                return res;
            }
        }
        return null;
    }

    /**
     * Return list of hosts which potentially can return more snippets.
     * @param results
     * @return
     * @throws JSONException
     */
    private static List<String> getNonEmptyHosts(List<JSONObject> results) throws JSONException {
        List<String> hosts = new ArrayList<>();
        for (JSONObject res:
                results) {
            if (res != null && res.getInt("recivedSnippets") >= res.getInt("expectedSnippets") && res.getJSONArray("snippetsFromHost").length() > 0){
                hosts.add(res.getString("hostname"));
            }
        }
        return hosts;
    }


    /**
     * Check if host is available.
     * @param server
     * @param params
     * @return
     */
    private static boolean hostAvailabilityCheck(String server, QueryParameters params) {
        Pattern regExp = Pattern.compile("(.*?):(.*)");
        Matcher m = regExp.matcher(server);
        int timeout = params.getInt("timeout");
        if (m.find()) {
            server = m.group(1);
            Integer port = Integer.parseInt(m.group(2));
            try {
                SocketAddress sockaddr = new InetSocketAddress(server, port);
                Socket s = new Socket();
                s.connect(sockaddr, timeout);
                s.close();
                return true;
            } catch (Exception ex) {
                System.err.println(ex.getMessage());
            }
        }
        return false;
    }

    /**
     * Runs threads which will be communicating with servers.
     * @param hosts
     * @param params
     * @throws JSONException
     * @throws IOException
     * @throws InterruptedException
     */
    public static void startQuery(List<String> hosts, QueryParameters params, List<JSONObject> response, ResponsePrinter printer, Map<String, Integer> offsetsMap) throws JSONException, IOException, InterruptedException, ClassNotFoundException {

        if (offsetsMap != null){
            hosts = new ArrayList<>(offsetsMap.keySet());
        }
        do{
            List<QueryThread> queries = new ArrayList<>();

            for (String host:
                    hosts) {
                if (hostAvailabilityCheck(host, params)) {
                    QueryThread q = new QueryThread();
                    q.setResponse(response);
                    q.setParams(params);
                    q.setResponsibleHost(host);

                    if (response.size() <= 0){
                        // First request on the server
                        if (offsetsMap != null){
                            q.setOffset(offsetsMap.get(host));
                        }else{
                            q.setOffset(0);
                        }
                        q.setExpectedSnippets((int) Math.ceil((double)params.getInt("snippets")/hosts.size()));
                    }else{
                        JSONObject hostsResp = getResponseFromHost(host, response);
                        if (hostsResp != null){
                            q.setOffset(hostsResp.getInt("offset"));
                            q.setExpectedSnippets((int) Math.ceil(params.getInt("snippets") - numberOfRespondSnippets(response)) / hosts.size());
                        }
                    }

                    q.start();
                    queries.add(q);
                }
            }
            for (QueryThread q : queries) {
                q.join();
            }
            hosts = getNonEmptyHosts(response);


            printer.print(response);

        }while(numberOfRespondSnippets(response) != 0 && (numberOfRespondSnippets(response) < params.getInt("snippets") || params.getInt("snippets") == 0) && hosts.size() > 0 && !params.getBoolean("getFullDocument"));
    }

}
