/**
 * Class process input cz.vutbr.fit.knot.corproc.parameters from and configuration in XML format.
 * All input cz.vutbr.fit.knot.corproc.parameters and configurations from XML will be stored in 'params' map.
 */

package cz.vutbr.fit.knot.corproc.parameters;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import cz.vutbr.fit.knot.corproc.query.Query;


public class Parameters{
	//Holds all input cz.vutbr.fit.knot.corproc.parameters and configurations.
	private Map<String, Object> params;
	//For processing input cz.vutbr.fit.knot.corproc.parameters.
	private JSAPResult jsapResult;
	private int document;

	private List<String> allowedFields = Arrays.asList("attributes", "nertags");

	public void setParams(Map<String, Object> p){
		this.params = p;
	}
	public Map<String, Object> getParams(){
		return this.params;
	}

	/**
	 * Method process input cz.vutbr.fit.knot.corproc.parameters.
	 * @param args
	 * @throws JSAPException
	 */
	public Parameters(String args[]) throws JSAPException {
		SimpleJSAP jsap = new SimpleJSAP(Query.class.getName(), "Queries distributed mg4j index.",
				new Parameter[] {
						new FlaggedOption("query", JSAP.STRING_PARSER, "", JSAP.NOT_REQUIRED, 'q', "query",
								"A query should be processed."),
						new FlaggedOption("constraint", JSAP.STRING_PARSER, "", JSAP.NOT_REQUIRED, 'c', "constraint",
								"Post-processing filter."),
						new FlaggedOption("hosts", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'h', "hosts",
						"List of hosts with server application."),
						new FlaggedOption("config", JSAP.STRING_PARSER, "config.xml", JSAP.REQUIRED, 'p', "config",
						"Path to configuration XML."),
						new FlaggedOption("document", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'd', "document",
								"Document identifier."),
						new FlaggedOption("readTimeout", JSAP.STRING_PARSER, "0", JSAP.NOT_REQUIRED, 'r', "readTimeout",
								"Read timeout defines how long should client's application wait for response in milliseconds."),
						new FlaggedOption("thread", JSAP.INTEGER_PARSER, "0", JSAP.NOT_REQUIRED, 't', "thread",
								"ID of thread."),
						new FlaggedOption("snippets", JSAP.INTEGER_PARSER, "100", JSAP.NOT_REQUIRED, 's', "snippets",
								"Number of snippets."),
						new FlaggedOption("keeper", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'k', "keeper",
								"Host keeps the document."),
						new FlaggedOption("allowedFields", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'f', "allowedFields",
								"Ask host to return a list of exists fields. Could be 'attributes' or 'nertags'."),
						new Switch("getFullDocument", 'g', "get", "Need be set in case of getting full document."),
						new Switch("errors", 'e', "err", "In case of -e/--err was not set, stderr will be ignored."),
						new Switch("next", 'n', "next", "Require next 'snippets' from the servers.")
				});
		this.jsapResult = jsap.parse(args);
		this.params = new HashMap<>();
		if (!jsapResult.success()) {
			System.out.println(jsap.getHelp());
			System.exit(1);
		}
	}

	public void setObject(String key, Object value){
		params.put(key, value);
	}

	/**
	 * Method process XML configuration file and fill 'params' map.
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	public void process() throws Exception {
		this.parseXML(this.jsapResult.getString("config"));

		String query = this.jsapResult.getString("query");
		String constraint = this.jsapResult.getString("constraint");
		String hosts = this.jsapResult.getString("hosts") == null ? (String) params.get("hosts") : this.jsapResult.getString("hosts");
		String logfile = this.jsapResult.getString("logfile");


		putNotNull("query", query);
		putNotNull("constraint", constraint);
		putNotNull("logfile", logfile);
		putNotNull("hosts", hosts);
		putNotNull("keeper", this.jsapResult.getString("keeper"));
		putNotNull("document", this.jsapResult.getString("document"));
		params.put("getFullDocument", this.jsapResult.getBoolean("getFullDocument"));
		params.put("snippets", this.jsapResult.getInt("snippets"));
		params.put("listOfHosts", getHosts());
		params.put("error", this.jsapResult.getBoolean("errors"));
		params.put("next", this.jsapResult.getBoolean("next"));
		params.put("thread", this.jsapResult.getInt("thread"));
		params.put("readTimeout", this.jsapResult.getString("readTimeout"));
		if (this.jsapResult.getString("allowedFields") == null ||
				allowedFields.contains(this.jsapResult.getString("allowedFields"))){
			params.put("allowedFields", this.jsapResult.getString("allowedFields"));
		}else {
			throw new Exception("Inappropriate value for -f/--allowedFields parameter");
		}

	}


	private void putNotNull(String key, String value) {
		if (value != null)
			params.put(key, value);
	}


	public String getString(String key) {
		String value = Objects.toString(params.get(key));
		return (value == null) ? null : value.trim();
	}




	public Map<String, String> getDye()
	{
		return (Map<String, String>) params.get("dye");
	}


	public int getInt(String key) {
		String mapValue = getString(key);
		if (mapValue != null) {
			int value = Integer.parseInt(getString(key));
			return value;
		}
		return (Integer) null;
	}

	public Object getObject(String key){
		return params.get(key);
	}

	public boolean getBoolean(String key){
		return (boolean) params.get(key);
	}

	private void setDefault() {
		params.put("timeout", 1000);
		params.put("raw", false);
		params.put("highlightOn", true);
		params.put("constraint", "nul");
		params.put("color", "white");
	}

	/**
	 * Process content of <nertags></nertags> field in XML configuration file.
	 * @param nertags
	 * @return
	 */
	private Map<String, List<String>> processNertags(Node nertags)
	{
		Map<String, List<String>> mapNertags = new HashMap<>();
		NodeList nodes = nertags.getChildNodes();
		for (int nertagIndex = 0; nertagIndex < nodes.getLength(); ++nertagIndex) {
			if (nodes.item(nertagIndex) != null && nodes.item(nertagIndex).getNodeType() == Node.ELEMENT_NODE){
				NodeList attribs = nodes.item(nertagIndex).getChildNodes();
				List<String> attributes = new ArrayList<>();

				for (int attribIndex = 0; attribIndex < attribs.getLength(); ++attribIndex) {
					if (attribs.item(attribIndex) != null && attribs.item(attribIndex).getNodeType() == Node.ELEMENT_NODE){
						attributes.add(attribs.item(attribIndex).getNodeName());
					}
				}
				mapNertags.put(nodes.item(nertagIndex).getNodeName(), attributes);
			}
		}
		return mapNertags;
	}

	/**
	 * Process content of <dye></dye> field in XML configuration file.
	 * @param dyeNode
	 * @return
	 */
	private Map<String, String> processDye(Node dyeNode)
	{
		Map<String, String> mapDye = new HashMap<>();
		NodeList dyes = dyeNode.getChildNodes();
		for (int dyeIndex = 0; dyeIndex < dyes.getLength(); ++dyeIndex)
		{
			if (dyes.item(dyeIndex) != null && dyes.item(dyeIndex).getNodeType() == Node.ELEMENT_NODE)
			{
				mapDye.put(dyes.item(dyeIndex).getNodeName(), dyes.item(dyeIndex).getTextContent());
			}
		}
		return mapDye;
	}

	/**
	 * Process content of <format></format> field in XML configuration file.
	 * @param formatNode
	 * @return
	 */
	private Map<String, Object> processFormat(Node formatNode){
		Map<String, Object> format = new HashMap<>();
		Map<String, List<String>> nertags = new HashMap<>();
		List<String> fields = new ArrayList<>();
		NodeList formatList = formatNode.getChildNodes();	//Get fields and nertags nodes
		for (int index = 0; index < formatList.getLength(); ++ index){
			if (formatList.item(index) != null && formatList.item(index).getNodeType() == Node.ELEMENT_NODE){
				if (formatList.item(index).getNodeName().equals("attributes")){
					NodeList fieldsContent = formatList.item(index).getChildNodes();

					for (int fieldindex = 0; fieldindex < fieldsContent.getLength(); ++fieldindex){
						if (fieldsContent.item(fieldindex) != null && fieldsContent.item(fieldindex).getNodeType() == Node.ELEMENT_NODE){
							fields.add(fieldsContent.item(fieldindex).getNodeName());
						}
					}
				}
				if (formatList.item(index).getNodeName().equals("nertags")){
					nertags = processNertags(formatList.item(index));
					format.put("nertags", nertags);
				}
			}
		}
		format.put("attributes", fields);
		return format;
	}

	/**
	 * Process content of <head></head> field in XML configuration file.
	 * @param headNode
	 * @return
	 */
	private List<String> processHead(Node headNode){
		List<String> head = new ArrayList<>();
		NodeList headContent = headNode.getChildNodes();
		for (int index = 0; index < headContent.getLength(); ++index){
			if (headContent.item(index) != null && headContent.item(index).getNodeType() == Node.ELEMENT_NODE){
				head.add(headContent.item(index).getNodeName());
			}
		}
		return  head;
	}


	/**
	 * Process ann XML configuration file.
	 * @param file
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 */
	private void parseXML(String file) throws ParserConfigurationException, SAXException {
		File inputFile = new File(file);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = null;
		try {
			doc = dBuilder.parse(inputFile);
		} catch (IOException e) {
			System.err.println(e.getMessage());
			this.setDefault();
			return;
		}	//Holds tags interesting for us.
		NodeList nodes = doc.getDocumentElement().getChildNodes();
		Map<String, String> dye = new HashMap<>();
		List<String> head = new ArrayList<>();
		Map<String, Object> format = new HashMap<>();
		for (int i = 0; i < nodes.getLength(); i++) {
			if (nodes.item(i) != null && nodes.item(i).getNodeType() == Node.ELEMENT_NODE) {
				NodeList childs = nodes.item(i).getChildNodes();
				for (int j = 0; j < childs.getLength(); j++) {
					if (childs.item(j) != null && childs.item(j).getNodeType() == Node.ELEMENT_NODE) {

						Element node = (Element) childs.item(j);

						if (nodes.item(i).getNodeName().equals("format")) {
							format = processFormat(nodes.item(i));
						}
						if (nodes.item(i).getNodeName().equals("dye")) {
							dye = processDye(nodes.item(i));
						}
						if (nodes.item(i).getNodeName().equals("head")) {
							head = processHead(nodes.item(i));
						}
						if (!nodes.item(i).getNodeName().equals("head") && !nodes.item(i).getNodeName().equals("format") && !nodes.item(i).getNodeName().equals("dye")) {
							params.put(node.getNodeName(), node.getTextContent());
						}

					}
				}
			}
		}
		params.put("dye", dye);
		params.put("format", format);
		params.put("head", head);
	}


	private List<String> getHosts(){
		List<String> hosts = new ArrayList<>();

		try{
			BufferedReader br = new BufferedReader(new FileReader(new File(this.getString("hosts"))));
			String line;
			Pattern withoutPortPtrn = Pattern.compile("[^:]+");

			while ((line = br.readLine()) != null) {
				Matcher withoutPortMtch = withoutPortPtrn.matcher(line);
				if (withoutPortMtch.matches()){
					line = line + ":" + this.getInt("port");
				}
				hosts.add(line);
			}
			br.close();
		}catch (Exception ex){
			System.err.println(ex.getMessage());
		}

		return hosts;

	}
}
