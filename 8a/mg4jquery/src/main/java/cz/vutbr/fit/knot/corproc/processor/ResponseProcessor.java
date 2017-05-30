/**
 * Class formatting data from the server.
 */

package cz.vutbr.fit.knot.corproc.processor;


import cz.vutbr.fit.knot.corproc.colorizer.Color;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;


public class ResponseProcessor {
	private String style;
	private Map<String, String> dye;
	private String title;
	private String uri;
	private Long document;
	private int thread;
	private boolean isHead = false;
	private String host;



	public String optimize(String data) {
		data = data.replaceAll("\\p{javaSpaceChar}*[¶|§]\\p{javaSpaceChar}*", "").trim();
		return data;
	}

	/**
	 * Return color for nertag.
	 * @param me
	 * @return
	 */
	private String getMyColor(String me) {
		Pattern whiteSpacesPattern = Pattern.compile("\\s+");
		Matcher whiteSpacesMatcher = whiteSpacesPattern.matcher(dye.get(me));
		String myColor = whiteSpacesMatcher.replaceAll("");
		for (Color color:
				Color.values()) {
			if (color.toString().toLowerCase().equals(myColor.toLowerCase()))
				return color.getColor();
		}

		return "";
	}

	/**
	 * Colorize nertag by color defined in the configuration file.
	 * @param me
	 * @param data
	 * @return
	 */
	private String colorizeMe(String me, String data) {
		String myColor = getMyColor(me);

		Pattern openTagPattern = Pattern.compile("<" + me);
		Pattern closeTagPattern = Pattern.compile("</" + me + ">");
		Matcher openTagMatcher  = openTagPattern.matcher(data);
		String coloredOpenTag = openTagMatcher.replaceAll(myColor + "<" + me);
		Matcher closeTagMatcher = closeTagPattern.matcher(coloredOpenTag);

		if (myColor.equals(""))
			return coloredOpenTag;

		return closeTagMatcher.replaceAll("</" + me + ">" + "\u001B[0m");
	}

	/**
	 * Call 'colorizeMe' method for every nertag.
	 * In case of <head></head> is needed, calls method for creating head.
	 * @param data
	 * @return
	 */
	public String processDataChunk(String data) {

		if (style.toLowerCase().equals("raw")) {
			data = colorizeMe("person", data);
			data = colorizeMe("artist", data);
			data = colorizeMe("location", data);
			data = colorizeMe("artwork", data);
			data = colorizeMe("event", data);
			data = colorizeMe("museum", data);
			data = colorizeMe("family", data);
			data = colorizeMe("group", data);
			data = colorizeMe("nationality", data);
			data = colorizeMe("form", data);
			data = colorizeMe("mythology", data);
			data = colorizeMe("medium", data);
			data = colorizeMe("movement", data);
			data = colorizeMe("genre", data);
			data = colorizeMe("date", data);
			data = colorizeMe("interval", data);
		}

		if (isHead){
			data = attacheHead(data);
		}

		return data;
	}

	/**
	 * Define amount of spaces.
	 * Need for preatty head.
	 * @param maxLength
	 * @param currLength
	 * @return
	 */
	private String getSpaces(int maxLength, int currLength){
		StringBuilder stringBuilder = new StringBuilder();
		for (int i = currLength; i < maxLength; ++i){
			stringBuilder.append(" ");
		}
		return stringBuilder.toString();
	}


	/**
	 * Add field defined in <head></head>.
	 * @param data
	 * @return
	 */
	private String attacheHead(String data){

		String title = "";
		String uri = "";
		String document = "";
		String host = "";
		String thread = "";

		if (this.title != null){
			title = "title: " + this.title;
		}

		if (this.uri != null){
			uri = "uri: " + this.uri;
		}

		if (this.host != null){
			host = "host: " + this.host;
		}

		if (this.document != null){
			document = "document: " + this.document;
		}
		if (this.thread >= 0){
			thread = "thread: " + this.thread;
		}

		int length = -1;

		length = title.length() > length ? title.length() : length;
		length = uri.length() > length ? uri.length() : length;
		length = document.length() > length ? document.length() : length;
		length = thread.length() > length ? thread.length() : length;
		length = host.length() > length ? host.length() : length;

		StringBuilder headedData = new StringBuilder();

		for (int i = -2; i <= length + 1; ++i){
			headedData.append("-");
		}

		headedData.append("\n");

		headedData.append(title.length() > 0 ? "| " + title + getSpaces(length, title.length()) + " |\n" : "");
		headedData.append(uri.length() > 0 ? "| " + uri + getSpaces(length, uri.length()) + " |\n" : "");
		headedData.append(document.length() > 0 ? "| " + document + getSpaces(length, document.length()) + " |\n" : "");
		headedData.append(thread.length() > 0 ? "| " + thread + getSpaces(length, thread.length()) + " |\n" : "");
		headedData.append(host.length() > 0 ? "| " + host + getSpaces(length, host.length()) + " |\n" : "");

		for (int i = -2; i <= length + 1; ++i){
			headedData.append("-");
		}
		headedData.append("\n");

		headedData.append(data);

		return headedData.toString();
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public void setUri(String uri) throws UnsupportedEncodingException {
		this.uri = URLDecoder.decode(uri, "UTF-8");
	}



	public void setDocument(Long document) {
		this.document = document;
	}



	public void setThread(int thread) {
		this.thread = thread;
	}



	public void setHead(boolean head) {
		isHead = head;
	}


	public void setHost(String host) {
		this.host = host;
	}

	public Map<String, String> getDye() {
		return dye;
	}

	public void setDye(Map<String, String> dye) {
		this.dye = dye;
	}

	public String getStyle() {
		return style;
	}

	public void setStyle(String style) {
		this.style = style;
	}
}
