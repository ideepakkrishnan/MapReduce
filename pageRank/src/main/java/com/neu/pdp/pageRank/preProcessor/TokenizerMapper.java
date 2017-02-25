/**
 * 
 */
package com.neu.pdp.pageRank.preProcessor;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;

import com.neu.pdp.resources.KeyPair;
import com.neu.pdp.resources.WikiParser;

/**
 * Pre-processing mapper.
 * This class extracts external links from a page and passes
 * it on to the reducer.
 * @author ideepakkrishnan
 */
public class TokenizerMapper extends Mapper<Object, Text, KeyPair, Text> {
	
	// Class level variables
	private Pattern namePattern;
	private Pattern linkPattern;
	private SAXParserFactory spf;
	private SAXParser saxParser;
	private XMLReader xmlReader;
	private HashSet<String> linkPageNames;
	private HashSet<String> allPageNames;
	
	/**
	 * Initializes the XML parser which will be used by the mappers
	 * to extract out-links
	 * @param context Current application context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void setup(Context context) 
			throws IOException, InterruptedException {
		
		allPageNames = new HashSet<String>();
		
		// Configure the regex patterns:
		// To keep only html pages not containing tilde (~)
		namePattern = Pattern.compile("^([^~]+)$");
		// To keep only html filenames having relative paths and not
		// containing tilde (~)
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
		
		try {
			
			// Configure the XML parser
			spf = SAXParserFactory.newInstance();
			
			spf.setFeature(
					"http://apache.org/xml/features/nonvalidating/load-external-dtd", 
					false);
			
			spf.setFeature(
					"http://apache.org/xml/features/continue-after-fatal-error", 
					true);
			
			saxParser = spf.newSAXParser();
			xmlReader = saxParser.getXMLReader();
			
			// Initialize the List object to store out-links
			linkPageNames = new LinkedHashSet<String>();
			
			xmlReader.setContentHandler(
					new WikiParser(linkPageNames, linkPattern));
			
		} catch (SAXNotRecognizedException e) {			
			e.printStackTrace();
		} catch (SAXNotSupportedException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * Reads data from a compressed bz2 file and extracts
	 * the href values for anchor tags inside the div tag
	 * with id 'bodyContent'. These extracted links are
	 * passed on to the reducer as key-value pairs where
	 * key represents the current page and value represents
	 * an external link from that page.
	 * @param key The input key
	 * @param value The input value as Text
	 * @param context The current context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
		
		// Local variables
		String strLine, strPageName, strHtml, strEmitVal = "";
		int iDelimLoc;
		Matcher matcher;
		
		strLine = value.toString();
		
		// Each line is formatted as (Wiki-page-name:Wiki-page-html)
		iDelimLoc = strLine.indexOf(':');
		
		// Split and get the page name and corresponding html
		strPageName = strLine.substring(0, iDelimLoc);
		strHtml = strLine.substring(iDelimLoc + 1);
		
		// Fix system identifier in <!DOCTYPE .. > if the tag is
		// malformed.
		// Since this doesn't affect our computation, do a blind
		// rewrite of <!DOCTYPE .. >
		if (strHtml.substring(0, 9).equals("<!DOCTYPE")) {
			int index = strHtml.indexOf('>');
			strHtml = "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">"
					+ strHtml.substring(index + 1);
		}
		
		// Encode '&' with '&amp;'
		strHtml = strHtml.replaceAll("&", "&amp;");
		
		// Extract external links from bodyContent of HTML			
		matcher = namePattern.matcher(strPageName);
		if (matcher.find()) {
			// Parse page and fill list of linked pages.
			linkPageNames.clear();
			try {
				xmlReader.parse(
						new InputSource(
								new StringReader(strHtml)));
				
				// Pass on the out-link information to reducer.
				// In order to reduce the number of records being
				// transferred, we convert the list into CSV and
				// pass it on as a string.
				for (String s : linkPageNames) {
					// Remove self-links since we do not want a
					// page bragging to us that it is important
					if (!s.equals(strPageName)) {
						strEmitVal += s;
						strEmitVal += ",";
						
						// Emit each adjacent node as well to keep
						// track of any dangling nodes
						context.write(
								new KeyPair(
										new Text(s), 
										new Text("ADJ")), 
								new Text(""));
						
						allPageNames.add(s);
					}
				}
				
				context.write(
						new KeyPair(
								new Text(strPageName), 
								new Text("ADJ")),
						new Text(strEmitVal));
				
				allPageNames.add(strPageName);
			} catch (Exception e) {
				// Discard ill-formatted pages.
			}
		}
		
	}
	
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		// Emit all the pages once again to find the number
		// of pages in the reducer
		for (String s : allPageNames) {
			context.write(
					new KeyPair(
							new Text("COUNT"), 
							new Text()), 
					new Text(s));
		}
	}
}
