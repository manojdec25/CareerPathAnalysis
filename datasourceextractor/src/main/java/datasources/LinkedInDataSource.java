package datasources;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;



import org.scribe.builder.ServiceBuilder;
import org.scribe.builder.api.LinkedInApi;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.model.Verifier;
import org.scribe.oauth.OAuthService;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class LinkedInDataSource { // implements IDataSource{
	// private static final String PROTECTED_RESOURCE_URL =
	// "http://api.linkedin.com/v1/people/~/connections:(id,last-name)";

	private static final String PEOPLE_ID_URL = "http://api.linkedin.com/v1/people/id=#:(first-name,last-name,positions:(title))";

	private static final String FIRST_NAME_SEARCH_URL = "http://api.linkedin.com/v1/people-search:(people:(id))?start=START_CHAR&count=25&first-name=#";

	private static final String INITIAL_SEARCH_URL = "http://api.linkedin.com/v1/people-search:(people:(id,first-name))?start=START_CHAR&count=25";
	//private static final String INITIAL_SEARCH_URL = "http://api.linkedin.com/v1/people-search:(people:(first-name))?start=0&count=25";
	
	private static final String ESCAPE_CHAR = "#";
	private static final int MAX_COUNT_PER = 110;
	private static final String START_CHAR = "START_CHAR";
	private static final int CNT_PER_REQ = 25;
	
	private static final String LOCATION = "LinkedinOutput.txt";
	private static final String COLUMNDELIMITER = ";";

	private static FileWriter fstream = null;
	private static BufferedWriter out = null;
	
	
	// private static final String PROTECTED_RESOURCE_URL =
	// "http://api.linkedin.com/v1/people/~/connections:(id,last-name,first-name,positions:(title))";

	// private static final String PROTECTED_RESOURCE_URL =
	// "http://api.linkedin.com/v1/people-search:(people,num-results)";

	public static void main(String[] args) {

		// Ujval's Key

		//OAuthService service = new ServiceBuilder().provider(LinkedInApi.class)
		//		.apiKey("ka22fmy3adzp").apiSecret("tAps1J6IZUpATa5G")
		//		.scope("r_network").build();

		// Manoj's Key
		
		OAuthService service = new
		ServiceBuilder().provider(LinkedInApi.class)
		.apiKey("uxh8odsb97ek").apiSecret("xM1DqpXRysQ77fk7").scope("r_network")
		.build();
		
		Scanner in = new Scanner(System.in);

		// Obtain the Request Token
		Token requestToken = service.getRequestToken();
		System.out.println(service.getAuthorizationUrl(requestToken));
		System.out.println("And paste the verifier here");
		System.out.print(">>");
		Verifier verifier = new Verifier(in.nextLine());
		System.out.println();

		// Trade the Request Token and Verfier for the Access Token
		Token accessToken = service.getAccessToken(requestToken, verifier);

		OpenFile(LOCATION);
		
		for(int cnt = 0; cnt < (MAX_COUNT_PER + CNT_PER_REQ); cnt = cnt+25)
		{
			
		String searchurl = INITIAL_SEARCH_URL.replaceAll(START_CHAR, String.valueOf(cnt));
		System.out.println("Search String " + searchurl );
		
		
		String responseString = makeLinkedInAPICall(searchurl,
				service, accessToken);

		System.out.println("Response String is :" + responseString);

		try {

		
			
			System.out.println("ROUND1 - Getting titles from the connections");
			String xpathExprConnIds = "/people-search/people/person/id";
			NodeList nodesConnIds = fetchNodeListFromXML(responseString, xpathExprConnIds);
			//ArrayList<String> listConnIds = new ArrayList<String>();
			System.out.println("ROUND 1 - Num of Connections - " + nodesConnIds.getLength() );

			ArrayList<String> listConnIds = extractTitlesUsingIDs(service, accessToken, nodesConnIds);
			if(listConnIds!=null && listConnIds.size()>0)
				WriteToFile(listConnIds);
			
			System.out.println("ROUND2 - People Search on the first-name");			
			String xpathExpr = "/people-search/people/person/first-name";
			NodeList nodes = fetchNodeListFromXML(responseString, xpathExpr);
			ArrayList<String> list = new ArrayList<String>();
			
			for (int i = 0; i < nodes.getLength(); i++) {
			//for (int i = 0; i < nodes.getLength() && i < 2; i++) {
				Node node = nodes.item(i);

				String url = FIRST_NAME_SEARCH_URL.replaceAll(ESCAPE_CHAR,
						node.getTextContent());
				
				for(int cnt1 = 0; cnt1 < (MAX_COUNT_PER + CNT_PER_REQ); cnt1 = cnt1+25)
				{
					
					String searchurl1 = url.replaceAll(START_CHAR, String.valueOf(cnt1));
					System.out.println("Search String 1 " + searchurl1 );
				
					String responseString2 = makeLinkedInAPICall(searchurl1, service,
							accessToken);
					
					String xpathExpr2 = "/people-search/people/person/id";
					NodeList nodes2 = fetchNodeListFromXML(responseString2,
							xpathExpr2);
	
					list = extractTitlesUsingIDs(service, accessToken, nodes2);
					if(list!=null && list.size()>0)
						WriteToFile(list);
				}				
			}			

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally{
			
		}
		}
		CloseFile();
		System.out.println("Program End !!!!!!!!!!!!!!!!!!!!!!!!");
		// System.out.println(response.getBody());
	}

	
	private static ArrayList<String> extractTitlesUsingIDs(OAuthService service,
			Token accessToken, NodeList nodes2)
			throws ParserConfigurationException, SAXException, IOException,
			XPathExpressionException {
		
		ArrayList<String> list = new ArrayList<String>();
		
		for (int j = 0; j < nodes2.getLength(); j++) {
			Node node2 = nodes2.item(j);
			if (node2 != null
					&& !node2.getTextContent().equalsIgnoreCase(
							"private")) {
				String url2 = PEOPLE_ID_URL.replaceAll(ESCAPE_CHAR,
						node2.getTextContent());
				String responseString3 = makeLinkedInAPICall(url2,
						service, accessToken);
				String xpathExpr3 = "/person/positions/position/title";
				NodeList nodes3 = fetchNodeListFromXML(responseString3,
						xpathExpr3);

				//if (nodes3.getLength() > 1) {

					list.clear();
					//Name 
					String xpathExpr4 = "/person/first-name";
					NodeList nodes4 = fetchNodeListFromXML(responseString3,
							xpathExpr4);							
					if (nodes4 != null && nodes4.getLength() > 0) {
						System.out.println("Name: "
								+ nodes4.item(0).getTextContent());
					}
					

					for (int k = 0; k < nodes3.getLength(); k++) {
						Node node3 = nodes3.item(k);
						System.out.println("Final Node Text: "
								+ node3.getTextContent());
						list.add(node3.getTextContent());
					}
					
				//}
			}
		}
		return list;		
	}

	private static NodeList fetchNodeListFromXML(String responseString,
			String xpathExpr) throws ParserConfigurationException,
			SAXException, IOException, XPathExpressionException {
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(new InputSource(new StringReader(
				responseString)));

		XPathFactory xPathfactory = XPathFactory.newInstance();
		XPath xpath = xPathfactory.newXPath();
		XPathExpression expr = xpath.compile(xpathExpr);
		NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
		return nodes;
	}

	private static String makeLinkedInAPICall(String apiCallURL,
			OAuthService service, Token accessToken) {
		OAuthRequest request = new OAuthRequest(Verb.GET, apiCallURL);
		service.signRequest(accessToken, request);
		Response response = request.send();
		return response.getBody();
	}
	
	
	public static void OpenFile(String file)
	{
		try {
			// Create file
			fstream = new FileWriter(file);
			out = new BufferedWriter(fstream);
		} catch (Exception e) {// Catch exception if any
			System.err.println("Error in Opening the file: " + e.getMessage());
		}
	}
	
	public static void CloseFile()
	{
		try {
			if(out!=null)
				out.close();
		} catch (IOException e) {
			System.err
			.println("Error in Closing Output! " + e.getMessage());
		}	
	}	

	public static void WriteToFile(ArrayList<String> list) {

		//List<String> list = new ArrayList<String>();
		//list.add("SE1,SE2,SE3");
		//list.add("Team Lead,Manager,Director");
		//list.add("Consultant,Senior Consultant,Architect");


		try {
			
			if(out == null)
			{
				System.out.println("Unable to write to file. Please initialize the file");
				return;
			}
			
			for (String value : list) {
				out.write(value);
				out.write(COLUMNDELIMITER);
				//out.newLine();
			}
			out.write("\n");
		} catch (Exception e) {// Catch exception if any
			System.err.println("Error in writing to file: " + e.getMessage());
		} 
	}

	public boolean InitDataSource() {
		// TODO Auto-generated method stub
		return false;
	}

	public void Run() {
		// TODO Auto-generated method stub
		
	}

	public boolean EndDataSource() {
		// TODO Auto-generated method stub
		return false;
	}	
}
