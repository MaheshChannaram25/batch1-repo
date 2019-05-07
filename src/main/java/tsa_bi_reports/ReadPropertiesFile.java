package tsa_bi_reports;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.mule.DefaultMuleMessage;
import org.mule.api.MuleContext;
import org.mule.api.MuleEventContext;
import org.mule.api.client.MuleClient;
import org.mule.api.lifecycle.Callable;

public class ReadPropertiesFile implements Callable{
	private final static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
	
	public Object onCall(MuleEventContext eventContext) throws Exception {
		List<HashMap<String, Object>> queryList = new ArrayList<HashMap<String, Object>>();

		InputStream  input = null;
		MuleContext muleContext = eventContext.getMuleContext();
		MuleClient muleClient = eventContext.getMuleContext().getClient();
		input = getClass().getResourceAsStream("/mule-app.properties");
		Properties prop = new Properties();
		prop.load(input);
		int queryCount = Integer.parseInt(prop.getProperty("tsa.query.count").toString());
		System.out.println("********** total number of queies:"+queryCount);
		Date maxCreatedDateInExctractTable=eventContext.getMessage().getInvocationProperty("maxCreatedDateInExctractTable");
		maxCreatedDateInExctractTable=maxCreatedDateInExctractTable!=null?maxCreatedDateInExctractTable:new Date();
		System.out.println("maxCreatedDateInExctractTable "+eventContext.getMessage().getInvocationProperty("maxCreatedDateInExctractTable"));
			
		for(int i=1; i<=queryCount; i++){
			HashMap<String, Object> TSAQuery = new HashMap<String, Object>();
			String tableType=prop.getProperty("tsa.select.table."+i);
			TSAQuery.put("selectQuery", prop.getProperty("tsa.select."+i).replaceAll("MAX_CREATED_DATE", "'"+sdf.format(maxCreatedDateInExctractTable)+"'"));
			TSAQuery.put("dateCheck", new Date());
			TSAQuery.put("count", i);
			TSAQuery.put("insertTable",  prop.getProperty("tsa.insert.table."+i));
			TSAQuery.put("dateParam",  prop.getProperty("tsa.insert.table.date."+i));
			TSAQuery.put("dataType",  prop.getProperty("tsa.insert.dbtype."+i));
			TSAQuery.put("operationtype",  prop.getProperty("tsa.insert.operationtype."+i));
			TSAQuery.put("tableType",  tableType);
			TSAQuery.put("dataBaseType",  prop.getProperty("tsa.db.flag."+i));
			if(tableType.equalsIgnoreCase("FACTTABLE")){
				TSAQuery.put("deleteQueryForFact",prop.getProperty("tsa.delete.query."+i));
				//TSAQuery.put("dataloadtype",prop.getProperty("tsa.insert.dataloadtype."+i));
			}
			TSAQuery.put("jobRunWeekly",eventContext.getMessage().getInvocationProperty("tsa.run.job.weekly"));
			TSAQuery.put("flowRunID",eventContext.getMessage().getInvocationProperty("flowRunID"));
			TSAQuery.put("tableRunID",eventContext.getMessage().getInvocationProperty("tableRunID"));
			queryList.add(TSAQuery);
			DefaultMuleMessage localMsg = new DefaultMuleMessage(TSAQuery, muleContext); 
			muleClient.dispatch("vm://TSADB", localMsg);
		}
		eventContext.getMessage().setInvocationProperty("queryList", queryList);
		System.out.println("!!!!!!! query::"+queryList); 
		return eventContext.getMessage();
	}

}
