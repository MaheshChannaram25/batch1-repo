package tsa_bi_reports;

import java.io.InputStream;
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

public class ReadPropertiesFileForSalesRevenue implements Callable{
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
		for(int i=1; i<=queryCount; i++){
			HashMap<String, Object> TSAQuery = new HashMap<String, Object>();
			TSAQuery.put("selectQuery", prop.getProperty("tsa.select."+i));
			TSAQuery.put("dateCheck", new Date());
			TSAQuery.put("count", i);
			TSAQuery.put("insertTable",  prop.getProperty("tsa.insert.table."+i));
			TSAQuery.put("dateParam",  prop.getProperty("tsa.insert.table.date."+i));
			TSAQuery.put("dataType",  prop.getProperty("tsa.insert.dbtype."+i));
			TSAQuery.put("operationtype",  prop.getProperty("tsa.insert.operationtype."+i));
			if(TSAQuery.get("dataType").toString().equalsIgnoreCase("e-learning_fact")){
				TSAQuery.put("createTable",prop.getProperty("tsa.create.table."+i));
				TSAQuery.put("dropTable",prop.getProperty("tsa.drop.table."+i));
				TSAQuery.put("selectQueryElearning",prop.getProperty("tsa.select.tp2."+i));
				TSAQuery.put("insertQueryElearning",prop.getProperty("tsa.insert.QA."+i));
			}
			queryList.add(TSAQuery);
			DefaultMuleMessage localMsg = new DefaultMuleMessage(TSAQuery, muleContext); 
			//muleClient.dispatch("vm://TSADB", localMsg);
			if(i == 14){
				muleClient.dispatch("vm://TSADB1", localMsg);
			}
			
			
		}
		eventContext.getMessage().setInvocationProperty("queryList", queryList);
		System.out.println("!!!!!!! query::"+queryList); 
		return eventContext.getMessage();
	}


}
