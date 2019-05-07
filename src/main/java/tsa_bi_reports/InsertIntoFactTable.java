package tsa_bi_reports;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;

import org.mule.DefaultMuleMessage;
import org.mule.api.MuleContext;
import org.mule.api.MuleEventContext;
import org.mule.api.client.MuleClient;
import org.mule.api.lifecycle.Callable;

public class InsertIntoFactTable implements Callable{
	@SuppressWarnings("unchecked")
	public Object onCall(MuleEventContext eventContext) throws Exception {
		LinkedList<?> queryResult = (LinkedList<?>) eventContext.getMessage().getPayload();
		//queryResult = eventContext.getMessage().getPayload();
		MuleContext muleContext = eventContext.getMuleContext();
		MuleClient muleClient = eventContext.getMuleContext().getClient();
		int limit = 1000;
		int querySize = queryResult.size();
		int quotient = querySize / limit;
		int remainder = querySize % limit;
		System.gc();
		 int batch = 0;
		 if(remainder > 0){
			 batch = quotient+1;
		 }else{
			 batch = quotient;
		 }
		int startWith=0;
		if(batch == 0){
			System.out.println("No data is avialable for given date range...");
		}else{
			for(int i=1; i<=batch; i++){
				ArrayList<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
				int batchSize = 0;
				if(i < batch){
					batchSize = limit*i;
				}else{
					if(batch == quotient+1){
						batchSize = (limit*(i-1)) + quotient;	
					}else{
						batchSize = limit*i;
					}
				}
				for(int j=startWith; j<batchSize; j++){
					dataList.add((Map<String, Object>) queryResult.get(j));
				}
				//System.out.println("dataList size():"+dataList.size());
				DefaultMuleMessage localMsg = new DefaultMuleMessage(dataList, muleContext); 
				localMsg.setOutboundProperty("fromLimit", startWith);
				localMsg.setOutboundProperty("offset", batchSize);
				localMsg.setOutboundProperty("count", eventContext.getMessage().getInvocationProperty("count"));
				startWith = batchSize;
				dataList = new ArrayList<Map<String, Object>>();
				muleClient.dispatch("vm://TSAFACTINSERT", localMsg);
				
			}
		}
		
		
		return queryResult;
	}

}
