package tsa_bi_reports;

import org.mule.api.MuleEventContext;
import org.mule.api.lifecycle.Callable;

public class GetLimitAndOffset implements Callable{
	public Object onCall(MuleEventContext eventContext) throws Exception {
		String selectQuery = eventContext.getMessage().getInvocationProperty("selectQuery");
		String limit = eventContext.getMessage().getInvocationProperty("limit");
		int offset = eventContext.getMessage().getInvocationProperty("offset");
		int batchCounter = eventContext.getMessage().getInvocationProperty("batchCount");
		System.out.println("batchCounter:"+batchCounter+" offset"+offset);
		/*if(batchCounter > 1){
			int skip = Integer.parseInt(offset)+Integer.parseInt(limit);
			eventContext.getMessage().setInvocationProperty("offset", skip);
			selectQuery = selectQuery.replace("$&&", skip+"").replace("$**", limit);
		}else{
			selectQuery = selectQuery.replace("$&&", offset).replace("$**", limit);
		}*/
		String selectQueryWithLimit = selectQuery.replace("$&&", offset+"").replace("$**", limit+"");
		System.out.println("selectQuery with limit and offset:"+selectQueryWithLimit);
		eventContext.getMessage().setInvocationProperty("selectQueryWithLimit", selectQueryWithLimit);
		return eventContext.getMessage();
	}

}
