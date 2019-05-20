package erreesse.backingbean;


import com.google.protobuf.ServiceException;
import erreesse.bean.Query1ResultBean;
import erreesse.hbase.client.AGenericQueryBaseClient;
import erreesse.hbase.client.Query1HBaseClient;
import lombok.Getter;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;
import java.io.IOException;
import java.util.List;

@ManagedBean(name = "Query1BackingBean")
@RequestScoped
public class Query1BackingBean {

    @Getter
    private List<Query1ResultBean> resultBeans;

    @Getter
    private String errorMessage;

    private AGenericQueryBaseClient hbaseClient;

    public Query1BackingBean() {
        hbaseClient = new Query1HBaseClient();
        initResults();

    }

    private void initResults() {
        try {
            resultBeans = hbaseClient.getResults(null,null);
        } catch (IOException e) {
            e.printStackTrace();
            errorMessage = e.getMessage();
        } catch (ServiceException e) {
            e.printStackTrace();
            errorMessage = e.getMessage();
        }
    }
}
