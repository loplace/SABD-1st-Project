package erreesse.backingbean;

import com.google.protobuf.ServiceException;
import erreesse.bean.Query3ResultBean;
import erreesse.hbase.client.AGenericQueryBaseClient;
import erreesse.hbase.client.Query3HBaseClient;
import lombok.Getter;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;
import java.io.IOException;
import java.util.List;

@ManagedBean(name = "Query3BackingBean")
@RequestScoped
public class Query3BackingBean {

    @Getter
    private List<Query3ResultBean> resultBeans;

    @Getter
    private String errorMessage;

    private AGenericQueryBaseClient hbaseClient;

    public Query3BackingBean() {
        hbaseClient = new Query3HBaseClient();
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
