package erreesse.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import javax.faces.bean.ManagedBean;

@ManagedBean
@Data
@AllArgsConstructor
public class Query2ResultBean {

    private String country;
    private String year;
    private String month;

    private String mean;
    private String devStd;
    private String min;
    private String max;
}
