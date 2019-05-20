package erreesse.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import javax.faces.bean.ManagedBean;

@ManagedBean
@Data
@AllArgsConstructor
public class Query1ResultBean {

    private String year;

    private String citiesList;
}
