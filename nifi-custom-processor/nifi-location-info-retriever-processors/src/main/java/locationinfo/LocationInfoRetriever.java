package locationinfo;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"customfilter"})
@CapabilityDescription("Provide a description")
@SeeAlso()
@ReadsAttributes({@ReadsAttribute(attribute="")})
@WritesAttributes({@WritesAttribute(attribute="")})
public class LocationInfoRetriever extends AbstractProcessor {


    private static final PropertyDescriptor LOCATION_HOST_ADDR = new PropertyDescriptor
            .Builder().name("LOCATION_HOST_ADDR")
            .displayName("Location retrieve info address")
            .description("IP or hostname for Location info retriever service")
            .defaultValue("citylocationhelper")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor LOCATION_HOST_PORT = new PropertyDescriptor
            .Builder().name("LOCATION_HOST_PORT")
            .displayName("Location retrieve info address")
            .description("Port number for Location info retriever service")
            .defaultValue("8888")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    private static final Relationship RETRIEVE_SUCCESS = new Relationship.Builder()
            .name("RETRIEVE_SUCCESS")
            .description("Location info successfully retrieved")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(LOCATION_HOST_ADDR);
        descriptors.add(LOCATION_HOST_PORT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(RETRIEVE_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    private synchronized String retrieveLocation(final ProcessContext context, double latitude, double longitude) {
        String msgFromServer = "";

        String hostService = context.getProperty(LOCATION_HOST_ADDR).evaluateAttributeExpressions().getValue();
        int portService = context.getProperty(LOCATION_HOST_PORT).evaluateAttributeExpressions().asInteger();

        try {
            Socket soc = new Socket(hostService, portService);
            BufferedWriter bout = new BufferedWriter(new OutputStreamWriter(soc.getOutputStream()));
            Scanner scan = new Scanner(soc.getInputStream());
            String dataToSend = latitude + ";" + longitude;
            bout.write(dataToSend);
            bout.flush();
            if (scan.hasNext()) {
                msgFromServer = scan.nextLine();
            }

            bout.close();
            scan.close();
            soc.close();
        } catch (Exception var9) {
            var9.printStackTrace();
        }
        return msgFromServer.replace(";",",");
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final AtomicReference<String> value = new AtomicReference<>();

        FlowFile flowfile = session.get();
        if (flowfile==null) {
            return;
        }

        session.read(flowfile, in -> {
            String line;
            String locationInfo = "";
            try{
                line= IOUtils.toString(in, StandardCharsets.UTF_8);
                getLogger().debug("receive line: "+line);

                if (line.startsWith("City,Latitude,Longitude")) {
                    value.set("City,Latitude,Longitude,TimeZone,Country");
                } else {
                    String[] tokens = line.split(",");
                    double latitude;
                    double longitude;
                    try {
                        if (tokens.length>=3) {
                            latitude = Double.parseDouble(tokens[1]);
                            longitude = Double.parseDouble(tokens[2]);

                            locationInfo = retrieveLocation(context,latitude, longitude);
                        }
                    } catch (NumberFormatException e) {
                        getLogger().debug("NumberFormatException: "+line);
                    }
                    getLogger().debug("sending line: "+line+","+locationInfo);
                    value.set(line+","+locationInfo);
                }

            }catch(Exception ex){
                ex.printStackTrace();
            }
        });


        String results = value.get();
        if( results != null && !results.isEmpty() ) {
            flowfile = session.putAttribute(flowfile, "match", results);
        }

        // To write the results back out ot flow file
        flowfile = session.write(flowfile, out -> {
            String result = value.get();
            if (result!=null) {
                out.write(value.get().getBytes());
                out.flush();
            }
            else {
                System.err.println("result is null");
                getLogger().debug("result is null");
            }
        });

        session.transfer(flowfile, RETRIEVE_SUCCESS);
    }
}
