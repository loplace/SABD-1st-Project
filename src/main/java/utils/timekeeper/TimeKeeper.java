package utils.timekeeper;

import lombok.*;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.joda.time.PeriodType;

import javax.validation.constraints.NotNull;
import java.util.*;

public class TimeKeeper {

    private Map<String,StagePhasesTime> phasesMap;

    public TimeKeeper() {
        phasesMap = new HashMap<>();

    }

    public void startPhase(@NotNull final String phaseName) {
        if (phasesMap.containsKey(phaseName)) {
            throw new IllegalStateException("Phase: "+phaseName+" already started");
        } else {
            StagePhasesTime phases = new StagePhasesTime();
            phases.setStartTime();
            phasesMap.put(phaseName,phases);
        }
    }

    public void endPhase(@NotNull final String phaseName) {
        if (phasesMap.containsKey(phaseName)) {
            StagePhasesTime phases = phasesMap.get(phaseName);
            phases.setEndTime();
        } else {
            throw new IllegalStateException("PhaneName not found: "+phaseName);
        }
    }


    public String getTableTimes() {
        Duration total = Duration.ZERO;
        StringBuilder sb = new StringBuilder();
        sb.append("Phase Name \t");
        sb.append("Start Time \t");
        sb.append("End Time \t");
        sb.append("Duration (sec)\n");
        for (Map.Entry<String,StagePhasesTime> e : phasesMap.entrySet()) {
            String phaseName = e.getKey();
            StagePhasesTime stagePhasesTime = e.getValue();
            sb.append("--------------------------------------------------------------------------------------------------------------------\n");
            sb.append(phaseName+"\t");
            sb.append(stagePhasesTime.getStartTime().toLocalTime()+"\t");
            sb.append(stagePhasesTime.getEndTime().toLocalTime()+"\t");
            sb.append(stagePhasesTime.getDuration().getStandardSeconds()+"."+stagePhasesTime.getDuration().getMillis()+"\n");

            total = total.plus(stagePhasesTime.getDuration());
        }
        sb.append("--------------------------------------------------------------------------------------------------------------------\n");
        sb.append("Total execution time: "+total.getStandardSeconds()+"."+total.getMillis()+" secs");
        return sb.toString();
    }


    public static void main(String[] args) throws InterruptedException {

        TimeKeeper tk = new TimeKeeper();

        tk.startPhase("INGESTION");
        Thread.sleep(4000);
        tk.endPhase("INGESTION");

        tk.startPhase("FASE 1");
        tk.startPhase("FASE 2");
        Thread.sleep(4000);
        tk.endPhase("FASE 2");
        Thread.sleep(1000);
        tk.endPhase("FASE 1");

        System.out.println(tk.getTableTimes());
    }

}

@NoArgsConstructor
@ToString
@EqualsAndHashCode
class StagePhasesTime {
    @Getter
    private DateTime startTime;
    @Getter
    private DateTime endTime;

    public void setStartTime() {
        startTime = DateTime.now();
    }

    public void setEndTime() {
        if (startTime==null || startTime.compareTo(DateTime.now())>0 ) {
            throw new IllegalStateException("StartTime was not set yet or EndTime is earlier");
        }
        endTime = DateTime.now();
    }

    public Duration getDuration() {
        return new Duration(startTime,endTime);
    }
}
