package edu.mcw.rgd.dataload;

import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.pheno.Condition;
import edu.mcw.rgd.datamodel.pheno.Experiment;
import edu.mcw.rgd.datamodel.pheno.Record;
import edu.mcw.rgd.datamodel.pheno.Study;
import edu.mcw.rgd.process.Utils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jdepons
 * @since 4/26/12
 * index experiment record table
 */
public class FullRecordIndex {

    Logger log = LogManager.getLogger("core");
    Logger logIncoming = LogManager.getLogger("incoming");
    FullRecordIndexDao dao = new FullRecordIndexDao();

    private String version;

    public static void main(String[] args) throws Exception {

        boolean debug = false;
        for( String arg: args ) {
           if( arg.contains("debug") ) {
               debug = true;
           }
        }

        FullRecordIndex fri = new FullRecordIndex();
        try {
            fri.runPipeline(debug);
        }catch (Exception e) {
            fri.log.error(e);
            throw e;
        }
    }

    public void runPipeline(boolean debug) throws Exception {

        Date date0 = new Date();
        long time0 = date0.getTime();
        log.info("Starting "+getVersion());
        log.info("   REC_COUNT: "+Utils.formatThousands(dao.getFullRecordCount()));

        AtomicInteger rowsIncoming = new AtomicInteger(0);
        AtomicInteger rowsUpToDate = new AtomicInteger(0);
        AtomicInteger rowsInserted = new AtomicInteger(0);
        AtomicInteger rowsDeleted = new AtomicInteger(0);
        AtomicInteger i = new AtomicInteger(0);

        dao.loadAspectMap();

        List<Study> studies = dao.getStudies();
        Collections.shuffle(studies);

        studies.parallelStream().forEach( s -> {

            if( debug ) {
                i.incrementAndGet();
                System.out.println(i+"/"+studies.size()+"  INS="+rowsInserted+",  DEL="+rowsDeleted+",  MATCH="+rowsUpToDate);
            }

            try {
                List<FullRecord> fullRecordsInRgd = dao.getFullRecordsForStudy(s.getId());
                Set<FullRecord> fullRecordsIncoming = new HashSet<FullRecord>();

                List<Experiment> experiments = dao.getExperiments(s.getId());

                for (Experiment e: experiments) {

                    List<Record> records = dao.getRecords(e.getId());

                    for (Record r : records) {

                        if (r.getCurationStatus() != 40) {
                            continue;
                        }

                        addIncomingRecord(r.getId(), r.getMeasurementMethod().getAccId(), r.getMeasurementMethod().getAccId(), s.getId(), s.getName(), e.getId(), e.getName(), fullRecordsIncoming);
                        List<Term> parents = dao.getAllActiveTermAncestors(r.getMeasurementMethod().getAccId());
                        for (Term t : parents) {
                            addIncomingRecord(r.getId(), t.getAccId(), r.getMeasurementMethod().getAccId(), s.getId(), s.getName(), e.getId(), e.getName(), fullRecordsIncoming);
                        }

                        addIncomingRecord(r.getId(), r.getClinicalMeasurement().getAccId(), r.getClinicalMeasurement().getAccId(), s.getId(), s.getName(), e.getId(), e.getName(), fullRecordsIncoming);
                        parents = dao.getAllActiveTermAncestors(r.getClinicalMeasurement().getAccId());
                        for (Term t : parents) {
                            addIncomingRecord(r.getId(), t.getAccId(), r.getClinicalMeasurement().getAccId(), s.getId(), s.getName(), e.getId(), e.getName(), fullRecordsIncoming);
                        }

                        addIncomingRecord(r.getId(), r.getSample().getStrainAccId(), r.getSample().getStrainAccId(), s.getId(), s.getName(), e.getId(), e.getName(), fullRecordsIncoming);
                        parents = dao.getAllActiveTermAncestors(r.getSample().getStrainAccId());
                        for (Term t : parents) {
                            addIncomingRecord(r.getId(), t.getAccId(), r.getSample().getStrainAccId(), s.getId(), s.getName(), e.getId(), e.getName(), fullRecordsIncoming);
                        }

                        List<Condition> conditions = r.getConditions();
                        for (Condition cond : conditions) {
                            addIncomingRecord(r.getId(), cond.getOntologyId(), cond.getOntologyId(), s.getId(), s.getName(), e.getId(), e.getName(), fullRecordsIncoming);
                            parents = dao.getAllActiveTermAncestors(cond.getOntologyId());
                            for (Term t : parents) {
                                addIncomingRecord(r.getId(), t.getAccId(), cond.getOntologyId(), s.getId(), s.getName(), e.getId(), e.getName(), fullRecordsIncoming);
                            }
                        }
                    }
                }

                if( !(fullRecordsIncoming.isEmpty() && fullRecordsInRgd.isEmpty()) ) {

                    rowsIncoming.addAndGet(fullRecordsIncoming.size());
                    for( FullRecord r: fullRecordsIncoming ) {
                        logIncoming.debug(r.dump("|"));
                    }

                    Collection<FullRecord> fullRecordsUpToDate = CollectionUtils.intersection(fullRecordsInRgd, fullRecordsIncoming);
                    if( !fullRecordsUpToDate.isEmpty() ) {
                        rowsUpToDate.addAndGet(fullRecordsUpToDate.size());
                        dao.refreshLastUpdateDate(fullRecordsUpToDate);
                    }

                    Collection<FullRecord> fullRecordsForInsert = CollectionUtils.subtract(fullRecordsIncoming, fullRecordsInRgd);
                    if( !fullRecordsForInsert.isEmpty() ) {
                        rowsInserted.addAndGet(fullRecordsForInsert.size());
                        dao.insertRecords(fullRecordsForInsert);
                    }

                    Collection<FullRecord> fullRecordsForDelete = CollectionUtils.subtract(fullRecordsInRgd, fullRecordsIncoming);
                    if( !fullRecordsForDelete.isEmpty() ) {
                        rowsDeleted.addAndGet(fullRecordsForDelete.size());
                        dao.deleteRecords(fullRecordsForDelete);
                    }
                }
            } catch(Exception e) {
                // exceptions not allowed within lambdas -- wrapping them as RuntimeExceptions to suppress lambda limitations
                throw new RuntimeException(e);
            }

        });

        int staleRowsDeleted = dao.deleteStaleRecords(date0, rowsIncoming.get(), log);

        if( rowsIncoming.get()!=0 ) {
            log.info("  incoming rows:   " + Utils.formatThousands(rowsIncoming));
        }
        if( rowsUpToDate.get()!=0 ) {
            log.info("  up-to-date rows: " + Utils.formatThousands(rowsUpToDate));
        }
        if( rowsInserted.get()!=0 ) {
            log.info("  inserted rows:   " + Utils.formatThousands(rowsInserted));
        }
        if( rowsDeleted.get()!=0 ) {
            log.info("  deleted rows:    " + Utils.formatThousands(rowsDeleted));
        }
        if( staleRowsDeleted!=0 ) {
            log.info("  deleted stale rows: " + Utils.formatThousands(staleRowsDeleted));
        }
        log.info("   REC_COUNT: "+Utils.formatThousands(dao.getFullRecordCount()));

        log.info("=== OK === time elapsed " + Utils.formatElapsedTime(time0, System.currentTimeMillis()));
    }

    void addIncomingRecord(int recordId, String accId, String primaryAccId, int studyId, String studyName,
                           int experimentId, String experimentName, Collection<FullRecord> fullRecords) {

        String aspect = dao.getAspect(accId);
        if( aspect==null ) {
            log.warn(" NULL aspect for term_acc:"+accId+", study_id:"+studyId+", exp_id:"+experimentId);
            return;
        }

        FullRecord r = new FullRecord();
        r.setExperimentRecordId(recordId);
        r.setTermAcc(accId);
        r.setPrimaryTermAcc(primaryAccId);
        r.setStudyId(studyId);
        r.setStudyName(studyName);
        r.setExperimentId(experimentId);
        r.setExperimentName(experimentName);
        r.setAspect(aspect);

        fullRecords.add(r);
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}
