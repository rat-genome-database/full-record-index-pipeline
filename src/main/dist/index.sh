# run full-record-index-pipeline with commandline parameters
#    ("$@" passes all cmdline parameters to pipeline program)
#
. /etc/profile
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
APPHOME=/home/rgddata/pipelines/full-record-index-pipeline

EMAIL_LIST=mtutaj@mcw.edu
if [ "$SERVER" == "REED" ]; then
  EMAIL_LIST=rgd.devops@mcw.edu
fi

$APPHOME/run.sh "$@"

mailx -s "[$SERVER] Output from full record index pipeline " $EMAIL_LIST < $APPHOME/logs/summary.log
