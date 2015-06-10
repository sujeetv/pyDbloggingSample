
# load the adapter
import psycopg2
import traceback
import sys
from Logger import Logger

# load the psycopg extras module
import psycopg2.extras

out_file = "input_file_demo.tsv"

try:
    conn = psycopg2.connect("dbname='pgsdwh' user='gvl_dea_sch' host='3.48.32.77' password='gvld3ASch'")
except:
    print ("Unable to connect to the database")

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
log = Logger.get()

try:
    text_file = open(out_file, "w")
    text_file.write('"EQ2"')
    text_file.write(chr(23))
    text_file.write('"LDTEXT"')
    text_file.write(chr(23))
    text_file.write('"WO_DESC"')
    text_file.write(chr(23))
    text_file.write('"WL_DESC"')
    text_file.write(chr(23))
    text_file.write('"WONUM"')
    text_file.write(chr(23))
    text_file.write('"ASSETNUM"')
    text_file.write(chr(23))
    text_file.write('"STATUS"')
    text_file.write(chr(23))
    text_file.write('"ACTSTART"')
    text_file.write(chr(23))
    text_file.write('"LOCATION"')
    text_file.write(chr(23))
    text_file.write('"WORKTYPE"')
    text_file.write(chr(23))
    text_file.write('"ORGID"')
    text_file.write(chr(23))
    text_file.write('"DESCRIPTION"')
    text_file.write(chr(23))
    text_file.write('"WORKORDERID"')
    text_file.write(chr(23))
    text_file.write('"PARENT"')
    text_file.write(chr(23))
    text_file.write('"WORKORDERSTATUS"')
    text_file.write(chr(23))
    text_file.write('"WODESCRIPTION"')
    text_file.write(chr(23))
    text_file.write('"WOFINISHDATE"\n')

    cur.copy_to(text_file,'(select \'"\'||eq2||\'"\',\'"\'||worklogdescription||\'"\' as "LDTEXT",\'"\'||wosummary||\'"\' AS "WO_DESC",\'"\'||worklogsummary||\'"\' as "WL_DESC",\'"\'||wonum||\'"\' as "WONUM",\'"\'||assetnum||\'"\' as "ASSETNUM",\'"\'||assetstatus||\'"\' as "STATUS",\'"\'||trim(to_char(wostartdate, \'YYYY-MM-DD HH:MM:SS\'))||\'"\' as "ACTSTART",\'"\'||location||\'"\' as "LOCATION",\'"\'||worktype||\'"\' as "WORKTYPE",\'"\'||orgid||\'"\' as "ORGID",\'"\'||description||\'"\',\'"\'||workorderid||\'"\',\'"\'||parent||\'"\',\'"\'||workorderstatus||\'"\',\'"\'||wodescription||\'"\',\'"\'||wofinishdate||\'"\' from gvl_dea.gvl_workorder_summary_stg)',sep=chr(23),null='""')

except Exception, err:
    print(traceback.format_exc())
    #or
    print(sys.exc_info()[0])
    log.error("I can't SELECT from bar", exc_info=err)