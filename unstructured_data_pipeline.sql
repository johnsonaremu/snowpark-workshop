use role sysadmin;
use warehouse demo_Wh;
use database datalake;
use schema unstructured;

create stage if not exists ext_pdf_stage
storage_integration = s3_int
url='s3://your-bucket/files/invoice/'
directory = (enable = TRUE, auto_refresh=TRUE);

desc stage ext_pdf_stage;

CREATE or REPLACE STREAM INVOICE_DEMO_STREAM ON DIRECTORY(@ext_pdf_stage);

ls @ext_pdf_stage;

SELECT * FROM DIRECTORY( @ext_pdf_stage );

ALTER STAGE ext_pdf_stage REFRESH;


/*
aws s3 cp /Users/upatel/work/data/unstructured/invoices/invoice1.pdf s3://your-bucket/files/invoice/
upload: work/data/unstructured/invoices/invoice2.pdf to s3://your-bucket/files/invoice/invoice2.pdf
*/


select * from INVOICE_DEMO_STREAM;

-- code in python and run in snowflake

select * from information_schema.packages
where LANGUAGE = 'python'
and package_name ilike  '%pdf%';

create or replace stage ext_jars_lib
storage_integration = s3_int
url='s3://your-bucket/jars/';

--https://mvnrepository.com/artifact/org.apache.pdfbox/pdfbox-app/2.0.24
-- upload file pdfbox-app-2.0.24.jar

ls @ext_jars_lib pattern = '.*pdf.*';

create or replace function read_pdf(file string)
returns string
language java
imports = ('@ext_jars_lib/pdfbox-app-2.0.24.jar')
HANDLER = 'PdfParser.ReadFile'
as
$$
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.PDFTextStripperByArea;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class PdfParser {

    public static String ReadFile(InputStream stream) throws IOException {
        try (PDDocument document = PDDocument.load(stream)) {

            document.getClass();

            if (!document.isEncrypted()) {

                PDFTextStripperByArea stripper = new PDFTextStripperByArea();
                stripper.setSortByPosition(true);

                PDFTextStripper tStripper = new PDFTextStripper();

                String pdfFileInText = tStripper.getText(document);
                return pdfFileInText;
            }
        }

        return null;
    }
}
$$;



select
    relative_path
    , file_url
    , read_pdf(BUILD_SCOPED_FILE_URL(@ext_pdf_stage, relative_path)) as parsed_text
from INVOICE_DEMO_STREAM;

create or replace table  parsed_invoices as
select
    relative_path
    , file_url
    , read_pdf(BUILD_SCOPED_FILE_URL(@ext_pdf_stage, relative_path)) as parsed_text
from INVOICE_DEMO_STREAM;

select * from parsed_invoices;

create or replace view invoices_data_vw as (
with items_to_array as (
    select
        *
        , split(
            substr(
              regexp_substr(parsed_text, 'Amount\n(.*)\n(.*)\n(.*)'
              ), 8
            ), '\n'
          )
        as items
    from parsed_invoices
)
, parsed_pdf_fields as (
    select
        substr(regexp_substr(parsed_text, '# [0-9]+'), 2)::int as invoice_number
        , to_number(substr(regexp_substr(parsed_text, '\\$[^A-Z]+'), 2), 10, 2) as balance_due
        , substr(
            regexp_substr(parsed_text, '[0-9]+\n[^\n]+')
                , len(regexp_substr(parsed_text, '# [0-9]+'))
            ) as invoice_from
        , to_date(substr(regexp_substr(parsed_text, 'To:\n[^\n]+'), 5), 'mon dd, yyyy') as invoice_date
        , i.value::string as line_item
        , parsed_text
    from
        items_to_array
        , lateral flatten(items_to_array.items) i
)
select
    invoice_number
    , balance_due
    , invoice_from
    , invoice_date
    , rtrim(regexp_substr(line_item, ' ([0-9]+) \\$')::string, ' $')::integer as item_quantity
    , to_number(ltrim(regexp_substr(line_item, '\\$[^ ]+')::string, '$'), 10, 2) as item_unit_cost
    , regexp_replace(line_item, ' ([0-9]+) \\$.*', '')::string as item_name
    , to_number(ltrim(regexp_substr(line_item, '\\$[^ ]+', 1, 2)::string, '$'), 10, 2) as item_total_cost
from parsed_pdf_fields

);

select * from invoices_data_vw;


CREATE TASK if not exists update_pdf_invoices
WAREHOUSE = COMPUTE_WH
--SCHEDULE = '1 MINUTE'
USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = 15
WHEN
  SYSTEM$STREAM_HAS_DATA('INVOICE_DEMO_STREAM')
AS
  INSERT INTO parsed_invoices(relative_path, file_url, parsed_text)
  select
    relative_path
    , file_url
    , read_pdf(BUILD_SCOPED_FILE_URL(@ext_pdf_stage, relative_path)) as parsed_text
  from INVOICE_DEMO_STREAM;

ALTER TASK update_pdf_invoices RESUME;



/*
aws s3 cp /Users/upatel/work/data/unstructured/invoices/invoice1.pdf s3://your-bucket/files/invoice/
upload: work/data/unstructured/invoices/invoice2.pdf to s3://your-bucket/files/invoice/invoice2.pdf

aws s3 cp /Users/upatel/work/data/unstructured/invoices s3://your-bucket/files/invoice/ --recursive --exclude "*" --include "invoice1*"
*/

select * from invoices_data_vw;


-- purchse item
select
    sum(item_quantity)
    , item_name
from invoices_data_vw
group by item_name
order by sum(item_quantity) desc
;

-- money spent
select
    sum(item_total_cost)
    , item_name
from invoices_data_vw
group by item_name
order by sum(item_total_cost) desc
limit 10;


select
    sum(item_total_cost)
    , date_trunc('month', invoice_date) as month
from invoices_data_vw
group by date_trunc('month', invoice_date);


--monitor task
-- When will the next task run
select *
from table(information_schema.task_history())
where state = 'SCHEDULED'
order by 1;

-- Show task history for load task

select timestampdiff(second, current_timestamp, scheduled_time) as next_run, scheduled_time, current_timestamp, name, state
from table(information_schema.task_history())
where state = 'SCHEDULED'
order by completed_time desc;

-- Document AI (no code solution)

SELECT DOCAI_DB.MODELS.INVOICE_MODEL!predict(
get_presigned_url('@ext_pdf_stage','invoice1.pdf'), 1);

-- cleanup
rm @ext_pdf_stage;
alter task update_pdf_invoices suspend;
