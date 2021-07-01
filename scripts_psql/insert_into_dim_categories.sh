echo "writing dim_categories to postgresql with copy command"
psql --command="COPY dim_categories FROM PROGRAM 'gzip -dc /Users/mateus.leao/Documents/mattssll/takeaway/output_data/dim_categories/*.csv.gz' delimiters',' CSV;" postgresql://postgres:admin@127.0.0.1:5432/dwh_takeaway
sleep 1
echo "done writing dim_categories"