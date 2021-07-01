echo "writing dim_pricebucket to postgresql with copy command"
psql --command="COPY dim_pricebucket FROM PROGRAM 'cat /Users/mateus.leao/Documents/mattssll/takeaway/output_data/dim_buckets/*.csv' delimiters',' CSV;" postgresql://postgres:admin@127.0.0.1:5432/dwh_takeaway
sleep 1
echo "done writing dim_pricebucket"
