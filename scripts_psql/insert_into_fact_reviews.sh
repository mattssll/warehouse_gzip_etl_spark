echo "writing fact_reviews to postgresql with copy command"
psql --command="COPY fct_reviews FROM PROGRAM 'gzip -dc /Users/mateus.leao/Documents/mattssll/takeaway/output_data/fct_reviews/*.csv.gz' delimiters',' CSV;" postgresql://postgres:admin@127.0.0.1:5432/dwh_takeaway
echo "done writing fact_reviews"