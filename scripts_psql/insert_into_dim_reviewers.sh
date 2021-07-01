echo "writing dim_reviewers to postgresql with copy command"
psql --command="COPY dim_reviewers FROM PROGRAM 'gzip -dc /Users/mateus.leao/Documents/mattssll/takeaway/output_data/dim_reviewers/*.csv.gz' delimiters',' CSV;" postgresql://postgres:admin@127.0.0.1:5432/dwh_takeaway
echo "done writing dim_reviewers"