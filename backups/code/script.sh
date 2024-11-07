num_processes=16

for i in $(seq 0 $((num_processes - 1))); do
  osascript -e "tell app \"Terminal\" to do script \"cd ~/Documents/Personal/Workspace/aws-mwaa-local-runner && caffeinate -i python backups/code/crawl_reviews.py $i $num_processes\""
done