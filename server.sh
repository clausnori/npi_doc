chmod +x install.sh
./install.sh
source app/bin/activate
chmod +x init_cron.sh
./init_cron.sh
chmod +x run_api.sh
./run_api.sh