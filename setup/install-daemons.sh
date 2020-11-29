#!/bin/bash

DAEMONS_PATH="/home/pi/etc/init.d"
ETC_PATH="/etc/init.d"

ls --colors=none ${DAEMONS_PATH} | while read DAEMON_FILE; do
    sudo cp ${DAEMONS_PATH}/${DAEMON_FILE} ${ETC_PATH}
    sudo chmod +x ${ETC_PATH}/${DAEMON_FILE} 
done

sudo systemctl daemon-reload

sudo systemctl enable read-websocket
sudo systemctl enable database-save
sudo systemctl enable database-read

sudo /etc/init.d/read-websocket start
sudo /etc/init.d/database-save start
sudo /etc/init.d/database-read start