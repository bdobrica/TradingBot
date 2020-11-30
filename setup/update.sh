#!/bin/bash

cd ~/TradingBot
git pull
chmod +x notebooks/daemons/*.py
chmod -x notebooks/daemons/__init__.py
cp -r notebooks/* ~/

ls --color=none etc/init.d | while read SCRIPT_NAME; do
    sudo /etc/init.d/${SCRIPT_NAME} stop
    sudo cp -v etc/init.d/${SCRIPT_NAME} /etc/init.d/
    sudo chmod +x /etc/init.d/${SCRIPT_NAME}
    sudo systemctl daemon-reload
    sudo /etc/init.d/${SCRIPT_NAME} start
done