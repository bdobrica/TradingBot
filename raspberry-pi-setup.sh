#!/bin/bash

## CONFIGURE THE RPI ##
## remember to:
## 8. Update
## 2. Network Options -> N1. Hostname (give it a nice name to find it by, but only if the router allows it)
## 2. Network Options -> N2. Wireless LAN (allow wi-fi access)
## 4. Localisation Options -> I1. Change Locale (US-UTF8) (allow normal text)
## 4. Localisation Options -> I2. Change Time Zone (set a normal clock)
## 5. Interfacing Options -> P2. SSH (allow remote access)
## it might require a reboot (but it will ask you to do it if needed)

sudo raspi-config

## UPDATE THE OS ##

sudo apt-get update
sudo apt-get dist-upgrade

## SHOULD REBOOT ##

reboot

## TEST NETWORKING ##

ping google.com
ifconfig

## INSTALL REQUIRED PACKAGES ##

sudo apt-get install -y vim               # a wicked text editor

## list of common shortcuts:
## by default, you are in view mode so you can't edit the text
## in view mode:
## arrow keys -> move cursor
## dd         -> deletes the current line
## SHIFT+V, y -> copy the current line
## p          -> paste the current line
## :q!        -> quit, without saving
## :w         -> write the file
## :wq or :x  -> write the file and exit
## INSERT     -> switch to insert mode (and allows you to change things)
## :50        -> jump to line 50
## /words     -> searches the text for "words"; press n to go to next results, b to go back to previous result
## :%s/a/b/g  -> searches for "a" and replace it with "b"; g is for global
## :set number -> display line numbers
## :set nonumber -> remove line numbers
## :set mouse=-a -> allows copy/paste with mouse in insert mode
## in insert mode:
## you can edit text as you wish
## ESC        -> switch to view mode

sudo apt-get install -y --upgrade python3 # the latest python version
sudo apt-get install -y mariadb-server    # mariadb database
sudo apt-get install -y rabbitmq-server   # rabbitmq messaging queue
sudo apt-get install -y python3-pip       # python package manager
sudo apt-get install -y screen            # something that allows us to use multiple terminals in one

## list of common shortcuts:
## CTRL+A, C                          -> create a new virtual window and move to it
## CTRL+A, CTRL+A                     -> switch between last two virtual windows
## CTRL+A, CTRL+N                     -> switch to next virtual window
## CTRL+A, CTRL+D                     -> disconnect the session (programs are still running inside)
## CTRL+A, :sessionname name          -> change the name of the session
## CTRL+D                             -> close the window
## list of common uses:
## screen -S name                     -> start a new screen session with name "name"
## screen -r name                     -> reconnect to the session with name "name"
## screen -ls                         -> list all opened sessions

sudo apt-get install -y libf77blas        # a numeric library required for numpy

## UPGRADE PIP AND INSTALL JUPYTER NOTEBOOK ##

sudo python3 -m pip install --user --upgrade pip
sudo pip3 install notebook

## SECURE THE MARIADB INSTALLATION ##

sudo mysql_secure_installation

## TEST THE MARIADB PASSWORD ##

sudo mysql -u root -p

## TEST THE MARIADB PASSWORD AS REGULAR USER ##

mysql -u root -p

## ENABLE RABBITMQ MANAGEMENT PLUGIN ##
sudo rabbitmq-plugins enable rabbitmq_management

## IF NOT WORKING, RUN AS ROOT THE FOLLOWING SQL SCRIPT ##
# grant all privileges on *.* to 'root'@'localhost' identified by 'password';
# flush privileges;

## REQUIRED FOR MATPLOTLIB ##
sudo apt-get install libopenjp2-7 libtiff5

## INSTALL TENSORFLOW ##
sudo pip3 install keras tensorflow
sudo pip3 uninstall tensorflow
sudo apt-get install gfortran
sudo apt-get install libhdf5-dev libc-ares-dev libeigen3-dev
sudo apt-get install libatlas-base-dev libopenblas-dev libblas-dev
sudo apt-get install liblapack-dev cython
sudo pip3 install pybind11
sudo pip3 install h5py
sudo pip3 install --upgrade setuptools
sudo pip3 install gdown
sudo gdown https://drive.google.com/uc?id=11mujzVaFqa7R1_lB7q0kVPW22Ol51MPg
sudo -H pip3 install tensorflow-2.2.0-cp37-cp37m-linux_armv7l.whl

jupyter notebook --NotebookApp.allow_remote_access=1

## IF USING IPV6 ##
# socat TCP6-LISTEN:8888,fork TCP4:127.0.0.1:8888