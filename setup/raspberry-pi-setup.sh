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
sudo apt-get install -y python3-pip       # python package manager
sudo apt-get install -y mariadb-server    # mariadb database
sudo apt-get install -y rabbitmq-server   # rabbitmq messaging queue
sudo apt-get install -y screen            # something that allows us to use multiple terminals in one
sudo apt-get install -y libopenjp2-7 libtiff5 # required for matplotlib
#sudo apt-get install -y libf77blas        # a numeric library required for numpy
sudo apt-get install -y gfortran libhdf5-dev libc-ares-dev libeigen3-dev libatlas-base-dev libopenblas-dev libblas-dev liblapack-dev cython # required for Tensorflow

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

## UPGRADE PIP AND INSTALL JUPYTER NOTEBOOK ##

sudo python3 -m pip install --user --upgrade pip
sudo pip3 install notebook

## SECURE THE MARIADB INSTALLATION ##

sudo mysql_secure_installation

## TEST THE MARIADB PASSWORD ##

sudo mysql -u root -p

## TEST THE MARIADB PASSWORD AS REGULAR USER ##

mysql -u root -p

## IF NOT WORKING, RUN AS ROOT THE FOLLOWING SQL SCRIPT ##
# grant all privileges on *.* to 'root'@'localhost' identified by 'password';
# flush privileges;

## ENABLE RABBITMQ MANAGEMENT PLUGIN ##
sudo rabbitmq-plugins enable rabbitmq_management

## INSTALL TENSORFLOW ##
sudo pip3 install --upgrade setuptools      # install requirements for Tensorflow
sudo pip3 install keras tensorflow          # try to install keras and Tensorflow. it will fail, but it'll install required dependencies
sudo pip3 uninstall tensorflow              # remove Tensorflow
sudo pip3 install pybind11                  # install requirements for Tensorflow
sudo pip3 install h5py                      # install requirements for Tensorflow
sudo pip3 install gdown                     # install requirements for Tensorflow
sudo gdown https://drive.google.com/uc?id=11mujzVaFqa7R1_lB7q0kVPW22Ol51MPg # download the Tensorflow 2.2.0 for ARM processors
sudo -H pip3 install tensorflow-2.2.0-cp37-cp37m-linux_armv7l.whl # install TensorFlow 2.2.0 for ARM processors
rm tensorflow-2.2.0-cp37-cp37m-linux_armv7l.whl # remove the TensorFlow installation package, as no longer needed

## THIS STARTS THE JUPYTER NOTEBOOK SERVER ON PORT 8888 ##
jupyter notebook --no-browser --port=8888 -NotebookApp.allow_remote_access=1 &

## ALLOW ACCESS FROM NETWORK TO JUPYTER NOTEBOOK ##
socat TCP6-LISTEN:8889,fork TCP4:127.0.0.1:8888 &

## YOU CAN USE THE run-jupyer.sh SCRIPT TO START THE JUPYTER NOTEBOOK IF NEEDED ##

## REMEMBER TO INSTALL DEPENDENCIES ##
sudo pip3 install -r requirements.txt