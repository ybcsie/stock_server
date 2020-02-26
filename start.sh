#!/bin/sh

#########################################################

INI_FILE="stockserver.ini"
SOCK_FILE="stockserver.sock"
NGINX_GROUP="www-data"

#########################################################

if ! id -nG "$USER" | grep -qw "$NGINX_GROUP"; then
	sudo usermod -a -G "$NGINX_GROUP" "$USER"
fi

rm -f "$INI_FILE"

echo [uwsgi] >> "$INI_FILE"
echo module = uwsgi_main >> "$INI_FILE"
echo socket = $SOCK_FILE >> "$INI_FILE"
echo chown-socket = $USER:$NGINX_GROUP >> "$INI_FILE"
echo chmod-socket = 660 >> "$INI_FILE"
echo die-on-term = true >> "$INI_FILE"
echo enable-threads = true >> "$INI_FILE"

uwsgi --ini "$INI_FILE"
