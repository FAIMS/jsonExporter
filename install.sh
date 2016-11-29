sudo apt-get install spatialite-bin -y


if lsb_release -d -s | grep -q 16.04; then
	sudo apt-get install libsqlite3-mod-spatialite
fi