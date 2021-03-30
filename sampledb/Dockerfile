FROM postgres:13-alpine
#RUN wget http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/$(curl http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/LATEST)/mbdump.tar.bz2
ENV PGDATA=/data
RUN mkdir /data && \
	chown postgres:postgres /data && \
	su -s /bin/bash postgres -c 'initdb --pwfile=<(echo insecure)' && \
	su postgres -c 'echo "host all all 192.168.0.0/16 md5" >> /data/pg_hba.conf' && \
	su postgres -c 'echo "host all all 172.16.0.0/12 md5" >> /data/pg_hba.conf' && \
	su postgres -c 'echo "host all all 10.0.0.0/8 md5" >> /data/pg_hba.conf' && \
	su postgres -c 'pg_ctl start' && \
	su postgres -c 'createdb pagila' && \
	wget -q -O - https://github.com/devrimgunduz/pagila/archive/v2.1.0.tar.gz | tar xzOf - 'pagila-2.1.0/pagila-insert-data.sql' | su postgres -c 'psql -q -d pagila -f -' && \
	su postgres -c 'psql -d pagila -c "select count(1) from actor"' && \
	su postgres -c 'pg_ctl stop'
RUN apk add --update --no-cache iproute2
