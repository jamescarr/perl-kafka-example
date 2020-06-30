FROM perl:5-buster

RUN cpanm Carton \
    && mkdir -p /usr/src/app

COPY . /usr/src/app

RUN cd /usr/src/app && ls -al && carton install

WORKDIR /usr/src/app

CMD carton exec perl app.pl

