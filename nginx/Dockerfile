FROM nginx:latest

ARG BRANCH_NAME
COPY configure.sh /usr/local/bin/configure.sh
RUN chmod +x /usr/local/bin/configure.sh

WORKDIR /etc/nginx

COPY GeoIP.dat .
COPY GeoIPCity.dat .
COPY .htpasswd .
COPY proxy_params .

RUN /usr/local/bin/configure.sh $BRANCH_NAME

EXPOSE 80
EXPOSE 443
