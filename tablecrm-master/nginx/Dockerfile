FROM nginx:latest

ARG BRANCH_NAME

WORKDIR /etc/nginx

COPY nginx.${BRANCH_NAME}.conf nginx.conf
COPY upstream.conf dir/upstream.conf
COPY GeoIP.dat .
COPY GeoIPCity.dat .
COPY .htpasswd .
COPY proxy_params .

EXPOSE 80
EXPOSE 443
