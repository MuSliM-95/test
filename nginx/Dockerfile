FROM nginx:latest

COPY GeoIP.dat /etc/nginx/GeoIP.dat
COPY GeoIPCity.dat /etc/nginx/GeoIPCity.dat
COPY .htpasswd /etc/nginx/htpasswd
COPY nginx.conf /etc/nginx/nginx.conf

EXPOSE 80