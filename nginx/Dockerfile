FROM nginx:latest

COPY ./front_build /var/www/app/
# COPY ./front2_build /var/www/app2/
COPY ./landing_build /var/www/landing/

COPY GeoIP.dat /etc/nginx/GeoIP.dat
COPY GeoIPCity.dat /etc/nginx/GeoIPCity.dat
COPY .htpasswd /etc/nginx/htpasswd
COPY nginx.conf /etc/nginx/nginx.conf

EXPOSE 80