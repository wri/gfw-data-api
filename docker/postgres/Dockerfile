FROM postgres:11

RUN apt-get update -y && apt-get install  postgresql-11-postgis-2.5 -y

ENTRYPOINT ["docker-entrypoint.sh"]

EXPOSE 5432
CMD ["postgres"]